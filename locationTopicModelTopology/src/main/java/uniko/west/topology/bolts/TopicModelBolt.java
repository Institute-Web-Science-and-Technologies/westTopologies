/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.topology.bolts;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import jgibblda.PredictLocation;
import uniko.west.topology.datatypes.MessageLocationPrediction;
import util.FileLoader;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 *
 * @author Martin Koerner <info@mkoerner.de> and nico
 */
public class TopicModelBolt extends BaseRichBolt {

	/**
     *
     */
	private static final long serialVersionUID = -8319149705129653575L;
	private OutputCollector collector;
	private String strExampleEmitFieldsId;

	private ArrayList<HashMap<String, Object>> messages;
	private int messagesPerPrediction;
	private String topicModelDirectoryURL;
	private int locationsPerMessage;
	private String localTopicModelDirectory;

	public TopicModelBolt(String strExampleEmitFieldsId,
			String topicModelDictionaryURL) {
		super();

		this.strExampleEmitFieldsId = strExampleEmitFieldsId;
		this.topicModelDirectoryURL = topicModelDictionaryURL;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(this.strExampleEmitFieldsId));
	}

	// TODO remove SuppressWarnings
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {
		// get localTopicModelDirectory
		try {
			File topologyJarFile = new File(TopicModelBolt.class
					.getProtectionDomain().getCodeSource().getLocation()
					.toURI().getPath());
			this.localTopicModelDirectory = topologyJarFile.getParent()
					+ "/topic-model";
			new File(localTopicModelDirectory).mkdirs();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String[] topicModelFiles = { "pi_0Alpha", "pisetasum", "qm",
				"topic_word" };
		for (String topicModelFile : topicModelFiles) {
			FileLoader.getFile(topicModelDirectoryURL + "/" + topicModelFile,
					localTopicModelDirectory + "/" + topicModelFile);
		}
		this.collector = collector;
		this.messages = new ArrayList<HashMap<String, Object>>();
		this.messagesPerPrediction = 50;
		this.locationsPerMessage = 5;

	}

	@Override
	public void execute(Tuple input) {
		@SuppressWarnings("unchecked")
		HashMap<String, Object> currentMessage = (HashMap<String, Object>) input
				.getValue(0);
		this.messages.add(currentMessage);

		if (messages.size() >= this.messagesPerPrediction) {
			this.runPrediction();

			for (HashMap<String, Object> message : messages) {
				// remove key-value pairs from message which are not part of the
				// specification
				// for (String key : message.keySet()) {
				// if (!key.equals("itinno:item_id")
				// && !key.equals("ukob:topic_set")) {
				// message.remove(key);
				// }
				// }
				// test printout
				try (PrintStream testOut = new PrintStream(new File(
						"/home/martin/test/topicModelBolt/location"
								+ message.hashCode() + ".log"), "UTF8")) {
					testOut.println("text: " + message.get("text"));
					testOut.println("ukob:topic_set: "
							+ message.get("ukob:topic_set"));
					for (String item : message.keySet()) {
						testOut.println(item);
					}
					testOut.println(message.toString());
				} catch (FileNotFoundException ex) {
					Logger.getLogger(TopicModelBolt.class.getName()).log(
							Level.SEVERE, null, ex);
				} catch (UnsupportedEncodingException ex) {
					Logger.getLogger(TopicModelBolt.class.getName()).log(
							Level.SEVERE, null, ex);
				}

				// create results for this message
				ArrayList<Object> results = new ArrayList<Object>();
				results.add((Object) message);
				this.collector.emit(results);
			}

			// reset messages
			this.messages = new ArrayList<HashMap<String, Object>>();
		}

	}

	/*
	 * this method is executed when there are more than
	 * this.messagesPerPrediction elements in this.messages
	 */
	private void runPrediction() {
		String predictionString = "";
		ArrayList<MessageLocationPrediction> messageLocationPredictions = new ArrayList<MessageLocationPrediction>();

		for (HashMap<String, Object> message : messages) {
			String messageTextIndices = (String) message
					.get("messageTextIndices");
			if (messageTextIndices.isEmpty()) {
				continue;
			}
			if (!predictionString.isEmpty()) {
				predictionString += "\n";
			}
			predictionString += messageTextIndices;
			MessageLocationPrediction messageLocationPrediction = new MessageLocationPrediction(
					message);
			messageLocationPredictions.add(messageLocationPrediction);
		}
		String prediction;
		if (predictionString.isEmpty()) {
			prediction = "";
		} else {
			prediction = PredictLocation.predict(predictionString,
					this.localTopicModelDirectory);
		}

		// store coordinates with according probabilities in the according
		// MessageLocationPrediction
		String[] coordinates = prediction.split("\\n");
		for (String coordinate : coordinates) {
			String[] coordinateSplit = coordinate.split("\\s");
			if (coordinateSplit.length < 3) {
				continue;
			}
			String coordinateString = coordinateSplit[0] + " "
					+ coordinateSplit[1];
			String[] probabilities = coordinateSplit[2].split(",");
			if (probabilities.length != messageLocationPredictions.size()) {
				throw new IllegalStateException(
						"probabilities.length!=messageList.size()");
			}

			for (int probabilitiesIndex = 0; probabilitiesIndex < probabilities.length; probabilitiesIndex++) {
				messageLocationPredictions
						.get(probabilitiesIndex)
						.addLocationProbability(
								coordinateString,
								Double.parseDouble(probabilities[probabilitiesIndex]));
			}
		}
		for (MessageLocationPrediction messageLocationPrediction : messageLocationPredictions) {
			messageLocationPrediction.message
					.put("ukob:topic_set",
							messageLocationPrediction
									.getTopLocationsWithProbability(this.locationsPerMessage));
			// remove unnessary data from message
			ArrayList<String> keysToRemove = new ArrayList<String>();
			for (String item : messageLocationPrediction.message.keySet()) {
				if (!item.equals("itinno:item_id")
						&& !item.equals("ukob:topic_set")) {
					keysToRemove.add(item);
				}
			}
			for (String keyToRemove : keysToRemove) {
				messageLocationPrediction.message.remove(keyToRemove);
			}
		}

	}
}
