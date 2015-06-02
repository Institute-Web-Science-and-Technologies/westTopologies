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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import jgibblda.Dictionary;
import jgibblda.PredictLocation;
import uniko.west.topology.datatypes.MessageLocationPrediction;
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

	private Dictionary dictionary;
	private HashSet<HashMap<String, Object>> messages;
	private int messagesPerPrediction;
	private String topicModelDirectory;

	public TopicModelBolt(String strExampleEmitFieldsId,
			String topicModelDictionary) {
		super();

		this.strExampleEmitFieldsId = strExampleEmitFieldsId;
		this.topicModelDirectory = topicModelDictionary;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(this.strExampleEmitFieldsId));
	}

	// TODO remove SuppressWarnings
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.messages = new HashSet<HashMap<String, Object>>();
		this.messagesPerPrediction = 10;

	}

	@Override
	public void execute(Tuple input) {
		@SuppressWarnings("unchecked")
		HashMap<String, Object> message = (HashMap<String, Object>) input
				.getValue(0);
		this.messages.add(message);

		if (messages.size() >= this.messagesPerPrediction) {
			this.runPrediction();

			// TODO remove testOut
			try (PrintStream testOut = new PrintStream(new File(
					"/home/martin/test/topicModelBolt/location"
							+ message.hashCode() + ".log"), "UTF8")) {
				//
				testOut.println("text: " + message.get("text"));
				testOut.println("messageTextIndices: "
						+ message.get("messageTextIndices"));
				testOut.println("prediction: "
						+ message.get("topicModelPrediction"));

			} catch (FileNotFoundException ex) {
				Logger.getLogger(TopicModelBolt.class.getName()).log(
						Level.SEVERE, null, ex);
			} catch (UnsupportedEncodingException ex) {
				Logger.getLogger(TopicModelBolt.class.getName()).log(
						Level.SEVERE, null, ex);
			}

			this.messages = new HashSet<HashMap<String, Object>>();
		}

	}

	/*
	 * this method is executed when there are more than
	 * this.messagesPerPrediction messages
	 */
	private void runPrediction() {
		String predictionString = "";
		ArrayList<MessageLocationPrediction> messageLocationPredictionList = new ArrayList<MessageLocationPrediction>();

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
			messageLocationPredictionList.add(messageLocationPrediction);
		}
		String prediction;
		if (predictionString.isEmpty()) {
			prediction = "";
		} else {
			prediction = PredictLocation.predict(predictionString,
					this.topicModelDirectory);
		}

		String[] coordinates = prediction.split("\\n");
		for (String coordinate : coordinates) {
			String[] coordinateSplit = coordinate.split("\\s");
			if (coordinateSplit.length < 3) {
				continue;
			}
			String coordinateString = coordinateSplit[0] + " "
					+ coordinateSplit[1];
			String[] probabilities = coordinateSplit[2].split(",");
			if (probabilities.length != messageLocationPredictionList.size()) {
				throw new IllegalStateException(
						"probabilities.length!=messageList.size()");
			}

			for (int probabilitiesIndex = 0; probabilitiesIndex < probabilities.length; probabilitiesIndex++) {
				messageLocationPredictionList
						.get(probabilitiesIndex)
						.addLocationProbability(
								coordinateString,
								Double.parseDouble(probabilities[probabilitiesIndex]));
			}

		}
		// TODO: sort and store probabilities
	}
}
