/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.westtopology.bolts;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import jgibblda.Dictionary;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import ckling.text.Text;

/**
 *
 * @author Martin Koerner <info@mkoerner.de> and nico
 */
public class LocationTopicModelBolt extends BaseRichBolt {

	/**
     *
     */
	private static final long serialVersionUID = -8319149705129653575L;
	private OutputCollector collector;
	private String strExampleEmitFieldsId;

	private String pathToWordMap;
	private Dictionary dictionary;

	public LocationTopicModelBolt(String strExampleEmitFieldsId,
			String pathToWordMap) {
		super();

		this.strExampleEmitFieldsId = strExampleEmitFieldsId;
		this.pathToWordMap = pathToWordMap;
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
		this.dictionary = new Dictionary();
		this.dictionary.readWordMap(this.pathToWordMap);

	}

	@Override
	public void execute(Tuple input) {
		// Retrieve hash map tuple object from Tuple input at index 0, index 1
		// will be message delivery tag (not used here)
		@SuppressWarnings("unchecked")
		Map<Object, Object> inputMap = (HashMap<Object, Object>) input
				.getValue(0);
		// Get JSON object from the HashMap from the Collections.singletonList
		@SuppressWarnings("unchecked")
		Map<String, Object> message = (Map<String, Object>) inputMap
				.get("message");
		this.collector.ack(input);

		if (!message.containsKey("created_at")) {
			return; // skip delete messages
		}

		// TODO remove testOut
		try (PrintStream testOut = new PrintStream(new File(
				"/home/martin/test/location" + message.hashCode() + ".log"),
				"UTF8")) {
			// for (Entry<String, Object> x : message.entrySet()) {
			// testOut.println(x.getKey() + " xxx " + x.getValue());
			// }
			//
			// testOut.println("text:");
			// testOut.println(message.get("text"));
			// testOut.println("input length " + input.size());
			testOut.println("size: " + this.dictionary.id2word.size());
			testOut.println("test: " + this.dictionary.getWord(5));
			testOut.println("test: " + this.dictionary.getID("are"));

			// Acknowledge the collector that we actually received the input
			this.collector.ack(input);

			if (message.containsValue("text")) {
				// extract tweet text
				String messageText = (String) message.get("text");
				testOut.println("text: " + messageText);
				Text text = new Text(messageText);
				// TODO: check for different languages and set in text

				// map words to indices

				String messageTextIndices = "";
				text.stem = false;
				// TODO: check for different languages and set in text
				Iterator<String> iterator = text.getTerms();
				String term;
				while (iterator.hasNext()) {
					term = iterator.next();
					testOut.println(term);
					if (this.dictionary.contains(term)) {
						messageTextIndices += this.dictionary.getID(term) + " ";
					}
				}
				testOut.println(messageTextIndices);
				// TODO: call topic model method for calculating
				// TODO: add result to json
				// TODO: send result over rabbitmq

			} else {
				// no text found
				return;
			}

			testOut.println("end of test printout");
		} catch (FileNotFoundException ex) {
			Logger.getLogger(LocationTopicModelBolt.class.getName()).log(
					Level.SEVERE, null, ex);
		} catch (UnsupportedEncodingException ex) {
			Logger.getLogger(LocationTopicModelBolt.class.getName()).log(
					Level.SEVERE, null, ex);
		}

	}
}
