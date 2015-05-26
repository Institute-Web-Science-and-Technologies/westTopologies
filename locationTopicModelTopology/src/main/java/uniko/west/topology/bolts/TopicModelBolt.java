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

	public TopicModelBolt(String strExampleEmitFieldsId) {
		super();

		this.strExampleEmitFieldsId = strExampleEmitFieldsId;
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

	}

	@Override
	public void execute(Tuple input) {
		// Retrieve hash map tuple object from Tuple input at index 0, index 1
		// will be message delivery tag (not used here)
		// @SuppressWarnings("unchecked")
		// Map<Object, Object> inputMap = (HashMap<Object, Object>) input
		// .getValue(0);
		// TODO: create hashMap in input
		String message = (String) input.getValue(0);
		// // Get JSON object from the HashMap from the
		// Collections.singletonList

		// @SuppressWarnings("unchecked")
		// Map<String, Object> message = (Map<String, Object>) inputMap
		// .get("message");
		// this.collector.ack(input);

		// // create result string in JSON
		// // String jsonResultString = null;
		// // try {
		// // ObjectMapper mapper = new ObjectMapper();
		// // HashMap<String, Object> jsonResult = new HashMap<>();
		// // // TODO set result
		// // jsonResult.put("result", "");
		// // jsonResultString = mapper.writeValueAsString(jsonResult);
		// // } catch (JsonProcessingException ex) {
		// // Logger.getLogger(TopicModelBolt.class.getName()).log(
		// // java.util.logging.Level.SEVERE, null, ex);
		// // }
		// // // end result
		// // this.collector.emit(new Values(jsonResultString));

		// TODO remove testOut
		try (PrintStream testOut = new PrintStream(new File(
				"/home/martin/test/topicModelBolt/location"
						+ message.hashCode() + ".log"), "UTF8")) {
			//
			// testOut.println("text:");
			// testOut.println(message.get("text"));
			// testOut.println("input length " + input.size());
			// testOut.println("size: " + this.dictionary.id2word.size());
			// testOut.println("test: " + this.dictionary.getWord(5));
			// testOut.println("test: " + this.dictionary.getID("are"));

			// testOut.println("json-output: " + jsonResultString);
			// testOut.println("json-output: " + jsonResultString);
			testOut.println("message: " + message);

		} catch (FileNotFoundException ex) {
			Logger.getLogger(TopicModelBolt.class.getName()).log(Level.SEVERE,
					null, ex);
		} catch (UnsupportedEncodingException ex) {
			Logger.getLogger(TopicModelBolt.class.getName()).log(Level.SEVERE,
					null, ex);
		}

	}
}
