/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.topology.bolts;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Level;
import java.util.logging.Logger;

import jgibblda.Dictionary;

import org.apache.commons.io.IOUtils;

import util.FileLoader;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import ckling.text.Text;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

/**
 *
 * @author Martin Koerner <info@mkoerner.de> and nico
 */
public class TweetIndexBolt extends BaseRichBolt {

	/**
     *
     */
	private static final long serialVersionUID = -8319149705129653575L;
	private OutputCollector collector;
	private String strExampleEmitFieldsId;

	private String urlToWordMap;
	private Dictionary dictionary;
	private String pathToLocalWordMap;

	public TweetIndexBolt(String strExampleEmitFieldsId, String urlToWordMap) {
		super();

		this.strExampleEmitFieldsId = strExampleEmitFieldsId;
		this.urlToWordMap = urlToWordMap;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(this.strExampleEmitFieldsId));
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			File topologyJarFile = new File(TweetIndexBolt.class
					.getProtectionDomain().getCodeSource().getLocation()
					.toURI().getPath());
			this.pathToLocalWordMap = topologyJarFile.getParent()
					+ "/topic-model/wordmap.txt";
			new File(new File(pathToLocalWordMap).getParent()).mkdirs();

			FileLoader.getFile(urlToWordMap, pathToLocalWordMap);

			this.dictionary = new Dictionary();
			this.dictionary.readWordMap(pathToLocalWordMap);
		} catch (URISyntaxException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// for language detection
		String dirname = "profiles/";
		Enumeration<URL> en;
		try {
			en = Detector.class.getClassLoader().getResources(dirname);
			List<String> profiles = new ArrayList<>();
			if (en.hasMoreElements()) {
				URL url = en.nextElement();
				JarURLConnection urlcon = (JarURLConnection) url
						.openConnection();
				try (JarFile jar = urlcon.getJarFile();) {
					Enumeration<JarEntry> entries = jar.entries();
					while (entries.hasMoreElements()) {
						String entry = entries.nextElement().getName();
						if (entry.startsWith(dirname)) {
							try (InputStream in = Detector.class
									.getClassLoader()
									.getResourceAsStream(entry);) {
								profiles.add(IOUtils.toString(in));
							}
						}
					}
				}
			}

			DetectorFactory.loadProfile(profiles);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (LangDetectException e) {
			e.printStackTrace();
		}

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

		// only consider tweets that include a text element
		if (!message.containsKey("text")) {
			return;
		}
		// extract tweet text
		String messageText = (String) message.get("text");

		// check the language in which the tweet was written
		Detector detector;
		String langDetected;
		try {
			detector = DetectorFactory.create();
			detector.append(messageText);
			langDetected = detector.detect();
			// only consider tweets written in Enlglish
			if (!langDetected.equals("en")) {
				return;
			}
		} catch (LangDetectException e) {
			e.printStackTrace();
			// return if language could not be determined
			return;
		}

		// create indices for calling the topic model predictor
		Text text = new Text(messageText);
		String messageTextIndices = "";
		text.stem = true;

		Iterator<String> iterator = text.getTerms();
		String term;
		while (iterator.hasNext()) {
			term = iterator.next();
			if (dictionary.contains(term)) {
				if (!messageTextIndices.isEmpty()) {
					messageTextIndices += " ";
				}
				messageTextIndices += dictionary.getID(term);
			}
		}

		ArrayList<Object> result = new ArrayList<Object>();
		message.put("messageTextIndices", messageTextIndices);

		result.add((Object) message);
		this.collector.emit(result);

		// test printout
		try (PrintStream testOut = new PrintStream(new File(
				"/home/martin/test/tweetIndexBolt/location"
						+ message.hashCode() + ".log"), "UTF8")) {
			testOut.println("text: " + messageText);
			testOut.println("detected language: " + langDetected);
			testOut.println("indicies: " + messageTextIndices);

		} catch (FileNotFoundException ex) {
			Logger.getLogger(TweetIndexBolt.class.getName()).log(Level.SEVERE,
					null, ex);
		} catch (UnsupportedEncodingException ex) {
			Logger.getLogger(TweetIndexBolt.class.getName()).log(Level.SEVERE,
					null, ex);
		}

	}
}
