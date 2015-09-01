package uniko.west.topology.datatypes;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MessageLocationPrediction {
	public HashMap<String, Object> message;
	public HashMap<String, Double> locationProbabilityMap;

	public MessageLocationPrediction(HashMap<String, Object> message) {
		this.message = message;
		locationProbabilityMap = new HashMap<String, Double>();
	}

	public void addLocationProbability(String location, Double probability) {
		locationProbabilityMap.put(location, probability);
	}

	public String getTopLocationsWithProbability(int numberOfLocations) {
		// approach taken form: http://stackoverflow.com/a/13913206/2174538
		List<Entry<String, Double>> list = new LinkedList<Entry<String, Double>>(
				locationProbabilityMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<String, Double>>() {
			public int compare(Entry<String, Double> o1,
					Entry<String, Double> o2) {
				return o2.getValue().compareTo(o1.getValue());

			}
		});

		// Maintaining insertion order with the help of LinkedList
		Map<String, Double> sortedLocationProbabilityMap = new LinkedHashMap<String, Double>();
		for (Entry<String, Double> entry : list) {
			sortedLocationProbabilityMap.put(entry.getKey(), entry.getValue());
		}

		String resultString = "[";
		int addedLocations = 0;
		for (Entry<String, Double> entry : sortedLocationProbabilityMap
				.entrySet()) {
			if (addedLocations == numberOfLocations) {
				break;
			}
			if (resultString.length() > 1) {
				resultString += "},";
			}
			// TODO format coordinates
			String[] coordinates = entry.getKey().split("\\s");
			resultString += "{\"location\":[" + entry.getKey()
					+ "],\"probability\":\"" + entry.getValue() + "\"";
			addedLocations++;
		}
		if (resultString.length() > 1) {
			resultString += "}";
		}
		resultString += "]";

		return resultString;

	}
}
