package uniko.west.topology.datatypes;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONArray;
import org.json.JSONObject;

public class MessageLocationPrediction {
	public HashMap<String, Object> message;
	public HashMap<String, Float> locationProbabilityMap;

	public MessageLocationPrediction(HashMap<String, Object> message) {
		this.message = message;
		this.locationProbabilityMap = new HashMap<String, Float>();
	}

	public void addLocationProbability(String location, Float probability) {
		this.locationProbabilityMap.put(location, probability);
	}

	public JSONArray getTopLocationsWithProbability(int numberOfLocations) {
		// approach taken form: http://stackoverflow.com/a/13913206/2174538
		List<Entry<String, Float>> list = new LinkedList<Entry<String, Float>>(this.locationProbabilityMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<String, Float>>() {
			@Override
			public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
				return o2.getValue().compareTo(o1.getValue());

			}
		});

		// Maintaining insertion order with the help of LinkedList
		Map<String, Float> sortedLocationProbabilityMap = new LinkedHashMap<String, Float>();
		for (Entry<String, Float> entry : list) {
			sortedLocationProbabilityMap.put(entry.getKey(), entry.getValue());
		}		
		

		JSONArray results = new JSONArray();

		int addedLocations = 0;
		for (Entry<String, Float> entry : sortedLocationProbabilityMap.entrySet()) {
			if (addedLocations == numberOfLocations) {
				break;
			}
			// TODO format coordinates
			String[] coordinates = entry.getKey().split("\\s");
			JSONObject result = new JSONObject();
			result.put("location", entry.getKey());
			result.put("probability", entry.getValue());
			results.put(result);
			addedLocations++;
		}
		
		return results;

	}
}
