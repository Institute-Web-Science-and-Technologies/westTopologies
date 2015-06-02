package uniko.west.topology.datatypes;

import java.util.HashMap;

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
}
