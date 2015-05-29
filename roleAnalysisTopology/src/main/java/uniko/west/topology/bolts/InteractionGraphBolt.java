/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.topology.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

/**
 *
 * @author nico
 */
public class InteractionGraphBolt extends BaseRichBolt {

    private OutputCollector collector;
    private String strExampleEmitFieldsId;

    private DateTime deadline;
    private int intervalInMinutes = 10;
    private DateTime bufferStartTime = null;
    private HashMap<String, HashMap<String, ArrayList<Interaction>>> interactionGraph = new HashMap<>();

    public InteractionGraphBolt(String strExampleEmitFieldsId) {
        super();

        this.strExampleEmitFieldsId = strExampleEmitFieldsId;
    }

    /**
     * Prepare method is similar the "Open" method for Spouts and is called when
     * a worker is about to be put to work. This method also initialise the main
     * example Storm Java bolt.
     *
     * @param stormConf map of the storm configuration (passed within Storm
     * topology itself, not be a user)
     * @param context context (e.g. similar to description) of the topology
     * (passed within Storm topology itself, not be a user)
     * @param collector output collector of the Storm (which is responsible to
     * emiting new tuples, passed within Storm topology itself, not be a user)
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * searches for locations in the message and computes related locations
     *
     * @param input standard Storm tuple input object (passed within Storm
     * topology itself, not be a user)
     */
    @Override
    public void execute(Tuple input) {
        // Retrieve hash map tuple object from Tuple input at index 0, index 1 will be message delivery tag (not used here)
        Map<Object, Object> inputMap = (HashMap<Object, Object>) input.getValue(0);
        // Get JSON object from the HashMap from the Collections.singletonList
        Map<Object, Object> message = (Map<Object, Object>) inputMap.get("message");
        
        // Acknowledge the collector that we actually received the input
        collector.ack(input);
        
        if(!message.containsKey("created_at")) {
            return;     // skip delete messages
        }
        
        DateTime timestamp = DateTime.parse((String) message.get("created_at"), DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.US));
        
        if (bufferStartTime == null) {
            bufferStartTime = timestamp;
            deadline = bufferStartTime.plusMinutes(intervalInMinutes);
        }
        
        String authorId = (String) ((Map<Object, Object>) message.get("user")).get("id_str");

        if (!interactionGraph.containsKey(authorId)) {
            interactionGraph.put(authorId, new HashMap<String, ArrayList<Interaction>>());
        }
        HashMap<String, ArrayList<Interaction>> authorActions = interactionGraph.get(authorId);

        countReplies(message, authorActions);
        countMentions(message, authorActions);
        countRetweets(message, authorActions);

        if (timestamp.isAfter(deadline) || timestamp.isEqual(deadline)) {
            deadline.plusMinutes(intervalInMinutes);
            ObjectMapper mapper = new ObjectMapper();
            String jsonResult;
            try {
                Map<String, Object> jsonResultObject = new HashMap();
                jsonResultObject.put("start", bufferStartTime.toString());
                jsonResultObject.put("end", timestamp.toString());
                jsonResultObject.put("flat_graph", flattenGraph(interactionGraph));
                jsonResultObject.put("verbose_graph", interactionGraph);
                jsonResult = mapper.writeValueAsString(jsonResultObject);
                Logger.getLogger(DiscussionTreeBolt.class.getName()).log(Level.INFO, "Deadline expired, Buffer size : " + interactionGraph.size());
                this.collector.emit(new Values(jsonResult));
//                mapper.enable(SerializationFeature.INDENT_OUTPUT);
//                mapper.writeValue(new File("/home/nico/storm_topology_dir/logs/interactionGraph-"+bufferStartTime), jsonResultObject);
                this.interactionGraph = new HashMap<>();
                this.bufferStartTime = null;
            } catch (JsonProcessingException ex) {
                Logger.getLogger(InteractionGraphBolt.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            } 
//            catch (IOException ex) {
//                Logger.getLogger(InteractionGraphBolt.class.getName()).log(Level.SEVERE, null, ex);
//            }
        }
    }

    private void countReplies(Map<Object, Object> message, HashMap<String, ArrayList<Interaction>> authorActions) {
        String replyId = (String) message.get("in_reply_to_user_id_str");

        if (replyId != null) {
            if (!authorActions.containsKey("replied_to")) {
                authorActions.put("replied_to", new ArrayList<Interaction>());
            }
            authorActions.get("replied_to").add(new Interaction(replyId, (String) message.get("created_at")));
        }
    }

    private void countMentions(Map<Object, Object> message, HashMap<String, ArrayList<Interaction>> authorActions) {
        List<Object> userMentions = (List<Object>) ((Map<Object, Object>) message.get("entities")).get("user_mentions");
        if (userMentions != null) {
            if (!authorActions.containsKey("mentioned")) {
                authorActions.put("mentioned", new ArrayList<Interaction>());
            }
            for (Object o : userMentions) {
                String mentionedUser = (String) ((Map<Object, Object>) o).get("id_str");
                authorActions.get("mentioned").add(new Interaction(mentionedUser, (String) message.get("created_at")));
            }
        }
    }

    private void countRetweets(Map<Object, Object> message, HashMap<String, ArrayList<Interaction>> authorActions) {
        Map<Object, Object> retweetStatus = (Map<Object, Object>) message.get("retweeted_status");
        if (retweetStatus != null) {
            if (!authorActions.containsKey("retweeted")) {
                authorActions.put("retweeted", new ArrayList<Interaction>());
            }
            String authorId = (String) ((Map<Object, Object>) retweetStatus.get("user")).get("id_str");
            authorActions.get("retweeted").add(new Interaction(authorId, (String) message.get("created_at")));
        }
    }

    /**
     * Declare output field name (in this case simple a string value that is
     * defined in the constructor call)
     *
     * @param declarer standard Storm output fields declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(strExampleEmitFieldsId));
    }

    private Object flattenGraph(HashMap<String, HashMap<String, ArrayList<Interaction>>> interactionGraph) {
        HashMap<String, HashMap<String, HashSet<String>>> result = new HashMap<>();
        for(Entry<String, HashMap<String, ArrayList<Interaction>>> userEntry : interactionGraph.entrySet()) {
            HashMap<String, ArrayList<Interaction>> interactions = userEntry.getValue();
            String userId = userEntry.getKey();
            HashMap<String, HashSet<String>> userResult = new HashMap<>();
            for(Entry<String, ArrayList<Interaction>> actionEntry : interactions.entrySet()) {
                HashSet<String> interactionPartners = new HashSet<>();
                String action = actionEntry.getKey();
                for(Interaction interAction : actionEntry.getValue()) {
                    interactionPartners.add(interAction.getUserId());
                }
                userResult.put(action, interactionPartners);
            }
            result.put(userId, userResult);
        }
        return result;
    }

    private static class Interaction {
        
        private String userId;
        private String timestamp;

        public Interaction(String userId, String timestamp) {
            this.userId = userId;
            this.timestamp = timestamp;
        }

        /**
         * @return the userId
         */
        public String getUserId() {
            return userId;
        }

        /**
         * @param userId the userId to set
         */
        public void setUserId(String userId) {
            this.userId = userId;
        }

        /**
         * @return the timestamp
         */
        public String getTimestamp() {
            return timestamp;
        }

        /**
         * @param timestamp the timestamp to set
         */
        public void setTimestamp(String timestamp) {
            this.timestamp = timestamp;
        }
    }

}
