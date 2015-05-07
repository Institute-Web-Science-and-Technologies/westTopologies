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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

/**
 *
 * @author nico
 */
public class DiscussionTreeBolt extends BaseRichBolt {

    private OutputCollector collector;
    private String strExampleEmitFieldsId;

    private DateTime deadline;

    private DateTime bufferStartTime = null;

    private int intervalInMinutes = 10;
    private Map<String, Tweet> rootTweetsMap = new LinkedHashMap<>();
    private Map<String, Tweet> childrenTweetsMap = new HashMap<>();

    public DiscussionTreeBolt(String strExampleEmitFieldsId) {
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

        if (!message.containsKey("created_at")) {
            return;     // skip delete messages
        }
        // Print received message
//        this.logger.info("Received message: " + message.toJSONString());

        String timeStamp = (String) message.get("created_at");
        DateTime timestamp = DateTime.parse(timeStamp, DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.US));

        if (bufferStartTime == null) {
            bufferStartTime = timestamp;
            deadline = bufferStartTime.plusMinutes(intervalInMinutes);
        }

        String authorId = (String) ((Map<Object, Object>) message.get("user")).get("id_str");
        String authorScreenName = (String) ((Map<Object, Object>) message.get("user")).get("screen_name");
        String text = (String) message.get("text");
        String tweetId = (String) message.get("id_str");
        boolean retweet = false;

        String ancestorTweetId = (String) message.get("in_reply_to_status_id_str");
        String ancestorAuthorId = (String) message.get("in_reply_to_user_id_str");
        String ancestorAutorScreenName = (String) message.get("in_reply_to_screen_name");

        Map<Object, Object> retweeted_status = (Map<Object, Object>) message.get("retweeted_status");
        if (retweeted_status != null) {
            retweet = true;
            ancestorTweetId = (String) ((Map<Object, Object>) message.get("retweeted_status")).get("id_str");
        }

        Tweet tweet = new Tweet(authorId, authorScreenName, tweetId, timestamp, text, ancestorTweetId, true, retweet);

        if (ancestorTweetId != null) {
            if (rootTweetsMap.containsKey(tweet.getIn_reply_to())) {
                rootTweetsMap.get(tweet.getIn_reply_to()).getReplies().add(tweet);
            } else if (childrenTweetsMap.containsKey(tweet.getIn_reply_to())) {
                childrenTweetsMap.get(tweet.getIn_reply_to()).getReplies().add(tweet);
            } else {
                // tweet is a reply or retweet but its ancestor was'nt observed by this bolt, therefore its ancestor is treated as a dummy entry
                Tweet dummyTweet = new Tweet(ancestorAuthorId, ancestorAutorScreenName, ancestorTweetId, null, null, null, false, false);
                dummyTweet.getReplies().add(tweet);
                rootTweetsMap.put(ancestorTweetId, dummyTweet);
            }
            childrenTweetsMap.put(tweetId, tweet);
        } else {
            // tweet is no reply or retweet 
            rootTweetsMap.put(tweetId, tweet);
        }

        if (timestamp.isAfter(deadline) || timestamp.isEqual(deadline)) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                String jsonResultString;
                HashMap<String, Object> jsonResult = new HashMap<>();
                jsonResult.put("start", bufferStartTime.toString());
                jsonResult.put("end", timestamp.toString());
                jsonResult.put("result", rootTweetsMap.values());
                jsonResultString = mapper.writeValueAsString(jsonResult);
                Logger.getLogger(DiscussionTreeBolt.class.getName()).log(Level.INFO, "Deadline expired, Buffer size : " + rootTweetsMap.size());
                this.collector.emit(new Values(jsonResultString));
//                mapper.enable(SerializationFeature.INDENT_OUTPUT);
//                mapper.writeValue(new File("/home/nico/storm_topology_dir/logs/discussionTree-" + bufferStartTime), jsonResult);
                bufferStartTime = null;
                this.rootTweetsMap = new LinkedHashMap<>();
                this.childrenTweetsMap = new HashMap<>();
            } catch (JsonProcessingException ex) {
                Logger.getLogger(DiscussionTreeBolt.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(DiscussionTreeBolt.class.getName()).log(Level.SEVERE, null, ex);
            }
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

    public class Tweet {

        private String author_id;
        private String author_screen_name;
        private String tweet_id;
        @JsonSerialize(using = ToStringSerializer.class)
        private DateTime timestamp;
        private String text;
        private String in_reply_to;
        private boolean observed;
        private boolean retweet;
        private List<Tweet> replies = new ArrayList<>();

        public Tweet(String authorId, String authorScreenName, String tweetId, DateTime timestamp, String text, String inReplyTo, boolean observed, boolean retweet) {
            this.author_id = authorId;
            this.author_screen_name = authorScreenName;
            this.tweet_id = tweetId;
            this.timestamp = timestamp;
            this.text = text;
            this.in_reply_to = inReplyTo;
            this.observed = observed;
            this.retweet = retweet;
        }

        /**
         * @return the author_id
         */
        public String getAuthor_id() {
            return author_id;
        }

        /**
         * @param author_id the author_id to set
         */
        public void setAuthor_id(String author_id) {
            this.author_id = author_id;
        }
        
        /**
         * @return the author_screen_name
         */
        public String getAuthor_screen_name() {
            return author_screen_name;
        }

        /**
         * @param author_screen_name the author_screen_name to set
         */
        public void setAuthor_screen_name(String author_screen_name) {
            this.author_screen_name = author_screen_name;
        }

        /**
         * @return the tweet_id
         */
        public String getTweet_id() {
            return tweet_id;
        }

        /**
         * @param tweet_id the tweet_id to set
         */
        public void setTweet_id(String tweet_id) {
            this.tweet_id = tweet_id;
        }

        /**
         * @return the timestamp
         */
        public DateTime getTimestamp() {
            return timestamp;
        }

        /**
         * @param timestamp the timestamp to set
         */
        public void setTimestamp(DateTime timestamp) {
            this.timestamp = timestamp;
        }

        /**
         * @return the text
         */
        public String getText() {
            return text;
        }

        /**
         * @param text the text to set
         */
        public void setText(String text) {
            this.text = text;
        }

        /**
         * @return the in_reply_to
         */
        public String getIn_reply_to() {
            return in_reply_to;
        }

        /**
         * @param in_reply_to the in_reply_to to set
         */
        public void setIn_reply_to(String in_reply_to) {
            this.in_reply_to = in_reply_to;
        }

        /**
         * @return the observed
         */
        public boolean isObserved() {
            return observed;
        }

        /**
         * @param observed the observed to set
         */
        public void setObserved(boolean observed) {
            this.observed = observed;
        }

        /**
         * @return the retweet
         */
        public boolean isRetweet() {
            return retweet;
        }

        /**
         * @param retweet the retweet to set
         */
        public void setRetweet(boolean retweet) {
            this.retweet = retweet;
        }

        /**
         * @return the replies
         */
        public List<Tweet> getReplies() {
            return replies;
        }

        /**
         * @param replies the replies to set
         */
        public void setReplies(List<Tweet> replies) {
            this.replies = replies;
        }

        @Override
        public String toString() {
            try {
                ObjectMapper mapper = new ObjectMapper();
                return mapper.writeValueAsString(this);
            } catch (JsonProcessingException ex) {
                Logger.getLogger(DiscussionTreeBolt.class.getName()).log(Level.SEVERE, null, ex);
            }
            return null;
        }
    }
}
