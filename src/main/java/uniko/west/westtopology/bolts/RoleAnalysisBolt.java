/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.westtopology.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.unikoblenz.west.reveal.analytics.CommunityAnalysis;
import de.unikoblenz.west.reveal.roles.RoleAssociation;
import de.unikoblenz.west.reveal.roles.UserWithFeatures;
import de.unikoblenz.west.reveal.roles.UserWithRole;
import de.unikoblenz.west.reveal.structures.Community;
import de.unikoblenz.west.reveal.twitter.storm.StormCommunityFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author nico
 */
public class RoleAnalysisBolt extends BaseRichBolt {

    private OutputCollector collector;
    private String strExampleEmitFieldsId;

    public RoleAnalysisBolt(String strExampleEmitFieldsId) {
        super();

        this.strExampleEmitFieldsId = strExampleEmitFieldsId;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(strExampleEmitFieldsId));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> message;
        try {
            message = mapper.readValue((String) input.getValue(0), Map.class);

            // Acknowledge the collector that we actually received the input
            collector.ack(input);

            // Calling the factory methods to create the internal community structures from the tweets in the files. 
            Logger.getLogger(RoleAnalysisBolt.class.getName()).log(Level.INFO, "Constructing community ...");
            Community seCommunity = StormCommunityFactory.parseCommunity("storm", message);
            int minLimit = 1;
            Logger.getLogger(RoleAnalysisBolt.class.getName()).log(Level.INFO, "Community Analysis, using minlimit: " + minLimit);
            HashSet<UserWithFeatures> uwf = CommunityAnalysis.analyseUserFeatures(seCommunity, minLimit);

            Logger.getLogger(RoleAnalysisBolt.class.getName()).log(Level.INFO, "Converting users ...");
            // Convert into UserWithRole objects suitable for Role analysis
            HashSet<UserWithRole> users = new HashSet<UserWithRole>();
            for (UserWithFeatures userFeatures : uwf) {
                UserWithRole u = userFeatures.convertToUserWithRole();
                users.add(u);
            }

            // Actual role analysis
            Logger.getLogger(RoleAnalysisBolt.class.getName()).log(Level.INFO, "Processing users (Role Analysis) ...");
            RoleAssociation ra = new RoleAssociation();
            ra.process(users);

            try (PrintStream out = new PrintStream(new File("/home/nico/storm_topology_dir/logs/out" + message.hashCode() + ".log"), "UTF8")) {
                for (UserWithRole uwr : users) {
                    out.println(uwr.id + "\t" + uwr.username + "\t" + uwr.role);
                }
            } catch (FileNotFoundException ex) {
                Logger.getLogger(RoleAnalysisBolt.class.getName()).log(Level.SEVERE, null, ex);
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(RoleAnalysisBolt.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (IOException ex) {
            Logger.getLogger(RoleAnalysisBolt.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
