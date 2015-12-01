/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniko.west.topology.bolts;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.unikoblenz.west.reveal.analytics.CommunityAnalysis;
import de.unikoblenz.west.reveal.pserver.PServerConfiguration;
import de.unikoblenz.west.reveal.pserver.PServerRoleClient;
import de.unikoblenz.west.reveal.roles.RoleAssociation;
import de.unikoblenz.west.reveal.roles.UserWithFeatures;
import de.unikoblenz.west.reveal.roles.UserWithRole;
import de.unikoblenz.west.reveal.structures.Community;
import de.unikoblenz.west.reveal.twitter.storm.StormCommunityFactory;

/**
 *
 * @author nico
 */
public class RoleAnalysisBolt extends BaseRichBolt {

	private OutputCollector collector;
	private String strExampleEmitFieldsId;
	private String hostname;
	private String mode;
	private String clientName;
	private String clientPasswd;
	private boolean initServerData;

	/**
	 *
	 * @param strExampleEmitFieldsId
	 * @param hostname
	 * @param mode
	 * @param clientName
	 * @param clientPasswd
	 * @param initServerData
	 */
	public RoleAnalysisBolt(String strExampleEmitFieldsId, String hostname, String mode, String clientName,
			String clientPasswd, boolean initServerData) {
		super();
		this.strExampleEmitFieldsId = strExampleEmitFieldsId;
		this.hostname = hostname;
		this.mode = mode;
		this.clientName = clientName;
		this.clientPasswd = clientPasswd;
		this.initServerData = initServerData;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(this.strExampleEmitFieldsId));
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
			this.collector.ack(input);

			// Calling the factory methods to create the internal community
			// structures from the tweets in the files.
			Logger.getLogger(RoleAnalysisBolt.class.getName()).log(Level.INFO, "Constructing community ...");
			Community seCommunity = StormCommunityFactory.parseCommunity("storm", message);
			int minLimit = 1;
			Logger.getLogger(RoleAnalysisBolt.class.getName()).log(Level.INFO,
					"Community Analysis, using minlimit: " + minLimit);
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

			try (PrintStream out = new PrintStream(
					new File("/home/martin/test/roleAnalysisBolt/out" + message.hashCode() + ".log"), "UTF8")) {
				for (UserWithRole uwr : users) {
					out.println(uwr.id + "\t" + uwr.username + "\t" + uwr.role);
				}
			} catch (FileNotFoundException ex) {
				Logger.getLogger(RoleAnalysisBolt.class.getName()).log(Level.SEVERE, null, ex);
			} catch (UnsupportedEncodingException ex) {
				Logger.getLogger(RoleAnalysisBolt.class.getName()).log(Level.SEVERE, null, ex);
			}
			PServerConfiguration config = new PServerConfiguration();
			config.setClientName(this.clientName);
			config.setClientPass(this.clientPasswd);
			config.setHost(this.hostname);
			config.setMode(this.mode);
			PServerRoleClient pservRoleClient = new PServerRoleClient(config);

			// if(initServerData) {
			// pservRoleClient.initializePserverModel();
			// }
			// for (UserWithRole u : users) {
			// pservRoleClient.addUser(u);
			// }

		} catch (IOException ex) {
			Logger.getLogger(RoleAnalysisBolt.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

}
