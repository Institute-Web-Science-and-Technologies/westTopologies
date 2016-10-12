/////////////////////////////////////////////////////////////////////////
//
// \xa9 University of Southampton IT Innovation, 2014
//
// Copyright in this software belongs to IT Innovation Centre of
// Gamma House, Enterprise Road, Southampton SO16 7NS, UK.
//
// This software may not be used, sold, licensed, transferred, copied
// or reproduced in whole or in part in any manner or form or in or
// on any media by any person other than in accordance with the terms
// of the Licence Agreement supplied with the software, or otherwise
// without the prior written consent of the copyright owners.
//
// This software is distributed WITHOUT ANY WARRANTY, without even the
// implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
// PURPOSE, except where stated in the Licence Agreement supplied with
// the software.
//
//	Created By :	Vadim Krivcov
//	Created Date :	2014/03/31
//	Created for Project:	REVEAL
//
/////////////////////////////////////////////////////////////////////////
//
// Dependencies: None
//
/////////////////////////////////////////////////////////////////////////
package uniko.west.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

// General helper imports
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Example java RabbitMQ publisher for use in sending JSON objects to Storm
 * spout
 *
 */
public class TwitterLogSender {

	public static void main(String[] args) {
/*		String strJSONFilePaths = { "/home/ubuntu/data/rawtweet-log-short.log",
				"/home/ubuntu/data/IITNNO-prepare-json-example.json",
				"/home/ubuntu/data/json/IITNNO-raw-JSON-example.json",
				"/home/ubuntu/data/json/ITINNO-aggregated-geoparse-example.json"
				"/home/ubuntu/data/json/CERTH-RSS-example.json" };
*/
		String strJSONFilePath = "/home/ubuntu/data/rawtweet-log-short.log"; //short twitter log
//		String strJSONFilePath = "/home/ubuntu/data/json/IITNNO-prepare-json-example.json";
		//String strJSONFilePath = "/home/ubuntu/data/json/IITNNO-raw-JSON-example.json"; //short itinno
		// example
//		String strJSONFilePath = "/home/ubuntu/data/json/ITINNO-aggregated-geoparse-example.json"; //another itinno example

		String exchangeName = "ukob_test";
//		for (String strJSONFilePath : strJSONFilePaths) {
			try (BufferedReader br = new BufferedReader(new FileReader(strJSONFilePath))) {

				// send a UTF-8 encoded JSON tweet to the RabbitMQ (for
				// stormspout
				// to pick up and send to bolt)
				// read UTF-8 JSON text from file
				Logger.getLogger(TwitterLogSender.class.getName()).log(Level.INFO, "ExampleRabbitmqClient started");

				// connect to rabbitmq broker
				// first of all create connection factory
				ConnectionFactory factory = new ConnectionFactory();
				factory.setUri("amqp://guest:guest@localhost:5672/%2F");
				// initialise connection and define channel
				Connection connection = factory.newConnection();
				Channel channel = connection.createChannel();
				String jsonLine;

				while ((jsonLine = br.readLine()) != null) {
					long timestampSinceEpoch = System.currentTimeMillis() / 1000;

					// initialise amqp basic peoperties object
					BasicProperties.Builder basicProperties = new AMQP.BasicProperties.Builder();
					basicProperties.build();
					basicProperties.timestamp(new Date(timestampSinceEpoch)).build();
					basicProperties.contentType("text/json").build();
					basicProperties.deliveryMode(1).build();

					// publish message
					channel.basicPublish(exchangeName, "test-routing", basicProperties.build(),
							jsonLine.getBytes("UTF-8"));
				}
				// close connection and channel
				channel.close();
				connection.close();

				Logger.getLogger(TwitterLogSender.class.getName()).log(Level.INFO, "ExampleRabbitmqClient finished");

			} catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException | IOException ex) {
				Logger.getLogger(TwitterLogSender.class.getName()).log(Level.SEVERE, null, ex);
//			}
		}
	}

}
