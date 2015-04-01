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
package uniko.west.westtopology;

// General helper imports
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Date;
import org.slf4j.LoggerFactory;

/**
 * Example java RabbitMQ publisher for use in sending JSON objects to Storm
 * spout
 *
 */
public class TwitterLogSender {

    public static void main(String[] args) {
        Logger logger = null;
        String patternLayout = "%5p %d{yyyy-MM-dd HH:mm:ss,sss} %file %t %L: %m%n";

        try {
            // Initialise logger context and logger pattern encoder
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            PatternLayoutEncoder patternLayoutEncoder = new PatternLayoutEncoder();

            // Set logging pattern (UTF-8 log)
            // logging patterns defined at http://logback.qos.ch/manual/layouts.html
            patternLayoutEncoder.setPattern(patternLayout);
            patternLayoutEncoder.setContext(loggerContext);
            patternLayoutEncoder.setCharset(java.nio.charset.Charset.forName("UTF-8"));
            patternLayoutEncoder.start();

            // log to console
            ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<ILoggingEvent>();
            appender.setContext(loggerContext);
            appender.setEncoder(patternLayoutEncoder);
            appender.start();

            // Finally setup the logger itself
            logger = (Logger) LoggerFactory.getLogger(TwitterLogSender.class.getName());
            logger.addAppender(appender);
            logger.setLevel(Level.DEBUG);
            logger.setAdditive(false);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        logger.info("rabitmq client started");

        // send a UTF-8 encoded JSON tweet to the RabbitMQ (for stormspout to pick up and send to bolt)
        try {

            String strJSONFilePath = "/home/nico/rawtweet-log-1426668618964.log";

            BufferedReader br = new BufferedReader(new FileReader(strJSONFilePath));

            // read UTF-8 JSON text from file
            logger.info("ExampleRabbitmqClient started");

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
                channel.basicPublish("test-exchange", "test-routing", basicProperties.build(), jsonLine.getBytes("UTF-8"));
            }
            // close connection and channel
            channel.close();
            connection.close();

            logger.info("ExampleRabbitmqClient finished");

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            System.exit(1);
        }
    }
}
