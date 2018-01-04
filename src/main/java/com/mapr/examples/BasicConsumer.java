package com.mapr.examples;/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

import java.io.IOException;
import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.*;

public class BasicConsumer {
    // Declare a new consumer.
    public static KafkaConsumer consumer;

    public static void main(String[] args) {

        Runtime runtime = Runtime.getRuntime();
        if (args.length < 2) {
            System.err.println("ERROR: You must specify a stream:topic to consume data from.");
            System.err.println("USAGE:\n" +
                    "\tjava -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer [stream:topic]\n" +
                    "Example:\n" +
                    "\tjava -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer /user/mapr/mystream:mytopic");

        }

        String topic =  args[1] ;
        System.out.println("Subscribed to : "+ topic);

        configureConsumer();

        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        // Subscribe to the topic.
        consumer.subscribe(topics);

        long pollTimeOut = 100;  // milliseconds
        long records_processed = 0L;

        long startTime = System.nanoTime();
        long last_update = 0;

        try {
            while (true) {
                // Request unread messages from the topic.
                ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
                if (records.count() > 0) {
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(record);
                    }
                    consumer.commitSync();

                    records_processed += records.count();
                }

            }
        } catch (Throwable throwable) {
            System.err.printf("%s", throwable.getStackTrace());
        } finally {
            consumer.close();
            System.out.println("Consumed " + records_processed + " messages from stream.");
            System.out.println("Finished.");
        }

    }

    public static void configureConsumer() {
        Properties props = new Properties();
        try {
            props.load(Resources.getResource("consumer.props").openStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
        props.put("enable.auto.commit","false");
        props.put("group.id", "mapr-workshop");

        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
    }

}
