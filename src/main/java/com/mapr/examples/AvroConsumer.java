package com.mapr.examples;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AvroConsumer {
    // Declare a new consumer.
    public static KafkaConsumer consumer;
    private static Injection<GenericRecord, byte[]> recordInjection;

    public static void main(String[] args) {

        Runtime runtime = Runtime.getRuntime();
        if (args.length < 2) {
            System.err.println("ERROR: You must specify a stream:topic to consume data from.");
            System.err.println("USAGE:\n" +
                    "\tjava -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.Run avroconsumer [stream:topic]\n" +
                    "Example:\n" +
                    "\tjava -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.Run avroconsumer /user/mapr/mystream:mytopic");

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

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(AvroProducer.USER_SCHEMA);
        recordInjection = GenericAvroCodecs.toBinary(schema);

        try {
            while (true) {
                // Request unread messages from the topic.
                ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeOut);
                if (records.count() > 0) {
                    for (ConsumerRecord<String, byte[]> record : records) {
                        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
                        GenericRecord genericRecord = recordInjection.invert(record.value()).get();

                        System.out.println("str1= " + genericRecord.get("str1")
                                + ", str2= " + genericRecord.get("str2")
                                + ", int1=" + genericRecord.get("int1"));

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
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumer = new KafkaConsumer<String, byte[]>(props);
    }

}
