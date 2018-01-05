package com.mapr.examples;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.io.IOException;
import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.*;

public class PojoConsumer {
    // Declare a new consumer.
    public static KafkaConsumer consumer;

    public static void main(String[] args) {
        Runtime runtime = Runtime.getRuntime();
        if (args.length != 1) {
            System.err.println("ERROR: You must specify a stream:topic to consume data from.");
            System.err.println("USAGE:\n" +
                    "\tjava -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.PojoConsumer stream:topic\n" +
                    "Example:\n" +
                    "\tjava -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.PojoConsumer /user/mapr/mystream:mytopic");
            runtime.exit(1);
        }

        String topic = args[0];
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
                ConsumerRecords<String, byte[]> records = consumer.poll(100);
                if (records.count() > 0) {
                    System.out.println("Consumed " + records.count() + " messages");
                    for (ConsumerRecord<String, byte[]> record : records) {
                        records_processed++;
                        //Create object from bytes:
                        ByteArrayInputStream bis = new ByteArrayInputStream(record.value());
                        ObjectInput in = new ObjectInputStream(bis);
                        try {
                            Object obj = in.readObject();
                            Person p2 = (Person) obj;
                            System.out.printf("offset = %d, pojo id = '%s'\n",
                                    record.offset(), p2.getId());
                        } finally {
                            try {
                                if (in != null) {
                                    in.close();
                                }
                            } catch (IOException ex) {
                                System.err.printf("%s", ex.getStackTrace());
                            }
                        }
                    }
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
        consumer = new KafkaConsumer<String, String>(props);
    }

}