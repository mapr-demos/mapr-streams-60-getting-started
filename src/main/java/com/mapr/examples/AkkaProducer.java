package com.mapr.examples;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;

public class AkkaProducer {

    public static KafkaProducer producer;

    public static void main(String[] args) throws IOException {

        if (args.length != 2 && args.length != 3) {
            System.err.println("USAGE:\n" +
                    "\tjava -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.Run akkaproducer stream:topic input_json_file\n" +
                    "Example:\n" +
                    "\tjava -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.Run akkaproducer /user/mapr/mystream:mytopic input_data.json");

        }

        String topic =  args[1] ;
        System.out.println("Publishing to topic: "+ topic);
        configureProducer();

        BufferedReader reader = null;
        if (args.length == 3) {
            System.out.println("Opening file " + args[2]);
            File f = new File(args[2]);
            FileReader fr = new FileReader(f);
            reader = new BufferedReader(fr);
        } else {
            // read from stdin if input file not specified
            reader = new BufferedReader(new InputStreamReader(System.in));
        }

        String line = reader.readLine();
        long records_processed = 0L;
        long startTime = System.nanoTime();
        long last_update = 0;

        try {
            while (line != null) {
                ProducerRecord<String, String> rec = new ProducerRecord<>(topic, line);

                // Send the record to the producer client library.
                producer.send(rec);
                records_processed++;

                // Print performance stats once per second
                if ((Math.floor(System.nanoTime() - startTime) / 1e9) > last_update) {
                    last_update++;
                    producer.flush();
                    System.out.printf("Producer ");
                    PerfMonitor.print_status(records_processed, startTime);
                }
                //System.out.println("Sent message: " + line);
                line = reader.readLine();
            }

        } catch (Throwable throwable) {
            System.err.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
            System.out.println("Published " + records_processed + " messages to stream.");
            System.out.println("Finished.");
        }
    }

    public static void configureProducer() {
        Properties props = new Properties();
        try {
            props.load(Resources.getResource("producer.props").openStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }

}
