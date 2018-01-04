package com.mapr.examples;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

// some of this comes from http://aseigneurin.github.io/2016/03/04/kafka-spark-avro-producing-and-consuming-avro-messages.html
public class AvroProducer {

    public static KafkaProducer producer;

        public static final String USER_SCHEMA = "{"
                + "\"type\":\"record\","
                + "\"name\":\"myrecord\","
                + "\"fields\":["
                + "  { \"name\":\"str1\", \"type\":\"string\" },"
                + "  { \"name\":\"str2\", \"type\":\"string\" },"
                + "  { \"name\":\"int1\", \"type\":\"int\" }"
                + "]}";

        public static void main(String[] args) throws IOException {
            if (args.length != 2 && args.length != 3) {
                System.err.println("USAGE:\n" +
                        "\tjava -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.Run avroproducer stream:topic \n" +
                        "Example:\n" +
                        "\tjava -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.Run avroproducer /user/mapr/mystream:mytopic");

            }

            String topic =  args[1] ;
            System.out.println("Publishing to topic: "+ topic);
            configureProducer();

            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(USER_SCHEMA);
            Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

            for (int i = 0; i < 100; i++) {
                GenericData.Record avroRecord = new GenericData.Record(schema);
                avroRecord.put("str1", "Str 1-" + i);
                avroRecord.put("str2", "Str 2-" + i);
                avroRecord.put("int1", i);

                byte[] bytes = recordInjection.apply(avroRecord);

                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, bytes);
                producer.send(record);

                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

            producer.close();
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
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<String, byte[]>(props);
    }

}
