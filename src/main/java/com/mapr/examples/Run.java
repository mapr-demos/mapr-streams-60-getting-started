package com.mapr.examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * Pick whether we want to run as producer or consumer. This lets us
 * have a single executable as a build target.
 */
public class Run {
    public static void main(String[] args) throws IOException {
        Logger.getRootLogger().setLevel(Level.OFF);

        if (args.length < 1) {
            System.err.println("USAGE:\n" +
                    "\tjava -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer stream:topic [source data file]\n" +
                    "\tjava -cp ./mapr-streams-study-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer stream:topic\n");
        }
        switch (args[0]) {
            case "producer":
                BasicProducer.main(args);
                break;
            case "consumer":
                BasicConsumer.main(args);
                break;
            case "pojoconsumer":
                PojoConsumer.main(args);
                break;
            case "pojoproducer":
                PojoProducer.main(args);
                break;
            case "avroconsumer":
                AvroConsumer.main(args);
                break;
            case "avroproducer":
                AvroProducer.main(args);
                break;
            case "akkaconsumer":
                AkkaConsumer.main(args);
                break;
            case "akkaproducer":
                AkkaProducer.main(args);
                break;
            default:
                throw new IllegalArgumentException("Don't know how to do " + args[0]);
        }
    }
}