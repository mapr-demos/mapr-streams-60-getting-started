package com.mapr.examples;

import akka.actor.AbstractActor;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;
import java.io.Serializable;
import java.util.UUID;

import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;

public class AkkaPersister {
    private static long records_processed = 0L;
    public static long startTime;
    public static String TABLE_PATH ="";
    public static Connection CONNECTION;
    public static class ParseMe implements Serializable {
        public final String rawtext;
        public ParseMe(String rawtext) {
            this.rawtext = rawtext;
        }
    }
    public static class Status implements Serializable {}
    public static class Parser extends AbstractActor {
        JSONObject json = new JSONObject();

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Status.class, message -> {
                        System.out.printf("Akka ");
                        System.out.printf("Total persisted: %d\n", records_processed);
                    })
                    .match(ParseMe.class, message -> {
                        try {
                            json = doParse(((ParseMe) message).rawtext);
                            dbInsert(json);
                        } catch (ParseException e) {
                            System.err.printf("%s", e.getStackTrace());
                        }
                    })
                    .matchAny(o -> System.out.println("received unknown message"))
                    .build();
        }

        private static JSONObject doParse(String rawtext) throws ParseException {
            records_processed++;
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(rawtext);
            return json;
        }

        private static void dbInsert(JSONObject json) {
            Document ojaiDocument = CONNECTION.newDocument(json.toJSONString());
            // need to set '_id' for the row key
            String newDocUUID = UUID.randomUUID().toString();
            ojaiDocument.setId(newDocUUID);

            // Get an instance of OJAI
            DocumentStore store = CONNECTION.getStore(TABLE_PATH);

            //insert and flush the document
            store.insertOrReplace(ojaiDocument);
            store.flush();
        }
    }

}
