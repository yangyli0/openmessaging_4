package io.openmessaging.demo;

import io.openmessaging.KeyValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lee on 5/16/17.
 */
public class MessageBroker {
    private static volatile MessageBroker INSTANCE = null;
    private KeyValue properties;

    private Map<String, MessageFile> fileMap = new HashMap<>(); //

    private Map<String, Map<String, Bookkeeper>> consumeRecord = new HashMap<>(); // c1 -> (messagefile -> bookkeeper)



    public static MessageBroker getInstance(KeyValue properties) {
        if (INSTANCE == null) {
            synchronized (MessageBroker.class) {
                if (INSTANCE == null)
                    INSTANCE = new MessageBroker(properties);
            }
        }
        return INSTANCE;
    }

    private MessageBroker(KeyValue properties) {
        this.properties = properties;
    }

    public Map<String, MessageFile> getFileMap() {
        return this.fileMap;
    }

    public Map<String, Map<String, Bookkeeper>> getConsumeRecord() {
        return this.consumeRecord;
    }



}
