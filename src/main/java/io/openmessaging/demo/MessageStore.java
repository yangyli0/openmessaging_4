package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by lee on 5/16/17.
 */
public class MessageStore {
    private KeyValue properties;
    private static volatile  MessageStore INSTANCE = null;
    //private Map<String, BlockingQueue<Message>> mqTable = new HashMap<>();
    private ConcurrentMap<String, BlockingQueue> mqTable = new ConcurrentHashMap<>();
    private final int MQ_CAPACITY = 100000;

    public static MessageStore getInstance(KeyValue properties) {
        if (INSTANCE  == null) {
            synchronized (MessageStore.class) {
                if (INSTANCE == null)
                    INSTANCE = new MessageStore(properties);
            }
        }
        return INSTANCE;
    }

    public MessageStore(KeyValue properties) {
        this.properties = properties;
    }

    public  void putMessage(Message message) {  //去掉同步关键字出错
        try {
            String queueOrTopic = message.headers().getString(MessageHeader.QUEUE);
            if (queueOrTopic == null)
                queueOrTopic = message.headers().getString(MessageHeader.TOPIC);
            if (queueOrTopic == null || queueOrTopic.length() == 0)
                throw new Exception("Queue or Topic is empty");

            /*
            if (mqTable.get(queueOrTopic) == null) {
                mqTable.put(queueOrTopic, new ArrayBlockingQueue<>(MQ_CAPACITY));

                BlockingQueue<Message> mq = mqTable.get(queueOrTopic);
                mq.put(message);
                new Thread(new MessageWriter(properties, queueOrTopic, mq)).start();
            }
            else    mqTable.get(queueOrTopic).put(message);
            */

            if (mqTable.get(queueOrTopic) == null) {
                mqTable.put(queueOrTopic, new LinkedBlockingQueue(MQ_CAPACITY));
               // mqTable.put(queueOrTopic, new LinkedBlockingDeque());
                BlockingQueue<Message> mq = mqTable.get(queueOrTopic);
                new  Thread(new MessageWriter(properties, queueOrTopic, mq)).start();
            }

            mqTable.get(queueOrTopic).put(message);





        } catch(InterruptedException e) {
            e.printStackTrace();
        } catch(Exception e) {
            System.out.println("Queue or Topic is empty");
        }


    }
}
