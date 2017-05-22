package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lee on 5/16/17.
 */
public class MessageStore {
    private KeyValue properties;
    private static volatile  MessageStore INSTANCE = null;

    private ConcurrentHashMap<String, MessageWriter> writerTable = new ConcurrentHashMap<>();

    private static AtomicInteger numOfProducer = new AtomicInteger(0);
    private static AtomicInteger finishCnt = new AtomicInteger(0);


    public static MessageStore getInstance(KeyValue properties) {
        numOfProducer.incrementAndGet();    //　统计生产者数目
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

    public  void finishCount() {    //TODO: 同步关键字待删除
        int cnt = finishCnt.incrementAndGet();
        if (cnt == numOfProducer.get()) {
            // 通知所有线程清空容器和队列
            for(String bucket: writerTable.keySet()) {
                writerTable.get(bucket).dump();
            }
            //System.out.printf("Total Producers: %d%n", cnt);    // TODO: 待删除
        }

    }

    public synchronized void putMessage(Message message) {  //去掉同步关键字出错
        try {
            String queueOrTopic = message.headers().getString(MessageHeader.QUEUE);
            if (queueOrTopic == null)
                queueOrTopic = message.headers().getString(MessageHeader.TOPIC);
            if (queueOrTopic == null || queueOrTopic.length() == 0)
                throw new Exception("Queue or Topic is empty");


            /*
            if (mqTable.get(queueOrTopic) == null) {
                mqTable.put(queueOrTopic, new LinkedBlockingQueue(MQ_CAPACITY));
               // mqTable.put(queueOrTopic, new LinkedBlockingDeque());
                BlockingQueue<Message> mq = mqTable.get(queueOrTopic);
                new  Thread(new MessageWriter(properties, queueOrTopic, mq)).start();
            }

            mqTable.get(queueOrTopic).put(message);
            */

            if (writerTable.get(queueOrTopic) == null) {    // TODO 这里有隐患
                writerTable.put(queueOrTopic, new MessageWriter(properties, queueOrTopic));
                new Thread(writerTable.get(queueOrTopic)).start();
            }
            writerTable.get(queueOrTopic).addMessage(message);


        } catch(InterruptedException e) {
            e.printStackTrace();
        } catch(Exception e) {
            System.out.println("Queue or Topic is empty");
        }


    }
}
