package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;

import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by lee on 5/16/17.
 */
public class DefaultPullConsumer implements PullConsumer{
    private KeyValue properties;
    private String queue;
    private List<String> bucketList = new ArrayList<>();
    private int curBucket = 0;

    private ConcurrentHashMap<String, MessageFile> messageFileMap = null;
   // private ConcurrentHashMap<String, Bookkeeper> consumeRecord = null;
    private ConcurrentHashMap<String, Integer> consumeRecord = null;


    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }

    @Override public KeyValue properties() { return properties; }

    @Override public  Message poll() {
        Message message = null;
        while (curBucket  < bucketList.size()) {
            String bucket = bucketList.get(curBucket);
            message = pullMessage(bucket);
            if (message != null) {
                return message;
            }
            curBucket++;
        }

        return null;
    }


    public  Message pullMessage(String bucket) {
        Message message = null;
        if (!messageFileMap.containsKey(bucket))
            messageFileMap.put(bucket, new MessageFile(properties, bucket));
        if (!consumeRecord.containsKey(bucket))
            consumeRecord.put(bucket, 0);

        MessageFile msgFile = messageFileMap.get(bucket);
        //Bookkeeper bookkeeper = consumeRecord.get(bucket);
        //int curBufIndex = bookkeeper.getCurBufIndex();
        int curBufIndex = consumeRecord.get(bucket);
        MappedByteBuffer mapBuf = msgFile.getMapBufList().get(curBufIndex);

        if (mapBuf.position() == mapBuf.capacity()) {
           // bookkeeper.increaseBufIndex();
            if (++curBufIndex >= msgFile.getMapBufList().size()) return null;
            consumeRecord.put(bucket, curBufIndex);
            //curBufIndex = bookkeeper.getCurBufIndex();
            //if (curBufIndex >= msgFile.getMapBufList().size())  return null;
            mapBuf = msgFile.getMapBufList().get(curBufIndex);
        }

        byte[] msgBytes = null;
        int i = mapBuf.position();
        for (; i < mapBuf.capacity() && mapBuf.get(i) != 10; i++);

        if (i >= mapBuf.capacity()) {   // 跨越两个buffer
            //bookkeeper.increaseBufIndex();  // 不用考虑越界
            int otherBufIndex = curBufIndex + 1;

            consumeRecord.put(bucket, ++curBufIndex);
            MappedByteBuffer otherMapBuf = msgFile.getMapBufList().get(otherBufIndex);
            int w = otherMapBuf.position();
            for(; otherMapBuf.get(w) != 10; w++);
            int firstLen = i - mapBuf.position();
            int secondLen = w;  // 省去－０
            msgBytes = new byte[firstLen + secondLen];

            mapBuf.get(msgBytes, 0, firstLen);
            otherMapBuf.get(msgBytes, firstLen, secondLen);

            otherMapBuf.get();  //跳过'\n'
        }
        else {
            msgBytes = new byte[i - mapBuf.position()];
            mapBuf.get(msgBytes, 0, i-mapBuf.position());
            mapBuf.get();   // 跳过'\n'
        }


        return assemble(msgBytes);
    }


    public Message assemble(byte[] msgBytes) {
        int i, j;
        // 获取property

        DefaultKeyValue property = new DefaultKeyValue();
        for (i = 0; i < msgBytes.length && msgBytes[i] != ','; i++);
        byte[] propertyBytes = Arrays.copyOfRange(msgBytes, 0, i);  // [start, end)
        insertKVs(propertyBytes, property);
        j = ++i; // 跳过","

        // 获取headers
        DefaultKeyValue header = new DefaultKeyValue();
        for (; i < msgBytes.length && msgBytes[i] != ','; i++);
        byte[] headerBytes = Arrays.copyOfRange(msgBytes, j, i);
        insertKVs(headerBytes, header);
        j = ++i; // 跳过","

        // 获取body
        for (; i < msgBytes.length && msgBytes[i] != '\n'; i++);
        byte[] body = Arrays.copyOfRange(msgBytes, j, i);

        String queueOrTopic = header.getString(MessageHeader.TOPIC);
        DefaultBytesMessage message = null;
        DefaultMessageFactory messageFactory = new DefaultMessageFactory();
        if (queueOrTopic != null)
            message = (DefaultBytesMessage) messageFactory.createBytesMessageToTopic(queueOrTopic, body);
        else
            message = (DefaultBytesMessage) messageFactory.createBytesMessageToQueue(queueOrTopic, body);

        message.setHeaders(header);
        message.setProperties(property);

        return message;

    }






    /*
    public  synchronized Message pullMessage(String bucket) {
        Message message = null;
        if (messageFileMap.get(bucket) == null)
            messageFileMap.put(bucket, new MessageFile(properties, bucket));
        if (consumeRecord.get(bucket) == null)
            consumeRecord.put(bucket, new Bookkeeper());

        MessageFile msgFile = messageFileMap.get(bucket);
        Bookkeeper bookkeeper = consumeRecord.get(bucket);

        int curBufIndex = bookkeeper.getCurBufIndex();

        int curOffset = bookkeeper.getCurOffset();
        MappedByteBuffer mapBuf = msgFile.getMapBufList().get(curBufIndex); // 这里不用做判断，后面有直接返回

        if (curOffset == mapBuf.capacity()) {   // 进入下一个buffer
            bookkeeper.increaseBufIndex();
            curBufIndex = bookkeeper.getCurBufIndex();
            if (curBufIndex >= msgFile.getMapBufList().size()) return null;
            mapBuf = msgFile.getMapBufList().get(curBufIndex);
            curOffset = 0;
        }

        byte[] msgBytes = null;
        int i = curOffset;
        for (; i < mapBuf.capacity() && mapBuf.get(i) != 10; i++);

        if (i >= mapBuf.capacity()) {   // 跨越两个buffer
            bookkeeper.increaseBufIndex();  // 因为消息不完整，所以不用判断是否为最后一个buffer
            //bookkeeper.setOffset(0);
            int otherBufIndex = curBufIndex + 1;
            //int otherCurOffset = 0;
            MappedByteBuffer otherMapBuf = msgFile.getMapBufList().get(otherBufIndex);
            int w = 0;
            for (; otherMapBuf.get(w) != 10; w++);

            int firstLen = i - curOffset;
            int secondLen = w;
            msgBytes = new byte[firstLen + secondLen];


            int k = 0;
            // 填入第一部分
            for(int j = curOffset; j < i; j++)
                msgBytes[k++] = mapBuf.get();
            // 末尾不用跳过最后一个'\n'

            // 填入第二部分
            for(int j = 0; j < w; j++)
                msgBytes[k++] = otherMapBuf.get();



            otherMapBuf.get();  // 跳过'\n'
            bookkeeper.setOffset(w+1);
        }
         else {
            msgBytes = new byte[i - curOffset];

            int k = 0;
            for (int j = curOffset; j < i; j++)
                msgBytes[k++] = mapBuf.get();

            //mapBuf.get(msgBytes, 0, i-curOffset);
            mapBuf.get();   // 跳过'\n'
            bookkeeper.setOffset(i+1);
        }
        return assemble(msgBytes);

    }
    */







    /*

    public Message assemble(byte[] msgBytes) {
        DefaultMessageFactory messageFactory = new DefaultMessageFactory();
        Message message = null;
        int i, j;
        // 获取property
        //KeyValue property = message.properties();
        DefaultKeyValue property = new DefaultKeyValue();
        for (i = 0; i < msgBytes.length && msgBytes[i] != ','; i++);
        byte[] propertyBytes = Arrays.copyOfRange(msgBytes, 0, i);  // [start, end)
        insertKVs(propertyBytes, property);
        j = ++i; // 跳过","

        // 获取headers
        DefaultKeyValue header = new DefaultKeyValue();
        for (; i < msgBytes.length && msgBytes[i] != ','; i++);
        byte[] headerBytes = Arrays.copyOfRange(msgBytes, j, i);
        insertKVs(headerBytes, header);
        j = ++i; // 跳过","

        // 获取body
        for (; i < msgBytes.length && msgBytes[i] != '\n'; i++);
        byte[] body = Arrays.copyOfRange(msgBytes, j, i);

        // 组装
        String queueOrTopic = header.getString(MessageHeader.TOPIC);
        if (queueOrTopic != null)
            message = messageFactory.createBytesMessageToTopic(queueOrTopic, body);
        else
            message = messageFactory.createBytesMessageToQueue(queueOrTopic, body);

        // put property
        for (String key: property.keySet()) {
           // message.putProperties(key, property.getString(key));

            if (header.isInt(key))
                message.putProperties(key, property.getInt(key));
            else if (header.isDouble(key))
                message.putProperties(key, property.getDouble(key));
            else if (header.isLong(key))
                message.putProperties(key, property.getLong(key));
            else
                message.putProperties(key, property.getString(key));

        }

        // put headers
        for (String key: header.keySet()) {

            //message.putHeaders(key, header.getString(key));

            if (header.isInt(key))
                message.putHeaders(key, header.getInt(key));
            else if (header.isDouble(key))
                message.putHeaders(key, header.getDouble(key));
            else if (header.isLong(key))
                message.putHeaders(key, header.getLong(key));
            else
                message.putHeaders(key, header.getString(key));

        }

        return message;
    }
    */


    public void insertKVs(byte[] kvBytes, KeyValue map) {
        String kvStr = new String(kvBytes);
        String[] kvPairs = kvStr.split("\\|");
        for (String kv: kvPairs) {

            String[] tuple = kv.split("#");

            if(tuple[1].startsWith("i"))
                map.put(tuple[0], Integer.parseInt(tuple[1].substring(1)));
            else if(tuple[1].startsWith("d"))
                map.put(tuple[0], Double.parseDouble(tuple[1].substring(1)));
            else if (tuple[1].startsWith("l"))
                map.put(tuple[0], Long.parseLong(tuple[1].substring(1)));
            else
                map.put(tuple[0], tuple[1].substring(1));

        }

    }


    @Override public Message poll(KeyValue properties) { throw new UnsupportedOperationException("Unsupported"); }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName))
            throw new ClientOMSException("You have already attached to a queue: " + queue);
        queue = queueName;

        bucketList.addAll(topics);
        bucketList.add(queueName);

        // 初始化
        messageFileMap = new ConcurrentHashMap<>(bucketList.size());
        consumeRecord = new ConcurrentHashMap<>(bucketList.size());


    }

    @Override public void ack(String messageId) { throw new UnsupportedOperationException("Unsupported"); }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

}
