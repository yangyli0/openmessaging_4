package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.nio.MappedByteBuffer;
import java.util.*;

/**
 * Created by lee on 5/16/17.
 */
public class DefaultPullConsumer implements PullConsumer{
    private KeyValue properties;
    private String queue;
    private List<String> bucketList = new ArrayList<>();
    private int curBucket = 0;

    private Map<String, MessageFile> messageFileMap = null;
    private Map<String, Bookkeeper> consumeRecord = null;


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

        if (messageFileMap.get(bucket) == null)
            messageFileMap.put(bucket, new MessageFile(properties, bucket)); // 这里隐含文件操作
        if (consumeRecord.get(bucket) == null)
            consumeRecord.put(bucket, new Bookkeeper());

        MessageFile msgFile = messageFileMap.get(bucket);
        Bookkeeper bookkeeper = consumeRecord.get(bucket);

        int curBufIndex = bookkeeper.getCurBufIndex();
        if (curBufIndex >= msgFile.getMapBufList().size())
            return message; // 当前bucket对应的文件已经被消费完

        MappedByteBuffer mapBuf = msgFile.getMapBufList().get(curBufIndex);
        int curOffset = bookkeeper.getCurOffset();

        // 取消息
        int i = curOffset;
        byte[] msgBytes = null;
        //for (; i < mapBuf.capacity() && mapBuf.get(i) != '\n'; i++) ;
        for (; mapBuf.get(i) != 0 && mapBuf.get(i) != '\n'; i++);

        if (mapBuf.get(i) == 0) return null;    // 到达边界

        // 不跨越buffer
        if (i < mapBuf.capacity()) {    // i 此时指向 '\n'
            msgBytes = new byte[i - curOffset];
            int k = 0;

            for (int j = curOffset; j < i; j++)
                msgBytes[k++] = mapBuf.get();

            mapBuf.get();   //让 buffer的position指针跳过'\n'

            if (i+1 == mapBuf.capacity()) { // 已到达当前buffer的末尾
                bookkeeper.increaseBufIndex();
                bookkeeper.setOffset(0);
            } else    bookkeeper.setOffset(i+1);   // 更新在buffer内的偏移量
        }

        // 跨越不同的buffer, 进入下一个buffer
        else {
            bookkeeper.increaseBufIndex();
            bookkeeper.setOffset(0);
            int otherBufIndex = bookkeeper.getCurBufIndex();
            int otherOffset = bookkeeper.getCurOffset();
            MappedByteBuffer otherMapBuf = msgFile.getMapBufList().get(otherBufIndex);

            int w = otherOffset;
            //for (; w < otherMapBuf.capacity() && otherMapBuf.get(w) != '\n'; w++) ;
            for (; otherMapBuf.get(w) != 0 && otherMapBuf.get(w) != '\n'; w++);

            if (otherMapBuf.get(w) == 0)    return null;    //到达边界

            int firstLen = mapBuf.capacity() - curOffset;
            int secondLen = w - otherOffset;

            msgBytes = new byte[firstLen + secondLen];

            // 获取前半段
            int k = 0;
            for (int j = curOffset; j < mapBuf.capacity(); j++)
                msgBytes[k++] = mapBuf.get();
            // 获取后半段
            for (int j = otherOffset; j < w; j++)
                msgBytes[k++] = otherMapBuf.get();

            otherMapBuf.get();  // 让position指针跳过'\n'

            // 更新buffer内偏移量
            bookkeeper.setOffset(w+1);
        }

        // 产生消息
        DefaultMessageFactory messageFactory = new DefaultMessageFactory();
        for (i = 0; i < msgBytes.length && msgBytes[i] != ','; i++) ;
        byte[] headBytes = Arrays.copyOfRange(msgBytes, 0, i ); // [start, end)
        byte[] bodyBytes = Arrays.copyOfRange(msgBytes, i + 1, msgBytes.length);
        String queueOrTopic = new String(headBytes);
        if (queueOrTopic.startsWith("QUEUE_"))
            message = messageFactory.createBytesMessageToQueue(queueOrTopic, bodyBytes);
        else
            message = messageFactory.createBytesMessageToTopic(queueOrTopic, bodyBytes);

        return message;

    }
    @Override public Message poll(KeyValue properties) { throw new UnsupportedOperationException("Unsupported"); }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName))
            throw new ClientOMSException("You have already attached to a queue: " + queue);
        queue = queueName;

        bucketList.addAll(topics);
        bucketList.add(queueName);

        // 初始化
        messageFileMap = new HashMap<>(bucketList.size());
        consumeRecord = new HashMap<>(bucketList.size());


    }

    @Override public void ack(String messageId) { throw new UnsupportedOperationException("Unsupported"); }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

}
