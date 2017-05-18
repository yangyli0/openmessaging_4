package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;

/**
 * Created by lee on 5/16/17.
 */
public class DefaultMessageFactory implements MessageFactory {
    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        DefaultBytesMessage message = new DefaultBytesMessage(body);
        message.putHeaders(MessageHeader.TOPIC, topic);
        return message;
    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        DefaultBytesMessage message = new DefaultBytesMessage(body);
        message.putHeaders(MessageHeader.QUEUE, queue);
        return message;
    }
}
