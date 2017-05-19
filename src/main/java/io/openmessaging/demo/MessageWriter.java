package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Created by lee on 5/16/17.
 */
public class MessageWriter implements Runnable {
    KeyValue properties;
    String fileName;
    BlockingQueue<Message> mq;
    private int BUFFER_SIZE = 256 * 1024 * 1024;

    public MessageWriter(KeyValue properties, String fileName, BlockingQueue<Message> mq) {
        this.properties = properties;
        this.fileName = fileName;
        this.mq = mq;
    }

    //ByteBuffer方式

    /*
    public void run() {
        String absPath = properties.getString("STORE_PATH")+"/messagestore/" + fileName;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(absPath, "rw");
            FileChannel fc = raf.getChannel();
            ByteBuffer buf = ByteBuffer.allocate(1024); // TODO: 修改消息体大小
            while(true) {
                BytesMessage msg = (BytesMessage) mq.take();
                byte[] headBytes = (fileName + ",").getBytes();
                byte[] bodyBytes = msg.getBody();
                buf.put(headBytes);
                buf.put(bodyBytes);
                buf.put((byte)'\n');
                buf.flip();
                fc.write(buf);
                buf.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    */






    // mappedByteBuffer
    public void run() {
        String absPath = properties.getString("STORE_PATH")+ "/" + fileName;
        RandomAccessFile raf = null;
        try {
          raf = new RandomAccessFile(absPath, "rw");
          FileChannel fc = raf.getChannel();
          long i = 0;   // 记录文件中游标位置
          MappedByteBuffer mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, i, BUFFER_SIZE);
          while (true) {

              BytesMessage msg = (BytesMessage) mq.take();

              // 获取body
              byte[] bodyBytes = msg.getBody();

              //获取property对应的byte数组

              DefaultKeyValue property = (DefaultKeyValue) msg.properties();
              StringBuilder sb = new StringBuilder();
              for (String key: property.keySet()) {
                  sb.append(key);
                  sb.append('#');
                  sb.append(property.getValue(key));
                  sb.append('|');
              }
              sb.deleteCharAt(sb.length()-1);
              sb.append(',');
              byte[] propertyBytes = sb.toString().getBytes();

              // 获取header对应的byte数组
              DefaultKeyValue headers = (DefaultKeyValue)msg.headers();
              sb = new StringBuilder();
              for (String key: headers.keySet()) {
                  sb.append(key);
                  sb.append('#');
                  sb.append(headers.getValue(key));
                  sb.append('|');
              }
              sb.deleteCharAt(sb.length()-1);
              sb.append(',');

              byte[] headerBytes = sb.toString().getBytes();



              long end = (i / BUFFER_SIZE + 1) * BUFFER_SIZE - 1;
              int msgLen = propertyBytes.length + headerBytes.length + bodyBytes.length + 1; // 算上结尾的'\n'

              // 跨越不同的块
              if ( i + msgLen >= end) {

                  int j = 0;
                  byte[] msgBytes = new byte[msgLen];
                  System.arraycopy(propertyBytes, 0, msgBytes, j, propertyBytes.length);
                  j += propertyBytes.length;

                  System.arraycopy(headerBytes, 0, msgBytes, j, headerBytes.length);
                  j += headerBytes.length;

                  System.arraycopy(bodyBytes, 0, msgBytes, j, bodyBytes.length);
                  msgBytes[msgBytes.length - 1] = (byte)('\n');

                  int len = (int) (end - i + 1);    // 前半段长度
                  int w = 0;
                  for (; w < len; w++) // 先放置前半段
                      mapBuf.put(msgBytes[w]);

                  // 申请一个新的buffer,放置剩余部分
                  mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, end+1, BUFFER_SIZE);
                  for (; w < msgBytes.length; w++)
                      mapBuf.put(msgBytes[w]);

              }
              else {
                  //mapBuf.put(headBytes);
                  //mapBuf.put(bodyBytes);
                  mapBuf.put(propertyBytes);
                  mapBuf.put(headerBytes);
                  mapBuf.put(bodyBytes);
                  mapBuf.put((byte)('\n'));
              }

              i += msgLen; // 更新游标在文件中的位置

          }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }




}
