package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by lee on 5/16/17.
 */
public class MessageWriter implements Runnable {
    KeyValue properties;
    String fileName;
    BlockingQueue<Message> mq;
    private int BUFFER_SIZE =   1024 * 1024;    //TODO:待调整
    private int MQ_CAPACITY = 100000;    //TODO: 待调整
    private byte[] bytesJar;  // 缓存消息
    private int jarCursor = 0; // bytesJar中游标当前位置 数组下标不能超过最大整数
    private long fileCursor = 0;    // 文件中游标的当前位置

    MappedByteBuffer mapBuf = null;
    FileChannel fc = null;

    private volatile  boolean sendOver = false;
    //private AtomicBoolean sendOver = new AtomicBoolean();

    //private AtomicBoolean lastFinish = new AtomicBoolean();
    //private volatile  Boolean sendOver = new Boolean(false);
    //private volatile  boolean lastFinish;
    //private volatile Boolean lastFinish = new Boolean(false);

    public MessageWriter(KeyValue properties, String fileName) {
        this.properties = properties;
        this.fileName = fileName;
        if (fileName.startsWith("QUEUE"))
            mq = new LinkedBlockingQueue<>(MQ_CAPACITY);
        else
            mq = new LinkedBlockingQueue<>(MQ_CAPACITY);
        bytesJar = new byte[BUFFER_SIZE];

        // 初始化fileChannel
    }

    public  void dump() {
       sendOver = true;
    }

        //sendOver.set(true);
        //if (!lastFinish) {

        /*
        while (!mq.isEmpty()) {  // 这时候可以不要考虑线程安全了
            BytesMessage  message = (BytesMessage)mq.remove();
            byte[] propertyBytes = getKeyValueBytes(message.properties());
            byte[] headerBytes = getKeyValueBytes(message.headers());
            byte[] body = message.getBody();
            


            // 注意填充的先后顺序
            fill(propertyBytes, "property");
            fill(headerBytes, "header");
            fill(body, "body");

        }
        // 倒空jar
        if (jarCursor > 0) {
            //mapBuf.put(bytesJar, 0, jarCursor);
            try {
                mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, jarCursor);
            } catch (IOException e) { e.printStackTrace();}
            mapBuf.put(bytesJar, 0, jarCursor);
        }
        */



    public void addMessage(Message message) {
        try {
            mq.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



    /*
    //先分配buffer
    public void fill(byte[] component, String name) {
        if (name.equals("property") || name.equals("header")) {
            if (jarCursor + component.length <= bytesJar.length) {
                for(int k = 0; k < component.length; )
                    bytesJar[jarCursor++] = component[k++];
            }
            else {
                // 先填入前半部分，填满jar
                int k = 0;
                for (; jarCursor < bytesJar.length;)
                    bytesJar[jarCursor++] = component[k++];
                try {
                    mapBuf.put(bytesJar);
                } catch (BufferOverflowException e) {
                    System.out.println(mapBuf.position());
                    System.out.println(name);
                }
              // mapBuf.put(bytesJar);


                try {
                    fileCursor += BUFFER_SIZE;  //映射之前先更新指针
                    mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                    jarCursor = 0;
                } catch (IOException e) { e.printStackTrace(); }
                // 填入剩余部分
                for(; k < component.length; )
                    bytesJar[jarCursor++] = component[k++];
            }
        }
        else {  // 添加行尾分隔符
            if (jarCursor + component.length + 1 <= bytesJar.length) {
                for (int k = 0; k < component.length;)
                    bytesJar[jarCursor++] = component[k++];

                bytesJar[jarCursor++] = (byte)('\n');
            }
            else {
                int k = 0;
                for (; jarCursor < bytesJar.length;)
                    bytesJar[jarCursor++] = component[k++];
                mapBuf.put(bytesJar);

                try {
                    fileCursor += BUFFER_SIZE;  //映射之前先更新指针
                    mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                    jarCursor = 0;
                } catch (IOException e) { e.printStackTrace(); }
                // 填入剩余部分
                for(; k < component.length; )
                    bytesJar[jarCursor++] = component[k++];

                bytesJar[jarCursor++] = (byte)('\n');
            }

        }

    }
    */




// 后分佩buffer
    public void fill(byte[] component, String name) {
        if (name.equals("property") || name.equals("header")) {
            if (jarCursor + component.length > BUFFER_SIZE) {
                // 第一部分
                int k = 0;
                for (; jarCursor < BUFFER_SIZE;)
                    bytesJar[jarCursor++] = component[k++];
                try {
                    mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                mapBuf.put(bytesJar);

                // 更新文件指针和容器内的指针
                fileCursor += BUFFER_SIZE;
                jarCursor = 0;

                // 第二部分
                for(; k < component.length;)
                    bytesJar[jarCursor++] = component[k++];
            } else{
                int k = 0;
                for (; k < component.length; )
                    bytesJar[jarCursor++] = component[k++];

            }
        }

        else {

            if (jarCursor + component.length + 1 > BUFFER_SIZE) {
                int k = 0;
                for (; jarCursor < BUFFER_SIZE;)
                    bytesJar[jarCursor++] = component[k++];
                try {
                    mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                } catch (IOException e) { e.printStackTrace(); }

                mapBuf.put(bytesJar);

                // 更新文件指针和容器内的指针
                fileCursor += BUFFER_SIZE;
                jarCursor = 0;

                for (; k < component.length;)
                    bytesJar[jarCursor++] = component[k++];

                bytesJar[jarCursor++] = (byte)('\n');   // 分隔符
            } else {
                int k = 0;
                for (; k < component.length;)
                    bytesJar[jarCursor++] = component[k++];
                bytesJar[jarCursor++] = (byte)('\n');   // 分隔符
            }
        }
    }






    public byte[] getKeyValueBytes(KeyValue map) {
        StringBuilder sb = new StringBuilder();
        DefaultKeyValue kvs = (DefaultKeyValue)map;
        for (String key: kvs.keySet()) {
            sb.append(key);
            sb.append('#');
            sb.append(kvs.getValue(key));
            sb.append('|');
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(',');
        return sb.toString().getBytes();
    }

    public void run() {
        String absPath = properties.getString("STORE_PATH")+ "/" + fileName;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(absPath, "rw");
            fc = raf.getChannel();
            //mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);   // TODO 待删除
            while (!sendOver) {
                BytesMessage message = (BytesMessage)mq.take();
                if (new String(message.getBody()).equals("end")) {
                    //System.out.println();
                    break;
                }

                byte[] propertyBytes = getKeyValueBytes(message.properties());
                byte[] headerBytes = getKeyValueBytes(message.headers());
                byte[] body = message.getBody();

                // 注意填充的先后顺序
                fill(propertyBytes, "property");
                fill(headerBytes, "header");
                fill(body, "body");

            }
            while(!sendOver);
            if (sendOver) {
                while (!mq.isEmpty()) {  // 这时候可以不要考虑线程安全了
                    BytesMessage  message = (BytesMessage)mq.remove();
                    if (new String(message.getBody()).equals("end")) {
                       // System.out.println();
                        break;
                    }
                    byte[] propertyBytes = getKeyValueBytes(message.properties());
                    byte[] headerBytes = getKeyValueBytes(message.headers());
                    byte[] body = message.getBody();



                    // 注意填充的先后顺序
                    fill(propertyBytes, "property");
                    fill(headerBytes, "header");
                    fill(body, "body");

                }
                // 倒空jar
                if (jarCursor > 0) {
                    //mapBuf.put(bytesJar, 0, jarCursor);
                    try {
                        mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, jarCursor);
                        mapBuf.put(bytesJar, 0, jarCursor);
                    } catch (IOException e) { e.printStackTrace();}
                    finally {
                        if (fc != null) {
                            fc.close();
                        }
                    }

                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }






    // mappedByteBuffer
    /*
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
    */


    //ByteBuffer方式




}
