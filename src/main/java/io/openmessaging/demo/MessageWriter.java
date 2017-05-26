package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by lee on 5/16/17.
 */
public class MessageWriter implements Runnable {
    KeyValue properties;
    String fileName;
    BlockingQueue<Message> mq;
    private int BUFFER_SIZE =   16 * 1024 * 1024;    //TODO:待调整
    private int MQ_CAPACITY = 10000;    //TODO: 待调整
    private byte[] bytesJar;  // 缓存消息
    private int jarCursor = 0; // bytesJar中游标当前位置 数组下标不能超过最大整数
    private long fileCursor = 0;    // 文件中游标的当前位置
    //private int JAR_SIZE = 4 * 1024 * 1024;

    MappedByteBuffer mapBuf = null;
    FileChannel fc = null;

    //private volatile  boolean sendOver = false;


    public MessageWriter(KeyValue properties, String fileName) {
        this.properties = properties;
        this.fileName = fileName;
        mq = new LinkedBlockingQueue<>(MQ_CAPACITY);
        bytesJar = new byte[BUFFER_SIZE];


        // 初始化fileChannel
    }

    /*
    public  void dump() {
       sendOver = true;
    }
    */




    public void addMessage(Message message) {
        try {
            mq.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }








// 后分佩buffer
    public void fill(byte[] component, String name) {
        if (name.equals("property") || name.equals("header")) {
            if (jarCursor + component.length > BUFFER_SIZE) {
                // 第一部分
                /*
                int k = 0;
                for (; jarCursor < BUFFER_SIZE;)
                    bytesJar[jarCursor++] = component[k++];
                    */
                int k = BUFFER_SIZE - jarCursor;
                System.arraycopy(component, 0, bytesJar, jarCursor, k);
                jarCursor += k;
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
                /*
                for(; k < component.length;)
                    bytesJar[jarCursor++] = component[k++];
                    */
                System.arraycopy(component, k, bytesJar, jarCursor, component.length-k);
                jarCursor += component.length-k;
            } else{
                /*
                int k = 0;
                for (; k < component.length; )
                    bytesJar[jarCursor++] = component[k++];
                */
                System.arraycopy(component, 0, bytesJar, jarCursor, component.length);
                jarCursor += component.length;

            }
        }

        else {

            if (jarCursor + component.length + 1 > BUFFER_SIZE) {
                /*
                int k = 0;
                for (; jarCursor < BUFFER_SIZE;)
                    bytesJar[jarCursor++] = component[k++];
                */
                int k = BUFFER_SIZE - jarCursor;
                System.arraycopy(component, 0, bytesJar, jarCursor, k);
                jarCursor += k;
                try {
                    mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                } catch (IOException e) { e.printStackTrace(); }

                mapBuf.put(bytesJar);

                // 更新文件指针和容器内的指针
                fileCursor += BUFFER_SIZE;
                jarCursor = 0;

                /*
                for (; k < component.length;)
                    bytesJar[jarCursor++] = component[k++];
                */
                System.arraycopy(component, k, bytesJar, jarCursor, component.length - k);
                jarCursor += component.length - k;

                bytesJar[jarCursor++] = (byte)('\n');   // 分隔符
            } else {
                /*
                int k = 0;
                for (; k < component.length;)
                    bytesJar[jarCursor++] = component[k++];
                */
                System.arraycopy(component, 0, bytesJar, jarCursor, component.length);
                jarCursor += component.length;
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
            while (true) {
                BytesMessage message = (BytesMessage)mq.take();
                if (new String(message.getBody()).equals("")) {
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

            /*
            while(!sendOver);
            if (sendOver) {

                while (!mq.isEmpty()) {  // 这时候可以不要考虑线程安全了
                    BytesMessage  message = (BytesMessage)mq.remove();
                    if (new String(message.getBody()).equals("")) {
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
            */
            while (!mq.isEmpty()) {  // 这时候可以不要考虑线程安全了
                BytesMessage  message = (BytesMessage)mq.remove();
                if (new String(message.getBody()).equals("")) {
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


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }






}
