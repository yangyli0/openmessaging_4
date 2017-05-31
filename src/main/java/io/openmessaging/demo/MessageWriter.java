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
    private final int BUFFER_SIZE =   256 * 1024 * 1024;    //TODO:待调整
    private final  int MQ_CAPACITY = 10000;    //TODO: 待调整
    private final int JAR_SIZE = 4 * 1024 * 1024;

    private byte[] bytesJar;  // 缓存消息
    private int jarCursor = 0; // bytesJar中游标当前位置 数组下标不能超过最大整数
    private long fileCursor = 0;    // 文件中游标的当前位置


    private MappedByteBuffer mapBuf = null;
    private FileChannel fc = null;

    //private volatile  boolean sendOver = false;


    public MessageWriter(KeyValue properties, String fileName) {
        this.properties = properties;
        this.fileName = fileName;
        mq = new LinkedBlockingQueue<>(MQ_CAPACITY);
        bytesJar = new byte[JAR_SIZE];

    }




    public void addMessage(Message message) {
        try {
            mq.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }








// 后分佩buffer 一次次分配buffer

    /*
    public void fill(byte[] component, String name) {
        if (name.equals("property") || name.equals("header")) {
            if (jarCursor + component.length > BUFFER_SIZE) {
                // 第一部分

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

                System.arraycopy(component, k, bytesJar, jarCursor, component.length-k);
                jarCursor += component.length-k;
            } else{

                System.arraycopy(component, 0, bytesJar, jarCursor, component.length);
                jarCursor += component.length;

            }
        }

        else {

            if (jarCursor + component.length + 1 > BUFFER_SIZE) {

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


                System.arraycopy(component, k, bytesJar, jarCursor, component.length - k);
                jarCursor += component.length - k;

                bytesJar[jarCursor++] = (byte)('\n');   // 分隔符
            } else {

                System.arraycopy(component, 0, bytesJar, jarCursor, component.length);
                jarCursor += component.length;
                bytesJar[jarCursor++] = (byte)('\n');   // 分隔符
            }
        }
    }
    */

    //  一次映射一次，一条一条写

    /*
    public void fill(byte[] component, char flag) {
        if (flag == 'p' || flag == 'h') {
            if (mapBuf.position() + component.length > BUFFER_SIZE) {    // 映射前半段
                int k = BUFFER_SIZE - mapBuf.position();
                mapBuf.put(component, 0, k);

                // 开启新的映射
                fileCursor += BUFFER_SIZE;
                try {
                    mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                } catch (IOException e) { e.printStackTrace(); }
                mapBuf.put(component, k, component.length - k);

            }
            else {
                mapBuf.put(component);
            }

        }

        else {
            if (mapBuf.position() + component.length + 1 > BUFFER_SIZE) {
                int k = BUFFER_SIZE - mapBuf.position();
                mapBuf.put(component, 0, k);

                // 开启新的映射
                fileCursor += BUFFER_SIZE;
                try {
                    mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (k < component.length)   // 是否只需要加入'\n'
                    mapBuf.put(component, k, component.length);

                mapBuf.put((byte)('\n'));
            }

            else {
                mapBuf.put(component);
                mapBuf.put((byte)('\n'));
            }
        }
    }
    */

    ///*

    public  void fill(byte[] component, char ch) {
        if (ch == 'p' || ch == 'h') {
            if (jarCursor + component.length > JAR_SIZE) {
                // 复制第一部分
                int k = JAR_SIZE - jarCursor;
                System.arraycopy(component, 0, bytesJar, jarCursor, k);
                // 填入mapBuf
                if (mapBuf.position() == mapBuf.capacity()) {
                    try {
                        fileCursor += BUFFER_SIZE;  //更新jarCursor
                        mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                mapBuf.put(bytesJar);
                jarCursor = 0;  // 倒空更新jarCursor

                // 复制第二部分
                System.arraycopy(component, k, bytesJar, jarCursor, component.length - k);
                jarCursor += component.length -k;   //更新jarCursor;
            }
            else {
                System.arraycopy(component, 0, bytesJar, jarCursor, component.length);
                jarCursor += component.length;  // 更新jarCursor;
            }

        }
        else {
            if (jarCursor + component.length + 1 > JAR_SIZE) {
                int k = JAR_SIZE - jarCursor;
                System.arraycopy(component, 0, bytesJar, jarCursor, k);
                if (mapBuf.position() == BUFFER_SIZE) {
                    try {
                        fileCursor += BUFFER_SIZE;
                        mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                    } catch (IOException e) { e.printStackTrace();}

                }
                mapBuf.put(bytesJar);
                jarCursor = 0;

                if (k < component.length)
                    System.arraycopy(component, k, bytesJar, jarCursor, component.length - k);
                jarCursor += component.length - k;
                bytesJar[jarCursor++] = (byte)('\n');

            }
            else {
                System.arraycopy(component, 0, bytesJar, jarCursor, component.length);
                jarCursor += component.length;
                bytesJar[jarCursor++] = (byte)('\n');
            }
        }
    }

    //*/







    public byte[] getKeyValueBytes(KeyValue map) {
        return ((DefaultKeyValue)map).getBytes();
    }

    public void run() {
        String absPath = properties.getString("STORE_PATH")+ "/" + fileName.substring(1);
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(absPath, "rw");
            fc = raf.getChannel();
            mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);   // TODO 待删除
            while (true) {
                BytesMessage message = (BytesMessage)mq.take();
                if (new String(message.getBody()).equals("")) {
                    break;
                }

                byte[] propertyBytes = getKeyValueBytes(message.properties());
                byte[] headerBytes = getKeyValueBytes(message.headers());
                byte[] body = message.getBody();

                // 注意填充的先后顺序
                fill(propertyBytes, 'p');
                fill(headerBytes, 'h');
                fill(body, 'b');
                //fill((byte)('\n'));
            }




            ///*
            if (jarCursor > 0) {
                if (mapBuf.position() == mapBuf.capacity()) {
                    try {
                        fileCursor += BUFFER_SIZE;
                        mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);   // TODO 待修改
                    } catch (IOException e) { e.printStackTrace(); }

                   // mapBuf.force();
                }
                mapBuf.put(bytesJar, 0, jarCursor);
            }
            //*/


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }






}
