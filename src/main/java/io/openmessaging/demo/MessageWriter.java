package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;



/**
 * Created by lee on 5/16/17.
 */
public class MessageWriter implements Runnable {
    KeyValue properties;
    String fileName;
    BlockingQueue<Message> mq;
    private int BUFFER_SIZE =   1024 * 1024 * 500 ;    //TODO:待调整
    private int MQ_CAPACITY = 100;    //TODO: 待调整
    private byte[] bytesJar;  // 缓存消息
    private int jarCursor = 0; // bytesJar中游标当前位置 数组下标不能超过最大整数
    private long fileCursor = 0;    // 文件中游标的当前位置
    int count = 0;
    int bufferNum = 1;
    boolean flag = false;
    MappedByteBuffer mapBuf = null;
    FileChannel fc = null;

    private boolean sendOver;

    public MessageWriter(KeyValue properties, String fileName) {
        this.properties = properties;
        this.fileName = fileName;
        mq = new LinkedBlockingQueue<>(MQ_CAPACITY);
        //bytesJar = new byte[BUFFER_SIZE];

        // 初始化fileChannel
    }

    public void dump() {
        sendOver = true;
    }

    public void addMessage(Message message) {
        try {
            mq.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void fill(byte[] component, String name) {
        if (name.equals("property") || name.equals("header")) {
            if (jarCursor + component.length <= bytesJar.length) {
                /*for(int k = 0; k < component.length; )
                    bytesJar[jarCursor++] = component[k++];*/
            	System.arraycopy(bytesJar, jarCursor, component, 0, component.length);
            	jarCursor += component.length;
            }
            else {
                // 先填入前半部分，填满jar
                int k = 0;
                /*for (; jarCursor < bytesJar.length;)
                    bytesJar[jarCursor++] = component[k++];*/
                
                System.arraycopy(bytesJar, jarCursor, component, 0, bytesJar.length-jarCursor-1);
            	k = bytesJar.length-jarCursor;
                
                mapBuf.put(bytesJar);
              
                try {
                    fileCursor += BUFFER_SIZE;  //映射之前先更新指针
                    mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                    jarCursor = 0;
                } catch (IOException e) { e.printStackTrace(); }
                // 填入剩余部分
                /*for(; k < component.length; )
                    bytesJar[jarCursor++] = component[k++];*/
                System.arraycopy(bytesJar, jarCursor, component, k, component.length-k-1);
            	
            }
        }
        else {  // 添加行尾分隔符
            if (jarCursor + component.length + 1 <= bytesJar.length) {
                /*for (int k = 0; k < component.length;)
                    bytesJar[jarCursor++] = component[k++];*/
            	System.arraycopy(bytesJar, jarCursor, component, 0, component.length);
            	jarCursor += component.length;
            	
                bytesJar[++jarCursor] = (byte)('\n');
            }
            else {
                int k = 0;
                /*for (; jarCursor < bytesJar.length;)
                    bytesJar[jarCursor++] = component[k++];*/
                System.arraycopy(bytesJar, jarCursor++, component, 0, bytesJar.length-jarCursor-1);
            	k = bytesJar.length-jarCursor;
                
                mapBuf.put(bytesJar);
                
                
                try {
                	
                    fileCursor += BUFFER_SIZE;  //映射之前先更新指针
                    mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, fileCursor, BUFFER_SIZE);
                    jarCursor = 0;
                } catch (IOException e) { e.printStackTrace(); }
                // 填入剩余部分
                /*for(; k < component.length; )
                    bytesJar[jarCursor++] = component[k++];*/
                
                System.arraycopy(bytesJar, jarCursor, component, k, component.length-k-1);
                bytesJar[++jarCursor] = (byte)('\n');
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
            mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, count, BUFFER_SIZE);
            while (!sendOver) {
                BytesMessage message = (BytesMessage)mq.take();
                byte[] propertyBytes = getKeyValueBytes(properties);
                byte[] headerBytes = getKeyValueBytes(message.headers());
                byte[] body = message.getBody();
                int messlength = propertyBytes.length+headerBytes.length+body.length+4;
               
                if(count+messlength >= BUFFER_SIZE * bufferNum){
                	mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, count, BUFFER_SIZE);
                	bufferNum++;
                }
                count += messlength;
                mapBuf.put(propertyBytes);
                mapBuf.put(headerBytes);
                mapBuf.put(body);
                mapBuf.put((byte)'\n');
                
            }
        	/*List<Object> list = new ArrayList<Object>();
        	
        	mq.drainTo(list);
        	for(Object m:list){*/
            while(!mq.isEmpty()){
        		 BytesMessage  message = (BytesMessage)mq.remove();
        		 
        		 byte[] propertyBytes = getKeyValueBytes(properties);
                 byte[] headerBytes = getKeyValueBytes(message.headers());
                 byte[] body = message.getBody();
                 
                 int messlength = propertyBytes.length+headerBytes.length+body.length+4;
                 
                 try{
                 	if(count+messlength>BUFFER_SIZE * bufferNum){
                     	mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, count, BUFFER_SIZE);
                     	bufferNum++;
                 	}
                 }catch(Exception e){
                 	e.printStackTrace();
                 }
                 count += messlength;
                 
                 mapBuf.put(propertyBytes);
                 mapBuf.put(headerBytes);
                 mapBuf.put(body);
                 mapBuf.put((byte)'\n');
                 
        	}
            

        }catch(Exception e){
        	e.printStackTrace();
        }
    }






}
