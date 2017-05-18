package io.openmessaging.demo;

/**
 * Created by lee on 5/16/17.
 */
public class Bookkeeper {
    private int curBufIndex = 0;    // 记录当前buffer在list里的下标
    private int curOffset = 0;  // 记录在当前buffer里的偏移量

    public void increaseBufIndex() { curBufIndex++; }
    public void setOffset(int offset) { this.curOffset = offset; }


    public int getCurBufIndex() { return this.curBufIndex; }
    public int getCurOffset() { return this.curOffset; }


}
