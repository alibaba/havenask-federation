package com.alibaba.search.common.arpc.util;

import java.nio.ByteBuffer;

public class DataBuffer {
    private byte[] buffer;
    private int dataStart; // begin of data
    private int dataEnd;   // end of data
    private int capacity;  // size of buffer

    public DataBuffer() {
        dataStart = 0;
        dataEnd = 0;
        capacity = Constants.DEFAULT_DATABUFFER_SIZE;
        buffer = new byte[Constants.DEFAULT_DATABUFFER_SIZE];
    }

    public DataBuffer(int capacity) {
        dataStart = 0;
        dataEnd = 0;
        this.capacity = capacity;
        buffer = new byte[capacity];
    }
    
    public void clear() {
        dataStart = dataEnd = 0;
    }

    public void drainData(int len) {
        dataStart += len;
        if (dataStart > dataEnd) {
            dataStart = dataEnd;
        }
        if (dataStart == dataEnd) {
            clear();
        }
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public int getDataEnd() {
        return dataEnd;
    }

    public int getDataStart() {
        return dataStart;
    }

    public int getDataLen() {
        return dataEnd - dataStart;
    }
    
    public void write(byte[] bytes) {
        expand(bytes.length);
        System.arraycopy(bytes, 0, buffer, dataEnd, bytes.length);
        dataEnd += bytes.length;
    }
    
    public void write(ByteBuffer bb) {
        int length = bb.limit() - bb.position();
        if (length <= 0) {
            return;
        }
        expand(length);
        System.arraycopy(bb.array(), bb.position(), buffer, dataEnd, length);
        dataEnd += length;
    }
    
    public byte[] read(int len) {
        assert(dataStart + len <= dataEnd);
        byte[] ret = new byte[len];
        System.arraycopy(buffer, dataStart, ret, 0, len);
        dataStart += len;
        if (dataStart == dataEnd) {
            clear();
        }
        return ret;
    }
    
    public int readInt() {
        int ret = 0;
        ret |= (buffer[dataStart] & 0x000000FF) << 24;
        ret |= (buffer[dataStart + 1] & 0x000000FF) << 16;
        ret |= (buffer[dataStart + 2] & 0x000000FF) << 8;
        ret |= (buffer[dataStart + 3] & 0x000000FF);
        dataStart += 4;
        return ret;
    }

    public void writeInt(int num) {
        expand(4);
        buffer[dataEnd] = (byte)((num >> 24) & 0xFF);
        buffer[dataEnd + 1] = (byte)((num >> 16) & 0xFF);
        buffer[dataEnd + 2] = (byte)((num >> 8) & 0xFF);
        buffer[dataEnd + 3] = (byte)(num & 0xFF);
        dataEnd += 4;
    }

    private void expand(int need) {
        if (capacity - dataEnd >= need) {
            return;
        }

        int freeLen = (capacity - dataEnd) + dataStart;
        int dataLen = dataEnd - dataStart;
        if (freeLen < need || freeLen * 4 < dataLen) {
            int newSize = capacity * 2;
            while (newSize < dataLen + need) {
                newSize <<= 1;
            }
            byte[] newBuffer = new byte[newSize];
            if (dataLen > 0) {
                System.arraycopy(buffer, dataStart, newBuffer, 0, dataLen);
            }
            dataStart = 0;
            dataEnd = dataLen;
            capacity = newSize;
            buffer = newBuffer;
        } else {
            System.arraycopy(buffer, dataStart, buffer, 0, dataLen);
            dataStart = 0;
            dataEnd = dataLen;
        }
    }
}
