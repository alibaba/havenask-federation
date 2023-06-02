package com.alibaba.search.common.arpc.util;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

public class DataBufferTest extends TestCase {

    public void testDrainData() {
        DataBuffer dataBuffer = new DataBuffer();
        byte[] bytes = { 0x12, 0x34, 0x56, 0x78, (byte) 0x9a, (byte) 0xbc,
                (byte) 0xde, (byte) 0xff };
        dataBuffer.write(bytes);
        assertEquals(0, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());
        dataBuffer.drainData(4);
        assertEquals(4, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());
        int actual = dataBuffer.readInt();
        assertEquals(0x9abcdeff, actual);
        assertEquals(8, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());
        dataBuffer.drainData(4);
        assertEquals(0, dataBuffer.getDataStart());
        assertEquals(0, dataBuffer.getDataEnd());
    }

    public void testWrite() {
        DataBuffer dataBuffer = new DataBuffer();
        byte[] bytes = { (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                0x00, 0x00, 0x00, 0x00 };
        dataBuffer.write(bytes);
        assertEquals(0, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());
        int actual = dataBuffer.readInt();
        assertEquals(-1, actual);
        assertEquals(4, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());
        actual = dataBuffer.readInt();
        assertEquals(0, actual);
        assertEquals(8, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());
    }

    public void testWriteByteBuffer() {
        DataBuffer dataBuffer = new DataBuffer();
        ByteBuffer bb = ByteBuffer.allocate(1000);
        bb.putInt(2);
        bb.putInt(3);
        bb.flip();
        dataBuffer.write(bb);
        assertEquals(0, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());
        int actual = dataBuffer.readInt();
        assertEquals(2, actual);
        assertEquals(4, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());
        actual = dataBuffer.readInt();
        assertEquals(3, actual);
        assertEquals(8, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());        
    }
    
    public void testRead() {
        DataBuffer dataBuffer = new DataBuffer();
        byte[] bytes = { (byte) 0xab, (byte) 0xcd, (byte) 0xef, (byte) 0xf1,
                (byte) 0xf2, (byte) 0xf3, 0x00, 0x00 };
        dataBuffer.write(bytes);
        byte[] ret = dataBuffer.read(3);
        assertEquals(3, ret.length);
        assertEquals((byte) 0xab, ret[0]);
        assertEquals((byte) 0xcd, ret[1]);
        assertEquals((byte) 0xef, ret[2]);

        ret = dataBuffer.read(2);
        assertEquals(2, ret.length);
        assertEquals((byte) 0xf1, ret[0]);
        assertEquals((byte) 0xf2, ret[1]);

        ret = dataBuffer.read(0);
        assertEquals(0, ret.length);
    }

    public void testReadInt() {
        DataBuffer dataBuffer = new DataBuffer();
        byte[] bytes = { 0x12, 0x34, 0x56, 0x78, (byte) 0x9a, (byte) 0xbc,
                (byte) 0xde, (byte) 0xff };
        dataBuffer.write(bytes);
        assertEquals(0, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());
        int actual = dataBuffer.readInt();
        assertEquals(0x12345678, actual);
        assertEquals(4, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());
        actual = dataBuffer.readInt();
        assertEquals(0x9abcdeff, actual);
        assertEquals(8, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());
    }

    public void testWriteInt() {
        DataBuffer dataBuffer = new DataBuffer();
        dataBuffer.writeInt(0x87654321);
        assertEquals(0, dataBuffer.getDataStart());
        assertEquals(4, dataBuffer.getDataEnd());
        dataBuffer.writeInt(0x89abcdef);
        assertEquals(0, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());
        int actual = dataBuffer.readInt();
        assertEquals(0x87654321, actual);
        assertEquals(4, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());
        actual = dataBuffer.readInt();
        assertEquals(0x89abcdef, actual);
        assertEquals(8, dataBuffer.getDataStart());
        assertEquals(8, dataBuffer.getDataEnd());
    }

    public void testExpandToNewBuffer() {
        DataBuffer dataBuffer = new DataBuffer();
        dataBuffer.writeInt(-1);
        byte[] bytes = new byte[Constants.DEFAULT_DATABUFFER_SIZE];
        bytes[0] = 1;
        bytes[bytes.length - 1] = (byte) 255;
        dataBuffer.write(bytes);
        byte[] buf = dataBuffer.getBuffer();
        assertEquals(-1, dataBuffer.readInt());
        assertEquals(Constants.DEFAULT_DATABUFFER_SIZE * 2, buf.length);
        assertEquals(1, buf[4]);
        assertEquals((byte) 255, buf[Constants.DEFAULT_DATABUFFER_SIZE + 3]);
    }

    public void testExpandInOldBuffer() {
        DataBuffer dataBuffer = new DataBuffer();
        dataBuffer.writeInt(0x12345678);
        dataBuffer.writeInt(0x11223344);
        dataBuffer.readInt();
        byte[] bytes = new byte[Constants.DEFAULT_DATABUFFER_SIZE - 4];
        bytes[0] = 1;
        bytes[bytes.length - 1] = (byte) 255;
        dataBuffer.write(bytes);
        assertEquals(0, dataBuffer.getDataStart());
        assertEquals(Constants.DEFAULT_DATABUFFER_SIZE, dataBuffer.getDataEnd());
        assertEquals(0x11223344, dataBuffer.readInt());
        byte[] buf = dataBuffer.getBuffer();
        assertEquals((byte) 1, buf[4]);
        assertEquals((byte) 255, buf[Constants.DEFAULT_DATABUFFER_SIZE - 1]);
    }

}
