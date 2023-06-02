package com.alibaba.search.common.arpc.io;

import java.nio.ByteBuffer;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.exceptions.ArpcInvalidPacketHeaderException;
import com.alibaba.search.common.arpc.packet.Packet;
import com.alibaba.search.common.arpc.packet.PacketHandler;
import com.alibaba.search.common.arpc.packet.PacketInfo;
import com.alibaba.search.common.arpc.packet.PacketManager;
import com.alibaba.search.common.arpc.packet.PacketStreamer;
import com.alibaba.search.common.arpc.packet.StreamerContext;
import com.alibaba.search.common.arpc.packet.impl.ControlPacket;
import com.alibaba.search.common.arpc.packet.impl.ControlPacket.CmdType;
import com.alibaba.search.common.arpc.util.Constants;
import com.alibaba.search.common.arpc.util.DataBuffer;

public class Connection implements IoHandler {
    private final static Logger logger = LoggerFactory
            .getLogger(Connection.class);

    private IoComponent ioComponent = null;
    private PacketStreamer packetStreamer = null;
    private StreamerContext streamerContext = null;
    private DataBuffer inputBuffer = new DataBuffer();
    private PacketManager packetManager = new PacketManager();
    private Object lockObj = new Object();
    private int queueLimit = Constants.DEFAULT_OUTPUT_QUEUE_LIMIT;
    // packet send end time
    private static ThreadLocal<Long> packetSendEndTime = new ThreadLocal<>();

    public static Long lastPacketSendEndTime() {
        return packetSendEndTime.get();
    }

    public Connection(IoComponent ioComponent, PacketStreamer packetStreamer) {
        this.ioComponent = ioComponent;
        this.packetStreamer = packetStreamer;
        this.streamerContext = packetStreamer.createStreamerContext();
    }

    public boolean isConnected() {
        return ioComponent.isConnected();
    }

    /**
     *  multi-thread, ioComponent may be closed anytime
     */
    public boolean postPacket(Packet packet, PacketHandler handler,
            Object args, long timeout, boolean blocking) {
        if (packet == null) {
            logger.error("packet is null");
            return false;
        }

        synchronized (lockObj) {
            if (!isConnected()) {
                logger.error("connection not avaliable, maybe closed");
                return false;
            }
            if (packetManager.size() >= queueLimit && !blocking) {
                logger.debug("queue full no block");
                return false;
            }
            while (packetManager.size() >= queueLimit && isConnected()) {
                try {
                    lockObj.wait();
                } catch (InterruptedException e) {
                }
            }
            if (!isConnected()) {
                logger.debug("wake up on closing");
                return false;
            }
            packetManager.addPacket(packet, handler, args, timeout);
        }

        byte[] buf = packetStreamer.encode(packet);
        ioComponent.write(ByteBuffer.wrap(buf));
        packetSendEndTime.set(System.currentTimeMillis());
        return true;
    }

    // run in a single thread
    public void handleRead(ByteBuffer buf) {
        inputBuffer.write(buf);
        try {
            while (true) {
                packetStreamer.decode(inputBuffer, streamerContext);
                if (!streamerContext.isCompleted()) {
                    // not enough data to construct a complete packet in
                    // inputBuffer
                    break;
                }
                // if no data remained
                if(inputBuffer.getDataLen() ==0) {
                    // fix memory leak(when vipserver has many node, hundreds idle connections hold too many data buffers)
                    // when decode finished, input buffer should be released
                    inputBuffer = new DataBuffer();
                }
                Packet packet = streamerContext.getPacket();
                handlePacket(packet);
                streamerContext.reset();
            }
        } catch (ArpcInvalidPacketHeaderException e) {
            logger.error("receive invalid packet header, " +
                         "connection will be closed now");
            ControlPacket packet = new ControlPacket(CmdType.CMD_BAD_PACKET);
            closeAndHandleAll(packet);
        }
    }

    public void handleClose() {
        Collection<PacketInfo> packetInfos = null;
        synchronized (lockObj) {
            packetInfos = packetManager.pullAllPacketInfos();
            lockObj.notifyAll();
        }
        ControlPacket packet = new ControlPacket(CmdType.CMD_CONNECTION_CLOSED);
        handleAll(packet, packetInfos);
    }

    public void handleException(Throwable cause) {
        ControlPacket packet = new ControlPacket(CmdType.CMD_CAUGHT_EXCEPTION);
        closeAndHandleAll(packet);
    }

    public void checkTimeout(long now) {
        Collection<PacketInfo> packetInfos = null;
        synchronized (lockObj) {
            packetInfos = packetManager.pullTimeoutPacketInfos(now);
            lockObj.notifyAll();
        }
        ControlPacket packet = new ControlPacket(CmdType.CMD_TIMEOUT_PACKET);
        handleAll(packet, packetInfos);
    }

    public void close() {
        synchronized (lockObj) {
            ioComponent.close();
            lockObj.notifyAll();
        }
    }

    public void setQueueLimit(int queueLimit) {
        this.queueLimit = queueLimit;
    }

    public int getQueueLimit() {
        return queueLimit;
    }

    protected PacketManager getPacketManager() {
        return packetManager;
    }

    private void closeAndHandleAll(Packet packet) {
        Collection<PacketInfo> packetInfos = null;
        synchronized (lockObj) {
            ioComponent.close();
            packetInfos = packetManager.pullAllPacketInfos();
            lockObj.notifyAll();
        }
        handleAll(packet, packetInfos);
    }

    private void handlePacket(Packet packet) {
        PacketInfo packetInfo = null;
        synchronized (lockObj) {
            packetInfo = packetManager.pullPacketInfo(packet);
            lockObj.notifyAll();
        }
        handlePacket(packet, packetInfo);
    }

    private void handleAll(Packet packet, Collection<PacketInfo> packetInfos) {
        for (PacketInfo packetInfo : packetInfos) {
            handlePacket(packet, packetInfo);
        }
    }

    private void handlePacket(Packet packet, PacketInfo packetInfo) {
        if (packetInfo == null) {
            logger.warn("packetInfo not exist, pakcetId: {}",
                    packet.getHeader().getPacketId());
            return;
        }
        PacketHandler packetHandler = packetInfo.getPacketHandler();
        if (packetHandler == null) {
            logger.warn("packetHandler is null, packetId: {}",
                    packet.getHeader().getPacketId());
            return;
        }
        try {
            packetHandler.handlePacket(packet, packetInfo.getArgs());
        } catch (RuntimeException e) {
            logger.error("handle packet causes RuntimeException: ", e);
        }
    }

}
