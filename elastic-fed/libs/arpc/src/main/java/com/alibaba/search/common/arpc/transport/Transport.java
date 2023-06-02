/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.alibaba.search.common.arpc.transport;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.io.Connection;
import com.alibaba.search.common.arpc.packet.PacketStreamer;
import com.alibaba.search.common.arpc.util.Constants;
import com.alibaba.search.common.arpc.util.SystemTimer;

public abstract class Transport {
    private final static Logger logger = LoggerFactory
            .getLogger(Transport.class);
    private static int checkTimeoutInterval = Constants.CHECK_TIMEOUT_INTEVAL;

    static {
        try {
            String intStr = System.getProperty("arpc.checkTimeout.interval", String.valueOf(Constants.CHECK_TIMEOUT_INTEVAL));
            checkTimeoutInterval = Math.max(5, Integer.valueOf(intStr));
        }
        catch (Throwable t) {
            // ignore
        }
    }
    protected PacketStreamer packetStreamer = null;
    protected ArrayList<Connection> connections = new ArrayList<Connection>();
    protected CheckTimeoutThread checkTimeoutThread =  new CheckTimeoutThread();
    private SystemTimer timer = new SystemTimer();

    public Transport(PacketStreamer packetStreamer) {
        this.packetStreamer = packetStreamer;
    }

    public Connection createConnection(String host, int port) {
        Connection conn = doCreateConnection(host, port);
        if (conn != null) {
            synchronized (connections) {
                connections.add(conn);
            }
        }
        return conn;
    }


    public final void checkTimeout() {
        long now = timer.getCurrentTime();
        synchronized (connections) {
            for (Connection conn : connections) {
                conn.checkTimeout(now);
            }
        }
    }

    public void start() {
        try {
            checkTimeoutThread.start();
        } catch (IllegalThreadStateException e) {
            logger.warn("thread already started.");
        }
    }

    public void dispose() {
        for (Connection conn : connections) {
            conn.close();
        }
        connections.clear();
        checkTimeoutThread.setStop();
        try {
            checkTimeoutThread.join();
        } catch (InterruptedException e) {
            logger.warn("join check timeout thread catch exception: ", e);
        }
    }

    protected abstract Connection doCreateConnection(String host, int port);

    class CheckTimeoutThread extends Thread {
        private volatile boolean stop = false;

        public void setStop() {
            stop = true;
        }

        @Override
        public void run() {
            while (!stop) {
                checkTimeout();
                try {
                    sleep(checkTimeoutInterval);
                } catch (InterruptedException e) {
                    logger.warn("check timeout thread catch exception: ", e);
                }
            }
        }
    }
}
