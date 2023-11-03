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

package org.havenask.engine.rpc.arpc;

import com.alibaba.search.common.arpc.ANetRPCChannel;
import com.alibaba.search.common.arpc.ANetRPCChannelManager;
import com.alibaba.search.common.arpc.ANetRPCController;
import com.alibaba.search.common.arpc.exceptions.ArpcException;
import com.google.protobuf.ServiceException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.engine.rpc.HeartbeatTargetResponse;
import org.havenask.engine.rpc.SearcherClient;
import org.havenask.engine.rpc.UpdateHeartbeatTargetRequest;
import org.havenask.engine.rpc.WriteRequest;
import org.havenask.engine.rpc.WriteResponse;
import suez.service.proto.ErrorCode;
import suez.service.proto.ErrorInfo;
import suez.service.proto.TableService;
import suez.service.proto.Write;

import java.io.Closeable;
import java.io.IOException;

public class SearcherArpcClient implements SearcherClient, Closeable {
    private static final Logger logger = LogManager.getLogger(SearcherArpcClient.class);
    private final ANetRPCChannelManager manager;
    private volatile ANetRPCChannel channel;
    private volatile TableService.BlockingInterface blockingStub;
    private final ANetRPCController controller = new ANetRPCController();
    private final String host = "127.0.0.1";
    private final int port;

    public SearcherArpcClient(int port) {
        this.manager = new ANetRPCChannelManager();
        this.port = port;
    }

    @Override
    public HeartbeatTargetResponse getHeartbeatTarget() throws IOException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public HeartbeatTargetResponse updateHeartbeatTarget(UpdateHeartbeatTargetRequest request) throws IOException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public WriteResponse write(WriteRequest request) {
        long start = System.nanoTime();
        Write write = Write.newBuilder().setHashId(request.getHashid()).setStr(request.getSource()).build();
        suez.service.proto.WriteRequest writeRequest = suez.service.proto.WriteRequest.newBuilder()
            .setTableName(request.getTable())
            .setFormat("ha3")
            .addWrites(write)
            .build();
        try {
            if (blockingStub == null) {
                init();
            }
            suez.service.proto.WriteResponse writeResponse = blockingStub.writeTable(controller, writeRequest);
            if (logger.isDebugEnabled()) {
                long end = System.nanoTime();
                logger.debug("write {}, length: {}, cost: {} us", request.getTable(), request.getSource().length(), (end - start) / 1000);
            }

            if (writeResponse == null) {
                resetChannel();
                return new WriteResponse(ErrorCode.TBS_ERROR_UNKOWN, "write response is null, channel reset");
            }

            if (writeResponse.getErrorInfo() == null || writeResponse.getErrorInfo().getErrorCode() == ErrorCode.TBS_ERROR_NONE) {
                return new WriteResponse(writeResponse.getCheckpoint());
            } else {
                return new WriteResponse(writeResponse.getErrorInfo().getErrorCode(), writeResponse.getErrorInfo().getErrorMsg());
            }
        } catch (ServiceException e) {
            logger.warn("write service error", e);
            resetChannel();
            return new WriteResponse(ErrorCode.TBS_ERROR_UNKOWN, "service error:" + e.getMessage());
        } catch (Exception e) {
            logger.warn("write upexpect error", e);
            resetChannel();
            return new WriteResponse(ErrorCode.TBS_ERROR_UNKOWN, "upexpect error:" + e.getMessage());
        }
    }

    private boolean isWriteQueueFull(ErrorInfo errorInfo) {
        if (errorInfo != null
            && errorInfo.getErrorCode() == ErrorCode.TBS_ERROR_OTHERS
            && errorInfo.getErrorMsg().contains("doc queue is full")) {
            return true;
        } else {
            return false;
        }
    }

    private void resetChannel() {
        logger.info("searcher arpc client reset");
        try {
            manager.closeChannel(host, port);
            channel = manager.openChannel(host, port);
        } catch (ArpcException e) {
            logger.warn("reset channel error", e);
        }
        blockingStub = TableService.newBlockingStub(channel);
        logger.info("searcher arpc client reset, reset BlockingStub success");
        controller.reset();
        logger.info("searcher arpc client reset, reset RpcController success");
    }

    private void closeChannel() {
        logger.info("searcher arpc client close channel");
        channel = null;
        blockingStub = null;
        try {
            manager.closeChannel(host, port);
        } catch (ArpcException e) {
            logger.warn("close channel error", e);
        }
    }

    private void init() {
        logger.info("searcher arpc client init");
        channel = manager.openChannel(host, port);
        logger.info("Open Channel");
        blockingStub = TableService.newBlockingStub(channel);
        logger.info("Open BlockingStub");
    }

    @Override
    public void close() {
        manager.dispose();
    }
}
