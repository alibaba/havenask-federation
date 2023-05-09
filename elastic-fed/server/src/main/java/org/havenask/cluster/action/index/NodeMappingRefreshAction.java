/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.cluster.action.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.action.IndicesRequest;
import org.havenask.action.support.IndicesOptions;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.MetadataMappingService;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.EmptyTransportResponseHandler;
import org.havenask.transport.TransportChannel;
import org.havenask.transport.TransportRequest;
import org.havenask.transport.TransportRequestHandler;
import org.havenask.transport.TransportResponse;
import org.havenask.transport.TransportService;

import java.io.IOException;

public class NodeMappingRefreshAction {

    private static final Logger logger = LogManager.getLogger(NodeMappingRefreshAction.class);

    public static final String ACTION_NAME = "internal:cluster/node/mapping/refresh";

    private final TransportService transportService;
    private final MetadataMappingService metadataMappingService;

    @Inject
    public NodeMappingRefreshAction(TransportService transportService, MetadataMappingService metadataMappingService) {
        this.transportService = transportService;
        this.metadataMappingService = metadataMappingService;
        transportService.registerRequestHandler(ACTION_NAME,
           ThreadPool.Names.SAME,  NodeMappingRefreshRequest::new, new NodeMappingRefreshTransportHandler());
    }

    public void nodeMappingRefresh(final DiscoveryNode masterNode, final NodeMappingRefreshRequest request) {
        if (masterNode == null) {
            logger.warn("can't send mapping refresh for [{}], no master known.", request.index());
            return;
        }
        transportService.sendRequest(masterNode, ACTION_NAME, request, EmptyTransportResponseHandler.INSTANCE_SAME);
    }

    private class NodeMappingRefreshTransportHandler implements TransportRequestHandler<NodeMappingRefreshRequest> {

        @Override
        public void messageReceived(NodeMappingRefreshRequest request, TransportChannel channel, Task task) throws Exception {
            metadataMappingService.refreshMapping(request.index(), request.indexUUID());
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    public static class NodeMappingRefreshRequest extends TransportRequest implements IndicesRequest {

        private String index;
        private String indexUUID = IndexMetadata.INDEX_UUID_NA_VALUE;
        private String nodeId;

        public NodeMappingRefreshRequest(StreamInput in) throws IOException {
            super(in);
            index = in.readString();
            nodeId = in.readString();
            indexUUID = in.readString();
        }

        public NodeMappingRefreshRequest(String index, String indexUUID, String nodeId) {
            this.index = index;
            this.indexUUID = indexUUID;
            this.nodeId = nodeId;
        }

        @Override
        public String[] indices() {
            return new String[]{index};
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        public String index() {
            return index;
        }

        public String indexUUID() {
            return indexUUID;
        }

        public String nodeId() {
            return nodeId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            out.writeString(nodeId);
            out.writeString(indexUUID);
        }
    }
}
