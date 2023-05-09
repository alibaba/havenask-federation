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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.gateway;

import org.havenask.action.ActionFuture;
import org.havenask.action.ActionType;
import org.havenask.action.FailedNodeException;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.PlainActionFuture;
import org.havenask.action.support.nodes.BaseNodeRequest;
import org.havenask.action.support.nodes.BaseNodeResponse;
import org.havenask.action.support.nodes.BaseNodesRequest;
import org.havenask.action.support.nodes.BaseNodesResponse;
import org.havenask.action.support.nodes.TransportNodesAction;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Nullable;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.unit.TimeValue;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportNodesListGatewayMetaState extends TransportNodesAction<TransportNodesListGatewayMetaState.Request,
                                                                             TransportNodesListGatewayMetaState.NodesGatewayMetaState,
                                                                             TransportNodesListGatewayMetaState.NodeRequest,
                                                                             TransportNodesListGatewayMetaState.NodeGatewayMetaState> {

    public static final String ACTION_NAME = "internal:gateway/local/meta_state";
    public static final ActionType<NodesGatewayMetaState> TYPE = new ActionType<>(ACTION_NAME, NodesGatewayMetaState::new);

    private final GatewayMetaState metaState;

    @Inject
    public TransportNodesListGatewayMetaState(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                              ActionFilters actionFilters, GatewayMetaState metaState) {
        super(ACTION_NAME, threadPool, clusterService, transportService, actionFilters,
            Request::new, NodeRequest::new, ThreadPool.Names.GENERIC, NodeGatewayMetaState.class);
        this.metaState = metaState;
    }

    public ActionFuture<NodesGatewayMetaState> list(String[] nodesIds, @Nullable TimeValue timeout) {
        PlainActionFuture<NodesGatewayMetaState> future = PlainActionFuture.newFuture();
        execute(new Request(nodesIds).timeout(timeout), future);
        return future;
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest();
    }

    @Override
    protected NodeGatewayMetaState newNodeResponse(StreamInput in) throws IOException {
        return new NodeGatewayMetaState(in);
    }

    @Override
    protected NodesGatewayMetaState newResponse(Request request, List<NodeGatewayMetaState> responses, List<FailedNodeException> failures) {
        return new NodesGatewayMetaState(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeGatewayMetaState nodeOperation(NodeRequest request) {
        return new NodeGatewayMetaState(clusterService.localNode(), metaState.getMetadata());
    }

    public static class Request extends BaseNodesRequest<Request> {

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request(String... nodesIds) {
            super(nodesIds);
        }
    }

    public static class NodesGatewayMetaState extends BaseNodesResponse<NodeGatewayMetaState> {

        public NodesGatewayMetaState(StreamInput in) throws IOException {
            super(in);
        }

        public NodesGatewayMetaState(ClusterName clusterName, List<NodeGatewayMetaState> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeGatewayMetaState> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeGatewayMetaState::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeGatewayMetaState> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    public static class NodeRequest extends BaseNodeRequest {
        NodeRequest() {}
        NodeRequest(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class NodeGatewayMetaState extends BaseNodeResponse {

        private Metadata metadata;

        public NodeGatewayMetaState(StreamInput in) throws IOException {
            super(in);
            if (in.readBoolean()) {
                metadata = Metadata.readFrom(in);
            }
        }

        public NodeGatewayMetaState(DiscoveryNode node, Metadata metadata) {
            super(node);
            this.metadata = metadata;
        }

        public Metadata metadata() {
            return metadata;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (metadata == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                metadata.writeTo(out);
            }
        }
    }
}
