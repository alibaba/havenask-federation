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

package org.havenask.action.admin.cluster.snapshots.status;

import org.havenask.HavenaskException;
import org.havenask.action.ActionType;
import org.havenask.action.FailedNodeException;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.nodes.BaseNodeRequest;
import org.havenask.action.support.nodes.BaseNodeResponse;
import org.havenask.action.support.nodes.BaseNodesRequest;
import org.havenask.action.support.nodes.BaseNodesResponse;
import org.havenask.action.support.nodes.TransportNodesAction;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.index.shard.ShardId;
import org.havenask.index.snapshots.IndexShardSnapshotStatus;
import org.havenask.snapshots.Snapshot;
import org.havenask.snapshots.SnapshotShardsService;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Transport client that collects snapshot shard statuses from data nodes
 */
public class TransportNodesSnapshotsStatus extends TransportNodesAction<TransportNodesSnapshotsStatus.Request,
                                                                        TransportNodesSnapshotsStatus.NodesSnapshotStatus,
                                                                        TransportNodesSnapshotsStatus.NodeRequest,
                                                                        TransportNodesSnapshotsStatus.NodeSnapshotStatus> {

    public static final String ACTION_NAME = SnapshotsStatusAction.NAME + "[nodes]";
    public static final ActionType<NodesSnapshotStatus> TYPE = new ActionType<>(ACTION_NAME, NodesSnapshotStatus::new);

    private final SnapshotShardsService snapshotShardsService;

    public TransportNodesSnapshotsStatus(ThreadPool threadPool, ClusterService clusterService,
                                         TransportService transportService, SnapshotShardsService snapshotShardsService,
                                         ActionFilters actionFilters) {
        super(ACTION_NAME, threadPool, clusterService, transportService, actionFilters,
            Request::new, NodeRequest::new, ThreadPool.Names.GENERIC, NodeSnapshotStatus.class);
        this.snapshotShardsService = snapshotShardsService;
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeSnapshotStatus newNodeResponse(StreamInput in) throws IOException {
        return new NodeSnapshotStatus(in);
    }

    @Override
    protected NodesSnapshotStatus newResponse(Request request, List<NodeSnapshotStatus> responses, List<FailedNodeException> failures) {
        return new NodesSnapshotStatus(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeSnapshotStatus nodeOperation(NodeRequest request) {
        Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> snapshotMapBuilder = new HashMap<>();
        try {
            final String nodeId = clusterService.localNode().getId();
            for (Snapshot snapshot : request.snapshots) {
                Map<ShardId, IndexShardSnapshotStatus> shardsStatus = snapshotShardsService.currentSnapshotShards(snapshot);
                if (shardsStatus == null) {
                    continue;
                }
                Map<ShardId, SnapshotIndexShardStatus> shardMapBuilder = new HashMap<>();
                for (Map.Entry<ShardId, IndexShardSnapshotStatus> shardEntry : shardsStatus.entrySet()) {
                    final ShardId shardId = shardEntry.getKey();

                    final IndexShardSnapshotStatus.Copy lastSnapshotStatus = shardEntry.getValue().asCopy();
                    final IndexShardSnapshotStatus.Stage stage = lastSnapshotStatus.getStage();

                    String shardNodeId = null;
                    if (stage != IndexShardSnapshotStatus.Stage.DONE && stage != IndexShardSnapshotStatus.Stage.FAILURE) {
                        // Store node id for the snapshots that are currently running.
                        shardNodeId = nodeId;
                    }
                    shardMapBuilder.put(shardEntry.getKey(), new SnapshotIndexShardStatus(shardId, lastSnapshotStatus, shardNodeId));
                }
                snapshotMapBuilder.put(snapshot, unmodifiableMap(shardMapBuilder));
            }
            return new NodeSnapshotStatus(clusterService.localNode(), unmodifiableMap(snapshotMapBuilder));
        } catch (Exception e) {
            throw new HavenaskException("failed to load metadata", e);
        }
    }

    public static class Request extends BaseNodesRequest<Request> {

        private Snapshot[] snapshots;

        public Request(StreamInput in) throws IOException {
            super(in);
            // This operation is never executed remotely
            throw new UnsupportedOperationException("shouldn't be here");
        }

        public Request(String[] nodesIds) {
            super(nodesIds);
        }

        public Request snapshots(Snapshot[] snapshots) {
            this.snapshots = snapshots;
            return this;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // This operation is never executed remotely
            throw new UnsupportedOperationException("shouldn't be here");
        }
    }

    public static class NodesSnapshotStatus extends BaseNodesResponse<NodeSnapshotStatus> {

        public NodesSnapshotStatus(StreamInput in) throws IOException {
            super(in);
        }

        public NodesSnapshotStatus(ClusterName clusterName, List<NodeSnapshotStatus> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeSnapshotStatus> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeSnapshotStatus::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeSnapshotStatus> nodes) throws IOException {
            out.writeList(nodes);
        }
    }


    public static class NodeRequest extends BaseNodeRequest {

        private final List<Snapshot> snapshots;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            snapshots = in.readList(Snapshot::new);
        }

        NodeRequest(TransportNodesSnapshotsStatus.Request request) {
            snapshots = Arrays.asList(request.snapshots);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(snapshots);
        }
    }

    public static class NodeSnapshotStatus extends BaseNodeResponse {

        private final Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> status;

        public NodeSnapshotStatus(StreamInput in) throws IOException {
            super(in);
            status = unmodifiableMap(
                    in.readMap(Snapshot::new, input -> unmodifiableMap(input.readMap(ShardId::new, SnapshotIndexShardStatus::new))));
        }

        public NodeSnapshotStatus(DiscoveryNode node, Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> status) {
            super(node);
            this.status = status;
        }

        public Map<Snapshot, Map<ShardId, SnapshotIndexShardStatus>> status() {
            return status;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (status != null) {
                out.writeMap(status, (o, s) -> s.writeTo(o),
                        (output, v) -> output.writeMap(v, (o, shardId) -> shardId.writeTo(o), (o, sis) -> sis.writeTo(o)));
            } else {
                out.writeVInt(0);
            }
        }
    }
}
