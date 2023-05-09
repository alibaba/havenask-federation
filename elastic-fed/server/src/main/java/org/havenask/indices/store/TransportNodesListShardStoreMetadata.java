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

package org.havenask.indices.store;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.LegacyESVersion;
import org.havenask.HavenaskException;
import org.havenask.action.ActionListener;
import org.havenask.action.ActionType;
import org.havenask.action.FailedNodeException;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.nodes.BaseNodeRequest;
import org.havenask.action.support.nodes.BaseNodeResponse;
import org.havenask.action.support.nodes.BaseNodesRequest;
import org.havenask.action.support.nodes.BaseNodesResponse;
import org.havenask.action.support.nodes.TransportNodesAction;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Nullable;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.env.NodeEnvironment;
import org.havenask.gateway.AsyncShardFetch;
import org.havenask.index.IndexService;
import org.havenask.index.IndexSettings;
import org.havenask.index.seqno.ReplicationTracker;
import org.havenask.index.seqno.RetentionLease;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.ShardId;
import org.havenask.index.shard.ShardPath;
import org.havenask.index.store.Store;
import org.havenask.index.store.StoreFileMetadata;
import org.havenask.indices.IndicesService;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class TransportNodesListShardStoreMetadata extends TransportNodesAction<TransportNodesListShardStoreMetadata.Request,
    TransportNodesListShardStoreMetadata.NodesStoreFilesMetadata,
    TransportNodesListShardStoreMetadata.NodeRequest,
    TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata>
    implements AsyncShardFetch.Lister<TransportNodesListShardStoreMetadata.NodesStoreFilesMetadata,
    TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> {

    public static final String ACTION_NAME = "internal:cluster/nodes/indices/shard/store";
    public static final ActionType<NodesStoreFilesMetadata> TYPE = new ActionType<>(ACTION_NAME, NodesStoreFilesMetadata::new);

    private final Settings settings;
    private final IndicesService indicesService;
    private final NodeEnvironment nodeEnv;

    @Inject
    public TransportNodesListShardStoreMetadata(Settings settings, ThreadPool threadPool,
                                                ClusterService clusterService, TransportService transportService,
                                                IndicesService indicesService, NodeEnvironment nodeEnv,
                                                ActionFilters actionFilters) {
        super(ACTION_NAME, threadPool, clusterService, transportService, actionFilters,
            Request::new, NodeRequest::new, ThreadPool.Names.FETCH_SHARD_STORE, NodeStoreFilesMetadata.class);
        this.settings = settings;
        this.indicesService = indicesService;
        this.nodeEnv = nodeEnv;
    }

    @Override
    public void list(ShardId shardId, String customDataPath, DiscoveryNode[] nodes, ActionListener<NodesStoreFilesMetadata> listener) {
        execute(new Request(shardId, customDataPath, nodes), listener);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeStoreFilesMetadata newNodeResponse(StreamInput in) throws IOException {
        return new NodeStoreFilesMetadata(in);
    }

    @Override
    protected NodesStoreFilesMetadata newResponse(Request request,
                                                  List<NodeStoreFilesMetadata> responses, List<FailedNodeException> failures) {
        return new NodesStoreFilesMetadata(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeStoreFilesMetadata nodeOperation(NodeRequest request) {
        try {
            return new NodeStoreFilesMetadata(clusterService.localNode(), listStoreMetadata(request));
        } catch (IOException e) {
            throw new HavenaskException("Failed to list store metadata for shard [" + request.shardId + "]", e);
        }
    }

    private StoreFilesMetadata listStoreMetadata(NodeRequest request) throws IOException {
        final ShardId shardId = request.getShardId();
        logger.trace("listing store meta data for {}", shardId);
        long startTimeNS = System.nanoTime();
        boolean exists = false;
        try {
            IndexService indexService = indicesService.indexService(shardId.getIndex());
            if (indexService != null) {
                IndexShard indexShard = indexService.getShardOrNull(shardId.id());
                if (indexShard != null) {
                    try {
                        final StoreFilesMetadata storeFilesMetadata = new StoreFilesMetadata(shardId,
                            indexShard.snapshotStoreMetadata(), indexShard.getPeerRecoveryRetentionLeases());
                        exists = true;
                        return storeFilesMetadata;
                    } catch (org.apache.lucene.index.IndexNotFoundException e) {
                        logger.trace(new ParameterizedMessage("[{}] node is missing index, responding with empty", shardId), e);
                        return new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList());
                    } catch (IOException e) {
                        logger.warn(new ParameterizedMessage("[{}] can't read metadata from store, responding with empty", shardId), e);
                        return new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList());
                    }
                }
            }
            final String customDataPath;
            if (request.getCustomDataPath() != null) {
                customDataPath = request.getCustomDataPath();
            } else {
                // TODO: Fallback for BWC with older predecessor (ES) versions.
                //  Remove this once request.getCustomDataPath() always returns non-null
                if (indexService != null) {
                    customDataPath = indexService.getIndexSettings().customDataPath();
                } else {
                    IndexMetadata metadata = clusterService.state().metadata().index(shardId.getIndex());
                    if (metadata != null) {
                        customDataPath = new IndexSettings(metadata, settings).customDataPath();
                    } else {
                        logger.trace("{} node doesn't have meta data for the requests index", shardId);
                        throw new HavenaskException("node doesn't have meta data for index " + shardId.getIndex());
                    }
                }
            }
            final ShardPath shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, customDataPath);
            if (shardPath == null) {
                return new StoreFilesMetadata(shardId, Store.MetadataSnapshot.EMPTY, Collections.emptyList());
            }
            // note that this may fail if it can't get access to the shard lock. Since we check above there is an active shard, this means:
            // 1) a shard is being constructed, which means the master will not use a copy of this replica
            // 2) A shard is shutting down and has not cleared it's content within lock timeout. In this case the master may not
            //    reuse local resources.
            final Store.MetadataSnapshot metadataSnapshot =
                Store.readMetadataSnapshot(shardPath.resolveIndex(), shardId, nodeEnv::shardLock, logger);
            // We use peer recovery retention leases from the primary for allocating replicas. We should always have retention leases when
            // we refresh shard info after the primary has started. Hence, we can ignore retention leases if there is no active shard.
            return new StoreFilesMetadata(shardId, metadataSnapshot, Collections.emptyList());
        } finally {
            TimeValue took = new TimeValue(System.nanoTime() - startTimeNS, TimeUnit.NANOSECONDS);
            if (exists) {
                logger.debug("{} loaded store meta data (took [{}])", shardId, took);
            } else {
                logger.trace("{} didn't find any store meta data to load (took [{}])", shardId, took);
            }
        }
    }

    public static class StoreFilesMetadata implements Iterable<StoreFileMetadata>, Writeable {
        private final ShardId shardId;
        private final Store.MetadataSnapshot metadataSnapshot;
        private final List<RetentionLease> peerRecoveryRetentionLeases;

        public StoreFilesMetadata(ShardId shardId, Store.MetadataSnapshot metadataSnapshot,
                                  List<RetentionLease> peerRecoveryRetentionLeases) {
            this.shardId = shardId;
            this.metadataSnapshot = metadataSnapshot;
            this.peerRecoveryRetentionLeases = peerRecoveryRetentionLeases;
        }

        public StoreFilesMetadata(StreamInput in) throws IOException {
            this.shardId = new ShardId(in);
            this.metadataSnapshot = new Store.MetadataSnapshot(in);
            if (in.getVersion().onOrAfter(LegacyESVersion.V_7_5_0)) {
                this.peerRecoveryRetentionLeases = in.readList(RetentionLease::new);
            } else {
                this.peerRecoveryRetentionLeases = Collections.emptyList();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            metadataSnapshot.writeTo(out);
            if (out.getVersion().onOrAfter(LegacyESVersion.V_7_5_0)) {
                out.writeList(peerRecoveryRetentionLeases);
            }
        }

        public ShardId shardId() {
            return this.shardId;
        }

        public boolean isEmpty() {
            return metadataSnapshot.size() == 0;
        }

        @Override
        public Iterator<StoreFileMetadata> iterator() {
            return metadataSnapshot.iterator();
        }

        public boolean fileExists(String name) {
            return metadataSnapshot.asMap().containsKey(name);
        }

        public StoreFileMetadata file(String name) {
            return metadataSnapshot.asMap().get(name);
        }

        /**
         * Returns the retaining sequence number of the peer recovery retention lease for a given node if exists; otherwise, returns -1.
         */
        public long getPeerRecoveryRetentionLeaseRetainingSeqNo(DiscoveryNode node) {
            assert node != null;
            final String retentionLeaseId = ReplicationTracker.getPeerRecoveryRetentionLeaseId(node.getId());
            return peerRecoveryRetentionLeases.stream().filter(lease -> lease.id().equals(retentionLeaseId))
                .mapToLong(RetentionLease::retainingSequenceNumber).findFirst().orElse(-1L);
        }

        public List<RetentionLease> peerRecoveryRetentionLeases() {
            return peerRecoveryRetentionLeases;
        }

        /**
         * @return commit sync id if exists, else null
         */
        public String syncId() {
            return metadataSnapshot.getSyncId();
        }

        @Override
        public String toString() {
            return "StoreFilesMetadata{" +
                ", shardId=" + shardId +
                ", metadataSnapshot{size=" + metadataSnapshot.size() + ", syncId=" + metadataSnapshot.getSyncId() + "}" +
                '}';
        }
    }


    public static class Request extends BaseNodesRequest<Request> {

        private final ShardId shardId;
        @Nullable
        private final String customDataPath;

        public Request(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            if (in.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
                customDataPath = in.readString();
            } else {
                customDataPath = null;
            }
        }

        public Request(ShardId shardId, String customDataPath, DiscoveryNode[] nodes) {
            super(nodes);
            this.shardId = Objects.requireNonNull(shardId);
            this.customDataPath = Objects.requireNonNull(customDataPath);
        }

        public ShardId shardId() {
            return shardId;
        }

        /**
         * Returns the custom data path that is used to look up information for this shard.
         * Returns an empty string if no custom data path is used for this index.
         * Returns null if custom data path information is not available (due to BWC).
         */
        @Nullable
        public String getCustomDataPath() {
            return customDataPath;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            if (out.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
                out.writeString(customDataPath);
            }
        }
    }

    public static class NodesStoreFilesMetadata extends BaseNodesResponse<NodeStoreFilesMetadata> {

        public NodesStoreFilesMetadata(StreamInput in) throws IOException {
            super(in);
        }

        public NodesStoreFilesMetadata(ClusterName clusterName, List<NodeStoreFilesMetadata> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeStoreFilesMetadata> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeStoreFilesMetadata::readListShardStoreNodeOperationResponse);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeStoreFilesMetadata> nodes) throws IOException {
            out.writeList(nodes);
        }
    }


    public static class NodeRequest extends BaseNodeRequest {

        private final ShardId shardId;
        @Nullable
        private final String customDataPath;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            if (in.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
                customDataPath = in.readString();
            } else {
                customDataPath = null;
            }
        }

        public NodeRequest(Request request) {
            this.shardId = Objects.requireNonNull(request.shardId());
            this.customDataPath = Objects.requireNonNull(request.getCustomDataPath());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            if (out.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
                assert customDataPath != null;
                out.writeString(customDataPath);
            }
        }

        public ShardId getShardId() {
            return shardId;
        }

        /**
         * Returns the custom data path that is used to look up information for this shard.
         * Returns an empty string if no custom data path is used for this index.
         * Returns null if custom data path information is not available (due to BWC).
         */
        @Nullable
        public String getCustomDataPath() {
            return customDataPath;
        }
    }

    public static class NodeStoreFilesMetadata extends BaseNodeResponse {

        private StoreFilesMetadata storeFilesMetadata;

        public NodeStoreFilesMetadata(StreamInput in) throws IOException {
            super(in);
            storeFilesMetadata = new StoreFilesMetadata(in);
        }

        public NodeStoreFilesMetadata(DiscoveryNode node, StoreFilesMetadata storeFilesMetadata) {
            super(node);
            this.storeFilesMetadata = storeFilesMetadata;
        }

        public StoreFilesMetadata storeFilesMetadata() {
            return storeFilesMetadata;
        }

        public static NodeStoreFilesMetadata readListShardStoreNodeOperationResponse(StreamInput in) throws IOException {
            return new NodeStoreFilesMetadata(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            storeFilesMetadata.writeTo(out);
        }

        @Override
        public String toString() {
            return "[[" + getNode() + "][" + storeFilesMetadata + "]]";
        }
    }
}
