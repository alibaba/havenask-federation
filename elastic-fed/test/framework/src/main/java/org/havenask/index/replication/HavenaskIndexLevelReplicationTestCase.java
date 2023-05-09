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

package org.havenask.index.replication;

import org.apache.lucene.store.AlreadyClosedException;
import org.havenask.Version;
import org.havenask.action.ActionListener;
import org.havenask.action.DocWriteRequest;
import org.havenask.action.DocWriteResponse;
import org.havenask.action.admin.indices.flush.FlushRequest;
import org.havenask.action.bulk.BulkItemRequest;
import org.havenask.action.bulk.BulkItemResponse;
import org.havenask.action.bulk.BulkShardRequest;
import org.havenask.action.bulk.BulkShardResponse;
import org.havenask.action.bulk.MappingUpdatePerformer;
import org.havenask.action.bulk.TransportShardBulkAction;
import org.havenask.action.delete.DeleteRequest;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.resync.ResyncReplicationRequest;
import org.havenask.action.resync.ResyncReplicationResponse;
import org.havenask.action.resync.TransportResyncReplicationAction;
import org.havenask.action.support.ActionTestUtils;
import org.havenask.action.support.PlainActionFuture;
import org.havenask.action.support.WriteRequest;
import org.havenask.action.support.replication.PendingReplicationActions;
import org.havenask.action.support.replication.ReplicatedWriteRequest;
import org.havenask.action.support.replication.ReplicationOperation;
import org.havenask.action.support.replication.ReplicationRequest;
import org.havenask.action.support.replication.ReplicationResponse;
import org.havenask.action.support.replication.TransportReplicationAction;
import org.havenask.action.support.replication.TransportReplicationAction.ReplicaResponse;
import org.havenask.action.support.replication.TransportWriteAction;
import org.havenask.action.support.replication.TransportWriteActionTestHelper;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodeRole;
import org.havenask.cluster.routing.AllocationId;
import org.havenask.cluster.routing.IndexShardRoutingTable;
import org.havenask.cluster.routing.RecoverySource;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.ShardRoutingHelper;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.cluster.routing.TestShardRouting;
import org.havenask.common.collect.Iterators;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.lease.Releasable;
import org.havenask.common.lease.Releasables;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.Index;
import org.havenask.index.IndexSettings;
import org.havenask.index.engine.DocIdSeqNoAndSource;
import org.havenask.index.engine.EngineFactory;
import org.havenask.index.engine.InternalEngineFactory;
import org.havenask.index.seqno.GlobalCheckpointSyncAction;
import org.havenask.index.seqno.RetentionLease;
import org.havenask.index.seqno.RetentionLeaseSyncAction;
import org.havenask.index.seqno.RetentionLeaseSyncer;
import org.havenask.index.seqno.RetentionLeases;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.IndexShardTestCase;
import org.havenask.index.shard.PrimaryReplicaSyncer;
import org.havenask.index.shard.ShardId;
import org.havenask.index.shard.ShardPath;
import org.havenask.index.translog.Translog;
import org.havenask.indices.recovery.RecoveryState;
import org.havenask.indices.recovery.RecoveryTarget;
import org.havenask.tasks.TaskManager;
import org.havenask.threadpool.ThreadPool;
import org.havenask.threadpool.ThreadPool.Names;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public abstract class HavenaskIndexLevelReplicationTestCase extends IndexShardTestCase {

    protected final Index index = new Index("test", "uuid");
    private final ShardId shardId = new ShardId(index, 0);
    protected final Map<String, String> indexMapping = Collections.singletonMap("type", "{ \"type\": {} }");

    protected ReplicationGroup createGroup(int replicas) throws IOException {
        return createGroup(replicas, Settings.EMPTY);
    }

    protected ReplicationGroup createGroup(int replicas, Settings settings) throws IOException {
        IndexMetadata metadata = buildIndexMetadata(replicas, settings, indexMapping);
        return new ReplicationGroup(metadata);
    }

    protected IndexMetadata buildIndexMetadata(int replicas) throws IOException {
        return buildIndexMetadata(replicas, indexMapping);
    }

    protected IndexMetadata buildIndexMetadata(int replicas, Map<String, String> mappings) throws IOException {
        return buildIndexMetadata(replicas, Settings.EMPTY, mappings);
    }

    protected IndexMetadata buildIndexMetadata(int replicas, Settings indexSettings, Map<String, String> mappings) throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean())
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(),
                randomBoolean() ? IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.get(Settings.EMPTY) : between(0, 1000))
            .put(indexSettings)
            .build();
        IndexMetadata.Builder metadata = IndexMetadata.builder(index.getName())
            .settings(settings)
            .primaryTerm(0, randomIntBetween(1, 100));
        for (Map.Entry<String, String> typeMapping : mappings.entrySet()) {
            metadata.putMapping(typeMapping.getKey(), typeMapping.getValue());
        }
        return metadata.build();
    }

    IndexRequest copyIndexRequest(IndexRequest inRequest) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            inRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new IndexRequest(in);
            }
        }
    }

    protected DiscoveryNode getDiscoveryNode(String id) {
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE), Version.CURRENT);
    }

    protected class ReplicationGroup implements AutoCloseable, Iterable<IndexShard> {
        private IndexShard primary;
        private IndexMetadata indexMetadata;
        private final List<IndexShard> replicas;
        private final AtomicInteger replicaId = new AtomicInteger();
        private final AtomicInteger docId = new AtomicInteger();
        boolean closed = false;
        private volatile ReplicationTargets replicationTargets;

        private final PrimaryReplicaSyncer primaryReplicaSyncer = new PrimaryReplicaSyncer(
            new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet()),
            (request, parentTask, primaryAllocationId, primaryTerm, listener) -> {
                try {
                    new ResyncAction(request, listener, ReplicationGroup.this).execute();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });

        private final RetentionLeaseSyncer retentionLeaseSyncer = new RetentionLeaseSyncer(
            (shardId, primaryAllocationId, primaryTerm, retentionLeases, listener) ->
                syncRetentionLeases(shardId, retentionLeases, listener),
            (shardId, primaryAllocationId, primaryTerm, retentionLeases) -> syncRetentionLeases(shardId, retentionLeases,
                ActionListener.wrap(
                    r -> { },
                    e -> {
                        throw new AssertionError("failed to background sync retention lease", e);
                    })));

        protected ReplicationGroup(final IndexMetadata indexMetadata) throws IOException {
            final ShardRouting primaryRouting = this.createShardRouting("s0", true);
            primary = newShard(
                primaryRouting, indexMetadata, null, getEngineFactory(primaryRouting), () -> {}, retentionLeaseSyncer);
            replicas = new CopyOnWriteArrayList<>();
            this.indexMetadata = indexMetadata;
            updateAllocationIDsOnPrimary();
            for (int i = 0; i < indexMetadata.getNumberOfReplicas(); i++) {
                addReplica();
            }
        }

        private ShardRouting createShardRouting(String nodeId, boolean primary) {
            return TestShardRouting.newShardRouting(shardId, nodeId, primary, ShardRoutingState.INITIALIZING,
                primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE);
        }

        protected EngineFactory getEngineFactory(ShardRouting routing) {
            return new InternalEngineFactory();
        }

        public int indexDocs(final int numOfDoc) throws Exception {
            for (int doc = 0; doc < numOfDoc; doc++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName(), "type", Integer.toString(docId.incrementAndGet()))
                        .source("{}", XContentType.JSON);
                final BulkItemResponse response = index(indexRequest);
                if (response.isFailed()) {
                    throw response.getFailure().getCause();
                } else {
                    assertEquals(DocWriteResponse.Result.CREATED, response.getResponse().getResult());
                }
            }
            return numOfDoc;
        }

        public int appendDocs(final int numOfDoc) throws Exception {
            for (int doc = 0; doc < numOfDoc; doc++) {
                final IndexRequest indexRequest = new IndexRequest(index.getName(), "type").source("{}", XContentType.JSON);
                final BulkItemResponse response = index(indexRequest);
                if (response.isFailed()) {
                    throw response.getFailure().getCause();
                } else if (response.isFailed() == false) {
                    assertEquals(DocWriteResponse.Result.CREATED, response.getResponse().getResult());
                }
            }
            return numOfDoc;
        }

        public BulkItemResponse index(IndexRequest indexRequest) throws Exception {
            return executeWriteRequest(indexRequest, indexRequest.getRefreshPolicy());
        }

        public BulkItemResponse delete(DeleteRequest deleteRequest) throws Exception {
            return executeWriteRequest(deleteRequest, deleteRequest.getRefreshPolicy());
        }

        private BulkItemResponse executeWriteRequest(
            DocWriteRequest<?> writeRequest, WriteRequest.RefreshPolicy refreshPolicy) throws Exception {
            PlainActionFuture<BulkItemResponse> listener = new PlainActionFuture<>();
            final ActionListener<BulkShardResponse> wrapBulkListener =
                ActionListener.map(listener, bulkShardResponse -> bulkShardResponse.getResponses()[0]);
            BulkItemRequest[] items = new BulkItemRequest[1];
            items[0] = new BulkItemRequest(0, writeRequest);
            BulkShardRequest request = new BulkShardRequest(shardId, refreshPolicy, items);
            new WriteReplicationAction(request, wrapBulkListener, this).execute();
            return listener.get();
        }

        public synchronized void startAll() throws IOException {
            startReplicas(replicas.size());
        }

        public synchronized int startReplicas(int numOfReplicasToStart) throws IOException {
            if (primary.routingEntry().initializing()) {
                startPrimary();
            }
            int started = 0;
            for (IndexShard replicaShard : replicas) {
                if (replicaShard.routingEntry().initializing()) {
                    recoverReplica(replicaShard);
                    started++;
                    if (started > numOfReplicasToStart) {
                        break;
                    }
                }
            }
            return started;
        }

        public void startPrimary() throws IOException {
            recoverPrimary(primary);
            computeReplicationTargets();
            HashSet<String> activeIds = new HashSet<>();
            activeIds.addAll(activeIds());
            activeIds.add(primary.routingEntry().allocationId().getId());
            ShardRouting startedRoutingEntry = ShardRoutingHelper.moveToStarted(primary.routingEntry());
            IndexShardRoutingTable routingTable = routingTable(shr -> shr == primary.routingEntry() ? startedRoutingEntry : shr);
            primary.updateShardState(startedRoutingEntry, primary.getPendingPrimaryTerm(), null,
                currentClusterStateVersion.incrementAndGet(), activeIds, routingTable);
            for (final IndexShard replica : replicas) {
                recoverReplica(replica);
            }
            computeReplicationTargets();
        }

        public IndexShard addReplica() throws IOException {
            final ShardRouting replicaRouting = createShardRouting("s" + replicaId.incrementAndGet(), false);
            final IndexShard replica =
                newShard(replicaRouting, indexMetadata, null, getEngineFactory(replicaRouting), () -> {}, retentionLeaseSyncer);
            addReplica(replica);
            return replica;
        }

        public synchronized void addReplica(IndexShard replica) throws IOException {
            assert shardRoutings().stream().anyMatch(shardRouting -> shardRouting.isSameAllocation(replica.routingEntry())) == false :
                "replica with aId [" + replica.routingEntry().allocationId() + "] already exists";
            replicas.add(replica);
            if (replicationTargets != null) {
                replicationTargets.addReplica(replica);
            }
            updateAllocationIDsOnPrimary();
        }

        protected synchronized void recoverPrimary(IndexShard primary) {
            final DiscoveryNode pNode = getDiscoveryNode(primary.routingEntry().currentNodeId());
            primary.markAsRecovering("store", new RecoveryState(primary.routingEntry(), pNode, null));
            recoverFromStore(primary);
        }

        public synchronized IndexShard addReplicaWithExistingPath(final ShardPath shardPath, final String nodeId) throws IOException {
            final ShardRouting shardRouting = TestShardRouting.newShardRouting(
                    shardId,
                    nodeId,
                    false, ShardRoutingState.INITIALIZING,
                    RecoverySource.PeerRecoverySource.INSTANCE);

            final IndexShard newReplica =
                    newShard(shardRouting, shardPath, indexMetadata, null, null, getEngineFactory(shardRouting),
                            () -> {}, retentionLeaseSyncer, EMPTY_EVENT_LISTENER);
            replicas.add(newReplica);
            if (replicationTargets != null) {
                replicationTargets.addReplica(newReplica);
            }
            updateAllocationIDsOnPrimary();
            return newReplica;
        }

        public synchronized List<IndexShard> getReplicas() {
            return Collections.unmodifiableList(replicas);
        }

        /**
         * promotes the specific replica as the new primary
         */
        public Future<PrimaryReplicaSyncer.ResyncTask> promoteReplicaToPrimary(IndexShard replica) throws IOException {
            PlainActionFuture<PrimaryReplicaSyncer.ResyncTask> fut = new PlainActionFuture<>();
            promoteReplicaToPrimary(replica, (shard, listener) -> {
                computeReplicationTargets();
                primaryReplicaSyncer.resync(shard,
                    new ActionListener<PrimaryReplicaSyncer.ResyncTask>() {
                        @Override
                        public void onResponse(PrimaryReplicaSyncer.ResyncTask resyncTask) {
                            listener.onResponse(resyncTask);
                            fut.onResponse(resyncTask);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                            fut.onFailure(e);
                        }
                    });
            });
            return fut;
        }

        public synchronized void promoteReplicaToPrimary(IndexShard replica,
                 BiConsumer<IndexShard, ActionListener<PrimaryReplicaSyncer.ResyncTask>> primaryReplicaSyncer)
            throws IOException {
            final long newTerm = indexMetadata.primaryTerm(shardId.id()) + 1;
            IndexMetadata.Builder newMetadata = IndexMetadata.builder(indexMetadata).primaryTerm(shardId.id(), newTerm);
            indexMetadata = newMetadata.build();
            assertTrue(replicas.remove(replica));
            closeShards(primary);
            primary = replica;
            assert primary.routingEntry().active() : "only active replicas can be promoted to primary: " + primary.routingEntry();
            ShardRouting primaryRouting = replica.routingEntry().moveActiveReplicaToPrimary();
            IndexShardRoutingTable routingTable = routingTable(shr -> shr == replica.routingEntry() ? primaryRouting : shr);

            primary.updateShardState(primaryRouting, newTerm, primaryReplicaSyncer, currentClusterStateVersion.incrementAndGet(),
                activeIds(), routingTable);
        }

        private synchronized Set<String> activeIds() {
            return shardRoutings().stream()
                .filter(ShardRouting::active).map(ShardRouting::allocationId).map(AllocationId::getId).collect(Collectors.toSet());
        }

        private synchronized IndexShardRoutingTable routingTable(Function<ShardRouting, ShardRouting> transformer) {
            IndexShardRoutingTable.Builder routingTable = new IndexShardRoutingTable.Builder(primary.shardId());
            shardRoutings().stream().map(transformer).forEach(routingTable::addShard);
            return routingTable.build();
        }

        public synchronized boolean removeReplica(IndexShard replica) throws IOException {
            final boolean removed = replicas.remove(replica);
            if (removed) {
                updateAllocationIDsOnPrimary();
                computeReplicationTargets();
            }
            return removed;
        }

        public void recoverReplica(IndexShard replica) throws IOException {
            recoverReplica(replica, (r, sourceNode) -> new RecoveryTarget(r, sourceNode, recoveryListener));
        }

        public void recoverReplica(IndexShard replica, BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier)
            throws IOException {
            recoverReplica(replica, targetSupplier, true);
        }

        public void recoverReplica(
            IndexShard replica,
            BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier,
            boolean markAsRecovering) throws IOException {
            final IndexShardRoutingTable routingTable = routingTable(Function.identity());
            final Set<String> inSyncIds = activeIds();
            HavenaskIndexLevelReplicationTestCase.this.recoverUnstartedReplica(
                replica, primary, targetSupplier, markAsRecovering, inSyncIds, routingTable);
            HavenaskIndexLevelReplicationTestCase.this.startReplicaAfterRecovery(replica, primary, inSyncIds, routingTable);
            computeReplicationTargets();
        }

        public synchronized DiscoveryNode getPrimaryNode() {
            return getDiscoveryNode(primary.routingEntry().currentNodeId());
        }

        public Future<Void> asyncRecoverReplica(
                final IndexShard replica, final BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier) {
            final FutureTask<Void> task = new FutureTask<>(() -> {
                recoverReplica(replica, targetSupplier);
                return null;
            });
            threadPool.generic().execute(task);
            return task;
        }

        public synchronized void assertAllEqual(int expectedCount) throws IOException {
            Set<String> primaryIds = getShardDocUIDs(primary);
            assertThat(primaryIds.size(), equalTo(expectedCount));
            for (IndexShard replica : replicas) {
                Set<String> replicaIds = getShardDocUIDs(replica);
                Set<String> temp = new HashSet<>(primaryIds);
                temp.removeAll(replicaIds);
                assertThat(replica.routingEntry() + " is missing docs", temp, empty());
                temp = new HashSet<>(replicaIds);
                temp.removeAll(primaryIds);
                assertThat(replica.routingEntry() + " has extra docs", temp, empty());
            }
        }

        public synchronized void refresh(String source) {
            for (IndexShard shard : this) {
                shard.refresh(source);
            }
        }

        public synchronized void flush() {
            final FlushRequest request = new FlushRequest();
            for (IndexShard shard : this) {
                shard.flush(request);
            }
        }

        public synchronized List<ShardRouting> shardRoutings() {
            return StreamSupport.stream(this.spliterator(), false).map(IndexShard::routingEntry).collect(Collectors.toList());
        }

        @Override
        public synchronized void close() throws Exception {
            if (closed == false) {
                closed = true;
                try {
                    final List<DocIdSeqNoAndSource> docsOnPrimary = getDocIdAndSeqNos(primary);
                    for (IndexShard replica : replicas) {
                        assertThat(replica.getMaxSeenAutoIdTimestamp(), equalTo(primary.getMaxSeenAutoIdTimestamp()));
                        assertThat(replica.getMaxSeqNoOfUpdatesOrDeletes(), greaterThanOrEqualTo(primary.getMaxSeqNoOfUpdatesOrDeletes()));
                        assertThat(getDocIdAndSeqNos(replica), equalTo(docsOnPrimary));
                    }
                } catch (AlreadyClosedException ignored) { }
                closeShards(this);
            } else {
                throw new AlreadyClosedException("too bad");
            }
        }

        @Override
        public Iterator<IndexShard> iterator() {
            return Iterators.concat(replicas.iterator(), Collections.singleton(primary).iterator());
        }

        public synchronized IndexShard getPrimary() {
            return primary;
        }

        public synchronized void reinitPrimaryShard() throws IOException {
            primary = reinitShard(primary);
            computeReplicationTargets();
        }

        public void syncGlobalCheckpoint() {
            PlainActionFuture<ReplicationResponse> listener = new PlainActionFuture<>();
            try {
                new GlobalCheckpointSync(listener, this).execute();
                listener.get();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        private void updateAllocationIDsOnPrimary() throws IOException {

            primary.updateShardState(primary.routingEntry(), primary.getPendingPrimaryTerm(), null,
                currentClusterStateVersion.incrementAndGet(),
                activeIds(), routingTable(Function.identity()));
        }

        private synchronized void computeReplicationTargets() {
            this.replicationTargets = new ReplicationTargets(this.primary, new ArrayList<>(this.replicas));
        }

        private ReplicationTargets getReplicationTargets() {
            return replicationTargets;
        }

        protected void syncRetentionLeases(ShardId shardId, RetentionLeases leases, ActionListener<ReplicationResponse> listener) {
            new SyncRetentionLeases(new RetentionLeaseSyncAction.Request(shardId, leases), this,
                ActionListener.map(listener, r -> new ReplicationResponse())).execute();
        }

        public synchronized RetentionLease addRetentionLease(String id, long retainingSequenceNumber, String source,
                                                ActionListener<ReplicationResponse> listener) {
            return getPrimary().addRetentionLease(id, retainingSequenceNumber, source, listener);
        }

        public synchronized RetentionLease renewRetentionLease(String id, long retainingSequenceNumber, String source) {
            return getPrimary().renewRetentionLease(id, retainingSequenceNumber, source);
        }

        public synchronized void removeRetentionLease(String id, ActionListener<ReplicationResponse> listener) {
            getPrimary().removeRetentionLease(id, listener);
        }

        public void executeRetentionLeasesSyncRequestOnReplica(RetentionLeaseSyncAction.Request request, IndexShard replica) {
            final PlainActionFuture<Releasable> acquirePermitFuture = new PlainActionFuture<>();
            replica.acquireReplicaOperationPermit(getPrimary().getOperationPrimaryTerm(), getPrimary().getLastKnownGlobalCheckpoint(),
                getPrimary().getMaxSeqNoOfUpdatesOrDeletes(), acquirePermitFuture, ThreadPool.Names.SAME, request);
            try (Releasable ignored = acquirePermitFuture.actionGet()) {
                replica.updateRetentionLeasesOnReplica(request.getRetentionLeases());
                replica.persistRetentionLeases();
            } catch (Exception e) {
                throw new AssertionError("failed to execute retention lease request on replica [" + replica.routingEntry() + "]", e);
            }
        }
    }

    static final class ReplicationTargets {
        final IndexShard primary;
        final List<IndexShard> replicas;

        ReplicationTargets(IndexShard primary, List<IndexShard> replicas) {
            this.primary = primary;
            this.replicas = replicas;
        }

        /**
         * This does not modify the replication targets, but only adds a replica to the list.
         * If the targets is updated to include the given replica, a replication action would
         * be able to find this replica to execute write requests on it.
         */
        synchronized void addReplica(IndexShard replica) {
            replicas.add(replica);
        }

        synchronized IndexShard findReplicaShard(ShardRouting replicaRouting) {
            for (IndexShard replica : replicas) {
                if (replica.routingEntry().isSameAllocation(replicaRouting)) {
                    return replica;
                }
            }
            throw new AssertionError("replica [" + replicaRouting + "] is not found; replicas[" + replicas + "] primary[" + primary + "]");
        }
    }

    protected abstract class ReplicationAction<Request extends ReplicationRequest<Request>,
        ReplicaRequest extends ReplicationRequest<ReplicaRequest>,
        Response extends ReplicationResponse> {
        private final Request request;
        private ActionListener<Response> listener;
        private final ReplicationTargets replicationTargets;
        private final String opType;

        protected ReplicationAction(Request request, ActionListener<Response> listener, ReplicationGroup group, String opType) {
            this.request = request;
            this.listener = listener;
            this.replicationTargets = group.getReplicationTargets();
            this.opType = opType;
        }

        public void execute() {
            try {
                new ReplicationOperation<>(request, new PrimaryRef(),
                    ActionListener.map(listener, result -> {
                        adaptResponse(result.finalResponse, getPrimaryShard());
                        return result.finalResponse;
                    }),
                    new ReplicasRef(), logger, threadPool, opType, primaryTerm, TimeValue.timeValueMillis(20),
                    TimeValue.timeValueSeconds(60))
                    .execute();
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        // to be overridden by subclasses
        protected void adaptResponse(Response response, IndexShard indexShard) {

        }

        protected IndexShard getPrimaryShard() {
            return replicationTargets.primary;
        }

        protected abstract void performOnPrimary(IndexShard primary, Request request, ActionListener<PrimaryResult> listener);

        protected abstract void performOnReplica(ReplicaRequest request, IndexShard replica) throws Exception;

        class PrimaryRef implements ReplicationOperation.Primary<Request, ReplicaRequest, PrimaryResult> {

            @Override
            public ShardRouting routingEntry() {
                return getPrimaryShard().routingEntry();
            }

            @Override
            public void failShard(String message, Exception exception) {
                throw new UnsupportedOperationException("failing a primary isn't supported. failure: " + message, exception);
            }

            @Override
            public void perform(Request request, ActionListener<PrimaryResult> listener) {
                performOnPrimary(getPrimaryShard(), request, listener);
            }

            @Override
            public void updateLocalCheckpointForShard(String allocationId, long checkpoint) {
                getPrimaryShard().updateLocalCheckpointForShard(allocationId, checkpoint);
            }

            @Override
            public void updateGlobalCheckpointForShard(String allocationId, long globalCheckpoint) {
                getPrimaryShard().updateGlobalCheckpointForShard(allocationId, globalCheckpoint);
            }

            @Override
            public long localCheckpoint() {
                return getPrimaryShard().getLocalCheckpoint();
            }

            @Override
            public long globalCheckpoint() {
                return getPrimaryShard().getLastSyncedGlobalCheckpoint();
            }

            @Override
            public long computedGlobalCheckpoint() {
                return getPrimaryShard().getLastKnownGlobalCheckpoint();
            }

            @Override
            public long maxSeqNoOfUpdatesOrDeletes() {
                return getPrimaryShard().getMaxSeqNoOfUpdatesOrDeletes();
            }

            @Override
            public org.havenask.index.shard.ReplicationGroup getReplicationGroup() {
                return getPrimaryShard().getReplicationGroup();
            }

            @Override
            public PendingReplicationActions getPendingReplicationActions() {
                return getPrimaryShard().getPendingReplicationActions();
            }
        }

        class ReplicasRef implements ReplicationOperation.Replicas<ReplicaRequest> {

            @Override
            public void performOn(
                final ShardRouting replicaRouting,
                final ReplicaRequest request,
                final long primaryTerm,
                final long globalCheckpoint,
                final long maxSeqNoOfUpdatesOrDeletes,
                final ActionListener<ReplicationOperation.ReplicaResponse> listener) {
                IndexShard replica = replicationTargets.findReplicaShard(replicaRouting);
                replica.acquireReplicaOperationPermit(
                    getPrimaryShard().getPendingPrimaryTerm(),
                    globalCheckpoint,
                    maxSeqNoOfUpdatesOrDeletes,
                    ActionListener.delegateFailure(listener, (delegatedListener, releasable) -> {
                        try {
                            performOnReplica(request, replica);
                            releasable.close();
                            delegatedListener.onResponse(new ReplicaResponse(replica.getLocalCheckpoint(),
                                replica.getLastKnownGlobalCheckpoint()));
                        } catch (final Exception e) {
                            Releasables.closeWhileHandlingException(releasable);
                            delegatedListener.onFailure(e);
                        }
                    }),
                    ThreadPool.Names.WRITE, request);
            }

            @Override
            public void failShardIfNeeded(ShardRouting replica, long primaryTerm, String message, Exception exception,
                                          ActionListener<Void> listener) {
                throw new UnsupportedOperationException("failing shard " + replica + " isn't supported. failure: " + message, exception);
            }

            @Override
            public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, long primaryTerm,
                                                     ActionListener<Void> listener) {
                throw new UnsupportedOperationException("can't mark " + shardId  + ", aid [" + allocationId + "] as stale");
            }
        }

        protected class PrimaryResult implements ReplicationOperation.PrimaryResult<ReplicaRequest> {
            final ReplicaRequest replicaRequest;
            final Response finalResponse;

            public PrimaryResult(ReplicaRequest replicaRequest, Response finalResponse) {
                this.replicaRequest = replicaRequest;
                this.finalResponse = finalResponse;
            }

            @Override
            public ReplicaRequest replicaRequest() {
                return replicaRequest;
            }

            @Override
            public void setShardInfo(ReplicationResponse.ShardInfo shardInfo) {
                finalResponse.setShardInfo(shardInfo);
            }

            @Override
            public void runPostReplicationActions(ActionListener<Void> listener) {
                listener.onResponse(null);
            }
        }

    }

    class WriteReplicationAction extends ReplicationAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

        WriteReplicationAction(BulkShardRequest request, ActionListener<BulkShardResponse> listener, ReplicationGroup replicationGroup) {
            super(request, listener, replicationGroup, "indexing");
        }

        @Override
        protected void performOnPrimary(IndexShard primary, BulkShardRequest request, ActionListener<PrimaryResult> listener) {
            executeShardBulkOnPrimary(primary, request,
                ActionListener.map(listener, result -> new PrimaryResult(result.replicaRequest(), result.finalResponseIfSuccessful)));
        }

        @Override
        protected void performOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
            executeShardBulkOnReplica(request, replica, getPrimaryShard().getPendingPrimaryTerm(),
                getPrimaryShard().getLastKnownGlobalCheckpoint(), getPrimaryShard().getMaxSeqNoOfUpdatesOrDeletes());
        }
    }

    private void executeShardBulkOnPrimary(IndexShard primary, BulkShardRequest request,
        ActionListener<TransportWriteAction.WritePrimaryResult<BulkShardRequest, BulkShardResponse>> listener) {
        for (BulkItemRequest itemRequest : request.items()) {
            if (itemRequest.request() instanceof IndexRequest) {
                ((IndexRequest) itemRequest.request()).process(Version.CURRENT, null, index.getName());
            }
        }
        final PlainActionFuture<Releasable> permitAcquiredFuture = new PlainActionFuture<>();
        primary.acquirePrimaryOperationPermit(permitAcquiredFuture, ThreadPool.Names.SAME, request);
        try (Releasable ignored = permitAcquiredFuture.actionGet()) {
            MappingUpdatePerformer noopMappingUpdater = (update, shardId, type, listener1) -> {};
            TransportShardBulkAction.performOnPrimary(request, primary, null, System::currentTimeMillis, noopMappingUpdater,
                null, ActionTestUtils.assertNoFailureListener(result -> {
                    TransportWriteActionTestHelper.performPostWriteActions(primary, request,
                        ((TransportWriteAction.WritePrimaryResult<BulkShardRequest, BulkShardResponse>) result).location, logger);
                    listener.onResponse((TransportWriteAction.WritePrimaryResult<BulkShardRequest, BulkShardResponse>) result);
                }), threadPool, Names.WRITE);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private <Request extends ReplicatedWriteRequest & DocWriteRequest> BulkShardRequest executeReplicationRequestOnPrimary(
            IndexShard primary, Request request) throws Exception {
        final BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, request.getRefreshPolicy(),
            new BulkItemRequest[]{new BulkItemRequest(0, request)});
        final PlainActionFuture<BulkShardRequest> res = new PlainActionFuture<>();
        executeShardBulkOnPrimary(
            primary, bulkShardRequest, ActionListener.map(res, TransportReplicationAction.PrimaryResult::replicaRequest));
        return res.get();
    }

    private void executeShardBulkOnReplica(BulkShardRequest request, IndexShard replica, long operationPrimaryTerm,
                                           long globalCheckpointOnPrimary, long maxSeqNoOfUpdatesOrDeletes) throws Exception {
        final PlainActionFuture<Releasable> permitAcquiredFuture = new PlainActionFuture<>();
        replica.acquireReplicaOperationPermit(operationPrimaryTerm, globalCheckpointOnPrimary,
            maxSeqNoOfUpdatesOrDeletes, permitAcquiredFuture, ThreadPool.Names.SAME, request);
        final Translog.Location location;
        try (Releasable ignored = permitAcquiredFuture.actionGet()) {
            location = TransportShardBulkAction.performOnReplica(request, replica);
        }
        TransportWriteActionTestHelper.performPostWriteActions(replica, request, location, logger);
    }

    /**
     * indexes the given requests on the supplied primary, modifying it for replicas
     */
    public BulkShardRequest indexOnPrimary(IndexRequest request, IndexShard primary) throws Exception {
        return executeReplicationRequestOnPrimary(primary, request);
    }

    /**
     * Executes the delete request on the primary, and modifies it for replicas.
     */
    BulkShardRequest deleteOnPrimary(DeleteRequest request, IndexShard primary) throws Exception {
        return executeReplicationRequestOnPrimary(primary, request);
    }

    /**
     * indexes the given requests on the supplied replica shard
     */
    public void indexOnReplica(BulkShardRequest request, ReplicationGroup group, IndexShard replica) throws Exception {
        indexOnReplica(request, group, replica, group.primary.getPendingPrimaryTerm());
    }

    void indexOnReplica(BulkShardRequest request, ReplicationGroup group, IndexShard replica, long term) throws Exception {
        executeShardBulkOnReplica(request, replica, term,
            group.primary.getLastKnownGlobalCheckpoint(), group.primary.getMaxSeqNoOfUpdatesOrDeletes());
    }

    /**
     * Executes the delete request on the given replica shard.
     */
    void deleteOnReplica(BulkShardRequest request, ReplicationGroup group, IndexShard replica) throws Exception {
        executeShardBulkOnReplica(request, replica, group.primary.getPendingPrimaryTerm(),
            group.primary.getLastKnownGlobalCheckpoint(), group.primary.getMaxSeqNoOfUpdatesOrDeletes());
    }

    class GlobalCheckpointSync extends ReplicationAction<
            GlobalCheckpointSyncAction.Request,
            GlobalCheckpointSyncAction.Request,
            ReplicationResponse> {

        GlobalCheckpointSync(final ActionListener<ReplicationResponse> listener, final ReplicationGroup replicationGroup) {
            super(
                    new GlobalCheckpointSyncAction.Request(replicationGroup.getPrimary().shardId()),
                    listener,
                    replicationGroup,
                    "global_checkpoint_sync");
        }

        @Override
        protected void performOnPrimary(IndexShard primary, GlobalCheckpointSyncAction.Request request,
                ActionListener<PrimaryResult> listener) {
            ActionListener.completeWith(listener, () -> {
                primary.sync();
                return new PrimaryResult(request, new ReplicationResponse());
            });
        }

        @Override
        protected void performOnReplica(final GlobalCheckpointSyncAction.Request request, final IndexShard replica) throws IOException {
            replica.sync();
        }
    }

    class ResyncAction extends ReplicationAction<ResyncReplicationRequest, ResyncReplicationRequest, ResyncReplicationResponse> {

        ResyncAction(ResyncReplicationRequest request, ActionListener<ResyncReplicationResponse> listener, ReplicationGroup group) {
            super(request, listener, group, "resync");
        }

        @Override
        protected void performOnPrimary(IndexShard primary, ResyncReplicationRequest request, ActionListener<PrimaryResult> listener) {
            ActionListener.completeWith(listener, () -> {
                final TransportWriteAction.WritePrimaryResult<ResyncReplicationRequest, ResyncReplicationResponse> result =
                    executeResyncOnPrimary(primary, request);
                return new PrimaryResult(result.replicaRequest(), result.finalResponseIfSuccessful);
            });
        }

        @Override
        protected void performOnReplica(ResyncReplicationRequest request, IndexShard replica) throws Exception {
            executeResyncOnReplica(replica, request, getPrimaryShard().getPendingPrimaryTerm(),
                getPrimaryShard().getLastKnownGlobalCheckpoint(), getPrimaryShard().getMaxSeqNoOfUpdatesOrDeletes());
        }
    }

    private TransportWriteAction.WritePrimaryResult<ResyncReplicationRequest, ResyncReplicationResponse> executeResyncOnPrimary(
        IndexShard primary, ResyncReplicationRequest request) {
        final TransportWriteAction.WritePrimaryResult<ResyncReplicationRequest, ResyncReplicationResponse> result =
            new TransportWriteAction.WritePrimaryResult<>(TransportResyncReplicationAction.performOnPrimary(request),
                new ResyncReplicationResponse(), null, null, primary, logger);
        TransportWriteActionTestHelper.performPostWriteActions(primary, request, result.location, logger);
        return result;
    }

    private void executeResyncOnReplica(IndexShard replica, ResyncReplicationRequest request, long operationPrimaryTerm,
                                        long globalCheckpointOnPrimary, long maxSeqNoOfUpdatesOrDeletes) throws Exception {
        final Translog.Location location;
        final PlainActionFuture<Releasable> acquirePermitFuture = new PlainActionFuture<>();
        replica.acquireReplicaOperationPermit(operationPrimaryTerm, globalCheckpointOnPrimary,
            maxSeqNoOfUpdatesOrDeletes, acquirePermitFuture, ThreadPool.Names.SAME, request);
        try (Releasable ignored = acquirePermitFuture.actionGet()) {
            location = TransportResyncReplicationAction.performOnReplica(request, replica);
        }
        TransportWriteActionTestHelper.performPostWriteActions(replica, request, location, logger);
    }

    class SyncRetentionLeases extends ReplicationAction<
        RetentionLeaseSyncAction.Request, RetentionLeaseSyncAction.Request, RetentionLeaseSyncAction.Response> {

        SyncRetentionLeases(RetentionLeaseSyncAction.Request request, ReplicationGroup group,
                            ActionListener<RetentionLeaseSyncAction.Response> listener)  {
            super(request, listener, group, "sync-retention-leases");
        }

        @Override
        protected void performOnPrimary(IndexShard primary, RetentionLeaseSyncAction.Request request,
                ActionListener<PrimaryResult> listener) {
            ActionListener.completeWith(listener, () -> {
                primary.persistRetentionLeases();
                return new PrimaryResult(request, new RetentionLeaseSyncAction.Response());
            });
        }

        @Override
        protected void performOnReplica(RetentionLeaseSyncAction.Request request, IndexShard replica) throws Exception {
            replica.updateRetentionLeasesOnReplica(request.getRetentionLeases());
            replica.persistRetentionLeases();
        }
    }

}
