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

package org.havenask.indices.flush;

import org.apache.lucene.index.Term;
import org.havenask.action.ActionListener;
import org.havenask.action.admin.indices.flush.FlushRequest;
import org.havenask.action.admin.indices.flush.FlushResponse;
import org.havenask.action.admin.indices.flush.SyncedFlushResponse;
import org.havenask.action.admin.indices.stats.IndexStats;
import org.havenask.action.admin.indices.stats.ShardStats;
import org.havenask.action.support.ActiveShardCount;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.allocation.command.MoveAllocationCommand;
import org.havenask.common.UUIDs;
import org.havenask.common.ValidationException;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.ByteSizeUnit;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.Index;
import org.havenask.index.IndexService;
import org.havenask.index.IndexSettings;
import org.havenask.index.engine.Engine;
import org.havenask.index.engine.InternalEngine;
import org.havenask.index.engine.InternalEngineTests;
import org.havenask.index.mapper.ParsedDocument;
import org.havenask.index.mapper.Uid;
import org.havenask.index.seqno.SequenceNumbers;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.IndexShardTestCase;
import org.havenask.index.shard.ShardId;
import org.havenask.indices.IndexingMemoryController;
import org.havenask.indices.IndicesService;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.InternalSettingsPlugin;
import org.havenask.test.InternalTestCluster;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST)
public class FlushIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(InternalSettingsPlugin.class);
    }

    public void testWaitIfOngoing() throws InterruptedException {
        createIndex("test");
        ensureGreen("test");
        final int numIters = scaledRandomIntBetween(10, 30);
        for (int i = 0; i < numIters; i++) {
            for (int j = 0; j < 10; j++) {
                client().prepareIndex("test", "test").setSource("{}", XContentType.JSON).get();
            }
            final CountDownLatch latch = new CountDownLatch(10);
            final CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
            for (int j = 0; j < 10; j++) {
                client().admin().indices().prepareFlush("test").execute(new ActionListener<FlushResponse>() {
                    @Override
                    public void onResponse(FlushResponse flushResponse) {
                        try {
                            // don't use assertAllSuccessful it uses a randomized context that belongs to a different thread
                            assertThat("Unexpected ShardFailures: " + Arrays.toString(flushResponse.getShardFailures()),
                                flushResponse.getFailedShards(), equalTo(0));
                            latch.countDown();
                        } catch (Exception ex) {
                            onFailure(ex);
                        }

                    }

                    @Override
                    public void onFailure(Exception e) {
                        errors.add(e);
                        latch.countDown();
                    }
                });
            }
            latch.await();
            assertThat(errors, emptyIterable());
        }
    }

    public void testRejectIllegalFlushParameters() {
        createIndex("test");
        int numDocs = randomIntBetween(0, 10);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "_doc").setSource("{}", XContentType.JSON).get();
        }
        assertThat(expectThrows(ValidationException.class,
            () -> client().admin().indices().flush(new FlushRequest().force(true).waitIfOngoing(false)).actionGet()).getMessage(),
            containsString("wait_if_ongoing must be true for a force flush"));
        assertThat(client().admin().indices().flush(new FlushRequest().force(true).waitIfOngoing(true)).actionGet()
            .getShardFailures(), emptyArray());
        assertThat(client().admin().indices().flush(new FlushRequest().force(false).waitIfOngoing(randomBoolean()))
            .actionGet().getShardFailures(), emptyArray());
    }

    public void testSyncedFlush() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)).get();
        ensureGreen();

        final Index index = client().admin().cluster().prepareState().get().getState().metadata().index("test").getIndex();

        IndexStats indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }

        ShardsSyncedFlushResult result;
        if (randomBoolean()) {
            logger.info("--> sync flushing shard 0");
            result = SyncedFlushUtil.attemptSyncedFlush(logger, internalCluster(), new ShardId(index, 0));
        } else {
            logger.info("--> sync flushing index [test]");
            SyncedFlushResponse indicesResult = client().admin().indices().prepareSyncedFlush("test").get();
            result = indicesResult.getShardsResultPerIndex().get("test").get(0);
        }
        assertFalse(result.failed());
        assertThat(result.totalShards(), equalTo(indexStats.getShards().length));
        assertThat(result.successfulShards(), equalTo(indexStats.getShards().length));

        indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        String syncId = result.syncId();
        for (ShardStats shardStats : indexStats.getShards()) {
            final String shardSyncId = shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID);
            assertThat(shardSyncId, equalTo(syncId));
        }

        // now, start new node and relocate a shard there and see if sync id still there
        String newNodeName = internalCluster().startNode();
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        ShardRouting shardRouting = clusterState.getRoutingTable().index("test").shard(0).iterator().next();
        String currentNodeName = clusterState.nodes().resolveNode(shardRouting.currentNodeId()).getName();
        assertFalse(currentNodeName.equals(newNodeName));
        internalCluster().client().admin().cluster().prepareReroute()
            .add(new MoveAllocationCommand("test", 0, currentNodeName, newNodeName)).get();

        client().admin().cluster().prepareHealth()
                .setWaitForNoRelocatingShards(true)
                .get();
        indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNotNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }

        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()).get();
        ensureGreen("test");
        indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNotNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, internalCluster().numDataNodes() - 1).build()).get();
        ensureGreen("test");
        indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNotNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }
    }

    public void testSyncedFlushWithConcurrentIndexing() throws Exception {

        internalCluster().ensureAtLeastNumDataNodes(3);
        createIndex("test");

        client().admin().indices().prepareUpdateSettings("test").setSettings(
                Settings.builder()
                    .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.PB))
                    .put("index.refresh_interval", -1)
                    .put("index.number_of_replicas", internalCluster().numDataNodes() - 1))
                .get();
        ensureGreen();
        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicInteger numDocs = new AtomicInteger(0);
        Thread indexingThread = new Thread() {
            @Override
            public void run() {
                while (stop.get() == false) {
                    client().prepareIndex().setIndex("test").setType("_doc").setSource("{}", XContentType.JSON).get();
                    numDocs.incrementAndGet();
                }
            }
        };
        indexingThread.start();

        IndexStats indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }
        logger.info("--> trying sync flush");
        SyncedFlushResponse syncedFlushResult = client().admin().indices().prepareSyncedFlush("test").get();
        logger.info("--> sync flush done");
        stop.set(true);
        indexingThread.join();
        indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        assertFlushResponseEqualsShardStats(indexStats.getShards(), syncedFlushResult.getShardsResultPerIndex().get("test"));
        refresh();
        assertThat(client().prepareSearch().setSize(0).get().getHits().getTotalHits().value, equalTo((long) numDocs.get()));
        logger.info("indexed {} docs", client().prepareSearch().setSize(0).get().getHits().getTotalHits().value);
        logClusterState();
        internalCluster().fullRestart();
        ensureGreen();
        assertThat(client().prepareSearch().setSize(0).get().getHits().getTotalHits().value, equalTo((long) numDocs.get()));
    }

    private void assertFlushResponseEqualsShardStats(ShardStats[] shardsStats, List<ShardsSyncedFlushResult> syncedFlushResults) {

        for (final ShardStats shardStats : shardsStats) {
            for (final ShardsSyncedFlushResult shardResult : syncedFlushResults) {
                if (shardStats.getShardRouting().getId() == shardResult.shardId().getId()) {
                    for (Map.Entry<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> singleResponse :
                        shardResult.shardResponses().entrySet()) {
                        if (singleResponse.getKey().currentNodeId().equals(shardStats.getShardRouting().currentNodeId())) {
                            if (singleResponse.getValue().success()) {
                                logger.info("{} sync flushed on node {}", singleResponse.getKey().shardId(),
                                    singleResponse.getKey().currentNodeId());
                                assertNotNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
                            } else {
                                logger.info("{} sync flush failed for on node {}", singleResponse.getKey().shardId(),
                                    singleResponse.getKey().currentNodeId());
                                assertNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
                            }
                        }
                    }
                }
            }
        }
    }

    public void testUnallocatedShardsDoesNotHang() throws InterruptedException {
        //  create an index but disallow allocation
        prepareCreate("test").setWaitForActiveShards(ActiveShardCount.NONE).setSettings(Settings.builder()
            .put("index.routing.allocation.include._name", "nonexistent")).get();

        // this should not hang but instead immediately return with empty result set
        List<ShardsSyncedFlushResult> shardsResult = client().admin().indices().prepareSyncedFlush("test").get()
            .getShardsResultPerIndex().get("test");
        // just to make sure the test actually tests the right thing
        int numShards = client().admin().indices().prepareGetSettings("test").get().getIndexToSettings().get("test")
            .getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, -1);
        assertThat(shardsResult.size(), equalTo(numShards));
        assertThat(shardsResult.get(0).failureReason(), equalTo("no active shards"));
    }

    private void indexDoc(Engine engine, String id) throws IOException {
        final ParsedDocument doc = InternalEngineTests.createParsedDoc(id, null);
        final Engine.IndexResult indexResult = engine.index(new Engine.Index(new Term("_id", Uid.encodeId(doc.id())), doc,
            ((InternalEngine) engine).getProcessedLocalCheckpoint() + 1, 1L, 1L, null, Engine.Operation.Origin.REPLICA, System.nanoTime(),
            -1L, false, SequenceNumbers.UNASSIGNED_SEQ_NO, 0));
        assertThat(indexResult.getFailure(), nullValue());
        engine.syncTranslog();
    }

    public void testSyncedFlushSkipOutOfSyncReplicas() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(between(2, 3));
        final int numberOfReplicas = internalCluster().numDataNodes() - 1;
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)).get()
        );
        ensureGreen();
        final Index index = clusterService().state().metadata().index("test").getIndex();
        final ShardId shardId = new ShardId(index, 0);
        final int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            index("test", "doc", Integer.toString(i));
        }
        final List<IndexShard> indexShards = internalCluster().nodesInclude("test").stream()
            .map(node -> internalCluster().getInstance(IndicesService.class, node).getShardOrNull(shardId))
            .collect(Collectors.toList());
        // Index extra documents to one replica - synced-flush should fail on that replica.
        final IndexShard outOfSyncReplica = randomValueOtherThanMany(s -> s.routingEntry().primary(), () -> randomFrom(indexShards));
        final int extraDocs = between(1, 10);
        for (int i = 0; i < extraDocs; i++) {
            indexDoc(IndexShardTestCase.getEngine(outOfSyncReplica), "extra_" + i);
        }
        final ShardsSyncedFlushResult partialResult = SyncedFlushUtil.attemptSyncedFlush(logger, internalCluster(), shardId);
        assertThat(partialResult.totalShards(), equalTo(numberOfReplicas + 1));
        assertThat(partialResult.successfulShards(), equalTo(numberOfReplicas));
        assertThat(partialResult.shardResponses().get(outOfSyncReplica.routingEntry()).failureReason, equalTo(
            "ongoing indexing operations: num docs on replica [" + (numDocs + extraDocs) + "]; num docs on primary [" + numDocs + "]"));
        // Index extra documents to all shards - synced-flush should be ok.
        for (IndexShard indexShard : indexShards) {
            // Do reindex documents to the out of sync replica to avoid trigger merges
            if (indexShard != outOfSyncReplica) {
                for (int i = 0; i < extraDocs; i++) {
                    indexDoc(IndexShardTestCase.getEngine(indexShard), "extra_" + i);
                }
            }
        }
        final ShardsSyncedFlushResult fullResult = SyncedFlushUtil.attemptSyncedFlush(logger, internalCluster(), shardId);
        assertThat(fullResult.totalShards(), equalTo(numberOfReplicas + 1));
        assertThat(fullResult.successfulShards(), equalTo(numberOfReplicas + 1));
    }

    public void testDoNotRenewSyncedFlushWhenAllSealed() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(between(2, 3));
        final int numberOfReplicas = internalCluster().numDataNodes() - 1;
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)).get()
        );
        ensureGreen();
        final Index index = clusterService().state().metadata().index("test").getIndex();
        final ShardId shardId = new ShardId(index, 0);
        final int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            index("test", "doc", Integer.toString(i));
        }
        final ShardsSyncedFlushResult firstSeal = SyncedFlushUtil.attemptSyncedFlush(logger, internalCluster(), shardId);
        assertThat(firstSeal.successfulShards(), equalTo(numberOfReplicas + 1));
        // Do not renew synced-flush
        final ShardsSyncedFlushResult secondSeal = SyncedFlushUtil.attemptSyncedFlush(logger, internalCluster(), shardId);
        assertThat(secondSeal.successfulShards(), equalTo(numberOfReplicas + 1));
        assertThat(secondSeal.syncId(), equalTo(firstSeal.syncId()));
        // Shards were updated, renew synced flush.
        final int moreDocs = between(1, 10);
        for (int i = 0; i < moreDocs; i++) {
            index("test", "doc", "more-" + i);
        }
        final ShardsSyncedFlushResult thirdSeal = SyncedFlushUtil.attemptSyncedFlush(logger, internalCluster(), shardId);
        assertThat(thirdSeal.successfulShards(), equalTo(numberOfReplicas + 1));
        assertThat(thirdSeal.syncId(), not(equalTo(firstSeal.syncId())));
        // Manually remove or change sync-id, renew synced flush.
        IndexShard shard = internalCluster().getInstance(IndicesService.class, randomFrom(internalCluster().nodesInclude("test")))
            .getShardOrNull(shardId);
        if (randomBoolean()) {
            // Change the existing sync-id of a single shard.
            shard.syncFlush(UUIDs.randomBase64UUID(random()), shard.commitStats().getRawCommitId());
            assertThat(shard.commitStats().syncId(), not(equalTo(thirdSeal.syncId())));
        } else {
            // Flush will create a new commit without sync-id
            shard.flush(new FlushRequest(shardId.getIndexName()).force(true).waitIfOngoing(true));
            assertThat(shard.commitStats().syncId(), nullValue());
        }
        final ShardsSyncedFlushResult forthSeal = SyncedFlushUtil.attemptSyncedFlush(logger, internalCluster(), shardId);
        assertThat(forthSeal.successfulShards(), equalTo(numberOfReplicas + 1));
        assertThat(forthSeal.syncId(), not(equalTo(thirdSeal.syncId())));
    }

    public void testFlushOnInactive() throws Exception {
        final String indexName = "flush_on_inactive";
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2, Settings.builder()
            .put(IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.getKey(), randomTimeValue(10, 1000, "ms")).build());
        assertAcked(client().admin().indices().prepareCreate(indexName).setSettings(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), randomTimeValue(200, 500, "ms"))
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), randomTimeValue(50, 200, "ms"))
            .put("index.routing.allocation.include._name", String.join(",", dataNodes))
            .build()));
        ensureGreen(indexName);
        int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(indexName, "_doc").setSource("f", "v").get();
        }
        if (randomBoolean()) {
            internalCluster().restartNode(randomFrom(dataNodes), new InternalTestCluster.RestartCallback());
            ensureGreen(indexName);
        }
        assertBusy(() -> {
            for (ShardStats shardStats : client().admin().indices().prepareStats(indexName).get().getShards()) {
                assertThat(shardStats.getStats().getTranslog().getUncommittedOperations(), equalTo(0));
            }
        }, 30, TimeUnit.SECONDS);
    }
}
