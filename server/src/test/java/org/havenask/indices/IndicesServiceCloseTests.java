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

package org.havenask.indices;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.routing.allocation.DiskThresholdSettings;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.cache.RemovalNotification;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.concurrent.HavenaskExecutors;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.index.IndexModule;
import org.havenask.index.IndexService;
import org.havenask.index.engine.Engine;
import org.havenask.index.shard.IndexShard;
import org.havenask.indices.IndicesRequestCache.Key;
import org.havenask.indices.breaker.HierarchyCircuitBreakerService;
import org.havenask.node.MockNode;
import org.havenask.node.Node;
import org.havenask.node.NodeValidationException;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.InternalSettingsPlugin;
import org.havenask.test.InternalTestCluster;
import org.havenask.test.MockHttpTransport;
import org.havenask.test.hamcrest.HavenaskAssertions;
import org.havenask.transport.nio.MockNioTransportPlugin;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.havenask.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.havenask.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.havenask.test.NodeRoles.dataNode;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;

public class IndicesServiceCloseTests extends HavenaskTestCase {

    private Node startNode() throws NodeValidationException {
        final Path tempDir = createTempDir();
        String nodeName = "node_s_0";
        Settings settings = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), InternalTestCluster.clusterName("single-node-cluster", random().nextLong()))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo"))
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), createTempDir().getParent())
            .put(Node.NODE_NAME_SETTING.getKey(), nodeName)
            .put(HavenaskExecutors.NODE_PROCESSORS_SETTING.getKey(), 1) // limit the number of threads created
            .put("transport.type", getTestTransportType())
            .put(dataNode())
            .put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), random().nextLong())
            // default the watermarks low values to prevent tests from failing on nodes without enough disk space
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b")
            // turning on the real memory circuit breaker leads to spurious test failures. As have no full control over heap usage, we
            // turn it off for these tests.
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()) // empty list disables a port scan for other nodes
            .putList(INITIAL_MASTER_NODES_SETTING.getKey(), nodeName)
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .build();

        Node node = new MockNode(settings,
                Arrays.asList(MockNioTransportPlugin.class, MockHttpTransport.TestPlugin.class, InternalSettingsPlugin.class), true);
        node.start();
        return node;
    }

    public void testCloseEmptyIndicesService() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());
        assertFalse(indicesService.awaitClose(0, TimeUnit.MILLISECONDS));
        node.close();
        assertEquals(0, indicesService.indicesRefCount.refCount());
        assertTrue(indicesService.awaitClose(0, TimeUnit.MILLISECONDS));
    }

    public void testCloseNonEmptyIndicesService() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());

        assertAcked(node.client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));

        assertEquals(2, indicesService.indicesRefCount.refCount());
        assertFalse(indicesService.awaitClose(0, TimeUnit.MILLISECONDS));

        node.close();
        assertEquals(0, indicesService.indicesRefCount.refCount());
        assertTrue(indicesService.awaitClose(0, TimeUnit.MILLISECONDS));
    }

    public void testCloseWithIncedRefStore() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());

        assertAcked(node.client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));

        assertEquals(2, indicesService.indicesRefCount.refCount());

        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        shard.store().incRef();
        assertFalse(indicesService.awaitClose(0, TimeUnit.MILLISECONDS));

        node.close();
        assertEquals(1, indicesService.indicesRefCount.refCount());
        assertFalse(indicesService.awaitClose(0, TimeUnit.MILLISECONDS));

        shard.store().decRef();
        assertEquals(0, indicesService.indicesRefCount.refCount());
        assertTrue(indicesService.awaitClose(0, TimeUnit.MILLISECONDS));
    }

    public void testCloseWhileOngoingRequest() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());

        assertAcked(node.client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        node.client().prepareIndex("test", "_doc", "1").setSource(Collections.emptyMap()).get();
        HavenaskAssertions.assertAllSuccessful(node.client().admin().indices().prepareRefresh("test").get());

        assertEquals(2, indicesService.indicesRefCount.refCount());

        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        Engine.Searcher searcher = shard.acquireSearcher("test");
        assertEquals(1, searcher.getIndexReader().maxDoc());

        node.close();
        assertEquals(1, indicesService.indicesRefCount.refCount());

        searcher.close();
        assertEquals(0, indicesService.indicesRefCount.refCount());
    }

    public void testCloseAfterRequestHasUsedQueryCache() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());

        assertAcked(node.client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true)));
        node.client().prepareIndex("test", "_doc", "1").setSource(Collections.singletonMap("foo", 3L)).get();
        HavenaskAssertions.assertAllSuccessful(node.client().admin().indices().prepareRefresh("test").get());

        assertEquals(2, indicesService.indicesRefCount.refCount());

        IndicesQueryCache cache = indicesService.getIndicesQueryCache();

        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        Engine.Searcher searcher = shard.acquireSearcher("test");
        assertEquals(1, searcher.getIndexReader().maxDoc());

        Query query = LongPoint.newRangeQuery("foo", 0, 5);
        assertEquals(0L, cache.getStats(shard.shardId()).getCacheSize());
        searcher.count(query);
        assertEquals(1L, cache.getStats(shard.shardId()).getCacheSize());

        searcher.close();
        assertEquals(2, indicesService.indicesRefCount.refCount());
        assertEquals(1L, cache.getStats(shard.shardId()).getCacheSize());

        node.close();
        assertEquals(0, indicesService.indicesRefCount.refCount());
        assertEquals(0L, cache.getStats(shard.shardId()).getCacheSize());
    }

    public void testCloseWhileOngoingRequestUsesQueryCache() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());

        assertAcked(node.client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true)));
        node.client().prepareIndex("test", "_doc", "1").setSource(Collections.singletonMap("foo", 3L)).get();
        HavenaskAssertions.assertAllSuccessful(node.client().admin().indices().prepareRefresh("test").get());

        assertEquals(2, indicesService.indicesRefCount.refCount());

        IndicesQueryCache cache = indicesService.getIndicesQueryCache();

        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        Engine.Searcher searcher = shard.acquireSearcher("test");
        assertEquals(1, searcher.getIndexReader().maxDoc());

        node.close();
        assertEquals(1, indicesService.indicesRefCount.refCount());

        Query query = LongPoint.newRangeQuery("foo", 0, 5);
        assertEquals(0L, cache.getStats(shard.shardId()).getCacheSize());
        searcher.count(query);
        assertEquals(1L, cache.getStats(shard.shardId()).getCacheSize());

        searcher.close();
        assertEquals(0, indicesService.indicesRefCount.refCount());
        assertEquals(0L, cache.getStats(shard.shardId()).getCacheSize());
    }

    public void testCloseWhileOngoingRequestUsesRequestCache() throws Exception {
        Node node = startNode();
        IndicesService indicesService = node.injector().getInstance(IndicesService.class);
        assertEquals(1, indicesService.indicesRefCount.refCount());

        assertAcked(node.client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true)));
        node.client().prepareIndex("test", "_doc", "1").setSource(Collections.singletonMap("foo", 3L)).get();
        HavenaskAssertions.assertAllSuccessful(node.client().admin().indices().prepareRefresh("test").get());

        assertEquals(2, indicesService.indicesRefCount.refCount());

        IndicesRequestCache cache = indicesService.indicesRequestCache;

        IndexService indexService = indicesService.iterator().next();
        IndexShard shard = indexService.getShard(0);
        Engine.Searcher searcher = shard.acquireSearcher("test");
        assertEquals(1, searcher.getIndexReader().maxDoc());

        node.close();
        assertEquals(1, indicesService.indicesRefCount.refCount());

        assertEquals(0L, cache.count());
        IndicesRequestCache.CacheEntity cacheEntity = new IndicesRequestCache.CacheEntity() {
            @Override
            public long ramBytesUsed() {
                return 42;
            }

            @Override
            public void onCached(Key key, BytesReference value) {}

            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public Object getCacheIdentity() {
                return this;
            }

            @Override
            public void onHit() {}

            @Override
            public void onMiss() {}

            @Override
            public void onRemoval(RemovalNotification<Key, BytesReference> notification) {}
        };
        cache.getOrCompute(cacheEntity, () -> new BytesArray("bar"), searcher.getDirectoryReader(), new BytesArray("foo"));
        assertEquals(1L, cache.count());

        searcher.close();
        assertEquals(0, indicesService.indicesRefCount.refCount());
        assertEquals(0L, cache.count());
    }
}
