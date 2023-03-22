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

import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.routing.RecoverySource;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.routing.ShardRoutingHelper;
import org.havenask.cluster.routing.UnassignedInfo;
import org.havenask.common.settings.Settings;
import org.havenask.index.Index;
import org.havenask.index.IndexService;
import org.havenask.index.IndexSettings;
import org.havenask.index.seqno.RetentionLeaseSyncer;
import org.havenask.index.shard.IndexEventListener;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.IndexShardTestCase;
import org.havenask.index.shard.ShardId;
import org.havenask.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.havenask.indices.recovery.RecoveryState;
import org.havenask.test.HavenaskSingleNodeTestCase;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.havenask.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;

public class IndicesLifecycleListenerSingleNodeTests extends HavenaskSingleNodeTestCase {

    public void testStartDeleteIndexEventCallback() throws Throwable {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        ensureGreen();
        Index idx = resolveIndex("test");
        IndexMetadata metadata = indicesService.indexService(idx).getMetadata();
        ShardRouting shardRouting = indicesService.indexService(idx).getShard(0).routingEntry();
        final AtomicInteger counter = new AtomicInteger(1);
        IndexEventListener countingListener = new IndexEventListener() {

            @Override
            public void beforeIndexCreated(Index index, Settings indexSettings) {
                assertEquals("test", index.getName());
                assertEquals(1, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void afterIndexCreated(IndexService indexService) {
                assertEquals("test", indexService.index().getName());
                assertEquals(2, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void beforeIndexShardCreated(ShardId shardId, Settings indexSettings) {
                assertEquals(3, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void afterIndexShardCreated(IndexShard indexShard) {
                assertEquals(4, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void afterIndexShardStarted(IndexShard indexShard) {
                assertEquals(5, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
                assertEquals(DELETED, reason);
                assertEquals(6, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void beforeIndexShardDeleted(ShardId shardId, Settings indexSettings) {
                assertEquals(7, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
                assertEquals(8, counter.get());
                counter.incrementAndGet();
            }

            @Override
            public void afterIndexRemoved(Index index, IndexSettings indexSettings, IndexRemovalReason reason) {
                assertEquals(DELETED, reason);
                assertEquals(9, counter.get());
                counter.incrementAndGet();
            }

        };
        indicesService.removeIndex(idx, DELETED, "simon says");
        try {
            IndexService index = indicesService.createIndex(metadata, Arrays.asList(countingListener), false);
            assertEquals(3, counter.get());
            idx = index.index();
            ShardRouting newRouting = shardRouting;
            String nodeId = newRouting.currentNodeId();
            UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "boom");
            newRouting = newRouting.moveToUnassigned(unassignedInfo)
                .updateUnassigned(unassignedInfo, RecoverySource.EmptyStoreRecoverySource.INSTANCE);
            newRouting = ShardRoutingHelper.initialize(newRouting, nodeId);
            IndexShard shard = index.createShard(newRouting, s -> {}, RetentionLeaseSyncer.EMPTY);
            IndexShardTestCase.updateRoutingEntry(shard, newRouting);
            assertEquals(5, counter.get());
            final DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
                    emptyMap(), emptySet(), Version.CURRENT);
            shard.markAsRecovering("store", new RecoveryState(newRouting, localNode, null));
            IndexShardTestCase.recoverFromStore(shard);
            newRouting = ShardRoutingHelper.moveToStarted(newRouting);
            IndexShardTestCase.updateRoutingEntry(shard, newRouting);
            assertEquals(6, counter.get());
        } finally {
            indicesService.removeIndex(idx, DELETED, "simon says");
        }
        assertEquals(10, counter.get());
    }

}
