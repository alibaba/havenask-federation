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

package org.havenask.indices.recovery;

import org.havenask.action.DocWriteResponse;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.havenask.action.delete.DeleteResponse;
import org.havenask.action.index.IndexResponse;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.routing.allocation.command.MoveAllocationCommand;
import org.havenask.common.Priority;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.index.query.QueryBuilders;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.hamcrest.HavenaskAssertions;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST)
public class IndexPrimaryRelocationIT extends HavenaskIntegTestCase {

    private static final int RELOCATION_COUNT = 15;

    public void testPrimaryRelocationWhileIndexing() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(randomIntBetween(2, 3));
        client().admin().indices().prepareCreate("test")
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
            .addMapping("type", "field", "type=text")
            .get();
        ensureGreen("test");
        AtomicInteger numAutoGenDocs = new AtomicInteger();
        final AtomicBoolean finished = new AtomicBoolean(false);
        Thread indexingThread = new Thread() {
            @Override
            public void run() {
                while (finished.get() == false && numAutoGenDocs.get() < 10_000) {
                    IndexResponse indexResponse = client().prepareIndex("test", "type", "id").setSource("field", "value").get();
                    assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
                    DeleteResponse deleteResponse = client().prepareDelete("test", "type", "id").get();
                    assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
                    client().prepareIndex("test", "type").setSource("auto", true).get();
                    numAutoGenDocs.incrementAndGet();
                }
            }
        };
        indexingThread.start();

        ClusterState initialState = client().admin().cluster().prepareState().get().getState();
        DiscoveryNode[] dataNodes = initialState.getNodes().getDataNodes().values().toArray(DiscoveryNode.class);
        DiscoveryNode relocationSource = initialState.getNodes().getDataNodes().get(initialState.getRoutingTable()
            .shardRoutingTable("test", 0).primaryShard().currentNodeId());
        for (int i = 0; i < RELOCATION_COUNT; i++) {
            DiscoveryNode relocationTarget = randomFrom(dataNodes);
            while (relocationTarget.equals(relocationSource)) {
                relocationTarget = randomFrom(dataNodes);
            }
            logger.info("--> [iteration {}] relocating from {} to {} ", i, relocationSource.getName(), relocationTarget.getName());
            client().admin().cluster().prepareReroute()
                .add(new MoveAllocationCommand("test", 0, relocationSource.getId(), relocationTarget.getId()))
                .execute().actionGet();
            ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth()
                .setTimeout(TimeValue.timeValueSeconds(60))
                .setWaitForEvents(Priority.LANGUID).setWaitForNoRelocatingShards(true).execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                final String hotThreads = client().admin().cluster().prepareNodesHotThreads().setIgnoreIdleThreads(false).get().getNodes()
                    .stream().map(NodeHotThreads::getHotThreads).collect(Collectors.joining("\n"));
                final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
                logger.info("timed out for waiting for relocation iteration [{}] \ncluster state {} \nhot threads {}",
                    i, clusterState, hotThreads);
                finished.set(true);
                indexingThread.join();
                throw new AssertionError("timed out waiting for relocation iteration [" + i + "] ");
            }
            logger.info("--> [iteration {}] relocation complete", i);
            relocationSource = relocationTarget;
            // indexing process aborted early, no need for more relocations as test has already failed
            if (indexingThread.isAlive() == false) {
                break;
            }
            if (i > 0  && i % 5 == 0) {
                logger.info("--> [iteration {}] flushing index", i);
                client().admin().indices().prepareFlush("test").get();
            }
        }
        finished.set(true);
        indexingThread.join();
        refresh("test");
        HavenaskAssertions.assertHitCount(client().prepareSearch("test").setTrackTotalHits(true).get(), numAutoGenDocs.get());
        HavenaskAssertions.assertHitCount(client().prepareSearch("test").setTrackTotalHits(true)// extra paranoia ;)
            .setQuery(QueryBuilders.termQuery("auto", true)).get(), numAutoGenDocs.get());
    }

}
