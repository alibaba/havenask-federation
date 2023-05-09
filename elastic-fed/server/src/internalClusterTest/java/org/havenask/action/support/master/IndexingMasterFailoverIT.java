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

package org.havenask.action.support.master;

import org.havenask.action.DocWriteResponse;
import org.havenask.action.index.IndexResponse;
import org.havenask.common.settings.Settings;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.disruption.NetworkDisruption;
import org.havenask.test.transport.MockTransportService;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.equalTo;

@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class IndexingMasterFailoverIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final HashSet<Class<? extends Plugin>> classes = new HashSet<>(super.nodePlugins());
        classes.add(MockTransportService.TestPlugin.class);
        return classes;
    }

    /**
     * Indexing operations which entail mapping changes require a blocking request to the master node to update the mapping.
     * If the master node is being disrupted or if it cannot commit cluster state changes, it needs to retry within timeout limits.
     * This retry logic is implemented in TransportMasterNodeAction and tested by the following master failover scenario.
     */
    public void testMasterFailoverDuringIndexingWithMappingChanges() throws Throwable {
        logger.info("--> start 4 nodes, 3 master, 1 data");

        internalCluster().setBootstrapMasterNodeIndex(2);

        internalCluster().startMasterOnlyNodes(3, Settings.EMPTY);

        String dataNode = internalCluster().startDataOnlyNode(Settings.EMPTY);

        logger.info("--> wait for all nodes to join the cluster");
        ensureStableCluster(4);

        // We index data with mapping changes into cluster and have master failover at same time
        client().admin().indices().prepareCreate("myindex")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .get();
        ensureGreen("myindex");

        final CyclicBarrier barrier = new CyclicBarrier(2);

        Thread indexingThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                } catch (InterruptedException e) {
                    logger.warn("Barrier interrupted", e);
                    return;
                } catch (BrokenBarrierException e) {
                    logger.warn("Broken barrier", e);
                    return;
                }
                for (int i = 0; i < 10; i++) {
                    // index data with mapping changes
                    IndexResponse response = client(dataNode).prepareIndex("myindex", "mytype").setSource("field_" + i, "val").get();
                    assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
                }
            }
        });
        indexingThread.setName("indexingThread");
        indexingThread.start();

        barrier.await();

        // interrupt communication between master and other nodes in cluster
        NetworkDisruption partition = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(partition);

        logger.info("--> disrupting network");
        partition.startDisrupting();

        logger.info("--> waiting for new master to be elected");
        ensureStableCluster(3, dataNode);

        partition.stopDisrupting();
        logger.info("--> waiting to heal");
        ensureStableCluster(4);

        indexingThread.join();

        ensureGreen("myindex");
        refresh();
        assertThat(client().prepareSearch("myindex").get().getHits().getTotalHits().value, equalTo(10L));
    }

}
