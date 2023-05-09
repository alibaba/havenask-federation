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

import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.client.Client;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.HavenaskIntegTestCase.ClusterScope;
import org.havenask.test.HavenaskIntegTestCase.Scope;
import org.havenask.test.InternalTestCluster.RestartCallback;

import java.util.concurrent.TimeUnit;

import static org.havenask.client.Requests.clusterHealthRequest;
import static org.havenask.common.xcontent.XContentFactory.jsonBuilder;
import static org.havenask.index.query.QueryBuilders.matchAllQuery;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertHitCount;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertNoFailures;

@ClusterScope(numDataNodes = 0, scope = Scope.TEST)
public class QuorumGatewayIT extends HavenaskIntegTestCase {

    @Override
    protected int numberOfReplicas() {
        return 2;
    }

    public void testQuorumRecovery() throws Exception {
        logger.info("--> starting 3 nodes");
        // we are shutting down nodes - make sure we don't have 2 clusters if we test network
        internalCluster().startNodes(3);

        createIndex("test");
        ensureGreen();
        final NumShards test = getNumShards("test");

        logger.info("--> indexing...");
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();
        //We don't check for failures in the flush response: if we do we might get the following:
        // FlushNotAllowedEngineException[[test][1] recovery is in progress, flush [COMMIT_TRANSLOG] is not allowed]
        flush();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).get();
        refresh();

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 2L);
        }
        logger.info("--> restart all nodes");
        internalCluster().fullRestart(new RestartCallback() {
            @Override
            public void doAfterNodes(int numNodes, final Client activeClient) throws Exception {
                if (numNodes == 1) {
                    assertBusy(() -> {
                        logger.info("--> running cluster_health (wait for the shards to startup)");
                        ClusterHealthResponse clusterHealth = activeClient.admin().cluster().health(clusterHealthRequest()
                            .waitForYellowStatus().waitForNodes("2").waitForActiveShards(test.numPrimaries * 2)).actionGet();
                        logger.info("--> done cluster_health, status {}", clusterHealth.getStatus());
                        assertFalse(clusterHealth.isTimedOut());
                        assertEquals(ClusterHealthStatus.YELLOW, clusterHealth.getStatus());
                    }, 30, TimeUnit.SECONDS);

                    logger.info("--> one node is closed -- index 1 document into the remaining nodes");
                    activeClient.prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject().field("field", "value3")
                        .endObject()).get();
                    assertNoFailures(activeClient.admin().indices().prepareRefresh().get());
                    for (int i = 0; i < 10; i++) {
                        assertHitCount(activeClient.prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 3L);
                    }
                }
            }

        });
        logger.info("--> all nodes are started back, verifying we got the latest version");
        logger.info("--> running cluster_health (wait for the shards to startup)");
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 3L);
        }
    }
}
