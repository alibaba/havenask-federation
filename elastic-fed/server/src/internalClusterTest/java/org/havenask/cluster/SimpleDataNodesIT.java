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

package org.havenask.cluster;

import org.havenask.action.UnavailableShardsException;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.index.IndexResponse;
import org.havenask.action.support.ActiveShardCount;
import org.havenask.client.Requests;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.Priority;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.HavenaskIntegTestCase.ClusterScope;
import org.havenask.test.HavenaskIntegTestCase.Scope;

import static org.havenask.client.Requests.createIndexRequest;
import static org.havenask.common.unit.TimeValue.timeValueSeconds;
import static org.havenask.test.NodeRoles.dataNode;
import static org.havenask.test.NodeRoles.nonDataNode;

import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class SimpleDataNodesIT extends HavenaskIntegTestCase {

    private static final String SOURCE = "{\"type1\":{\"id\":\"1\",\"name\":\"test\"}}";

    public void testIndexingBeforeAndAfterDataNodesStart() {
        internalCluster().startNode(nonDataNode());
        client().admin().indices().create(createIndexRequest("test").waitForActiveShards(ActiveShardCount.NONE)).actionGet();
        try {
            client().index(Requests.indexRequest("test").id("1").source(SOURCE, XContentType.JSON)
                .timeout(timeValueSeconds(1))).actionGet();
            fail("no allocation should happen");
        } catch (UnavailableShardsException e) {
            // all is well
        }

        internalCluster().startNode(nonDataNode());
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2")
            .setLocal(true).execute().actionGet().isTimedOut(), equalTo(false));

        // still no shard should be allocated
        try {
            client().index(Requests.indexRequest("test").id("1").source(SOURCE, XContentType.JSON)
                .timeout(timeValueSeconds(1))).actionGet();
            fail("no allocation should happen");
        } catch (UnavailableShardsException e) {
            // all is well
        }

        // now, start a node data, and see that it gets with shards
        internalCluster().startNode(dataNode());
        assertThat(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("3")
            .setLocal(true).execute().actionGet().isTimedOut(), equalTo(false));

        IndexResponse indexResponse = client().index(Requests.indexRequest("test").id("1")
            .source(SOURCE, XContentType.JSON)).actionGet();
        assertThat(indexResponse.getId(), equalTo("1"));
    }

    public void testShardsAllocatedAfterDataNodesStart() {
        internalCluster().startNode(nonDataNode());
        client().admin().indices().create(createIndexRequest("test")
            .settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)).waitForActiveShards(ActiveShardCount.NONE))
            .actionGet();
        final ClusterHealthResponse healthResponse1 = client().admin().cluster().prepareHealth()
            .setWaitForEvents(Priority.LANGUID).execute().actionGet();
        assertThat(healthResponse1.isTimedOut(), equalTo(false));
        assertThat(healthResponse1.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse1.getActiveShards(), equalTo(0));

        internalCluster().startNode(dataNode());

        assertThat(client().admin().cluster().prepareHealth()
            .setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").setWaitForGreenStatus().execute().actionGet().isTimedOut(),
            equalTo(false));
    }

    public void testAutoExpandReplicasAdjustedWhenDataNodeJoins() {
        internalCluster().startNode(nonDataNode());
        client().admin().indices().create(createIndexRequest("test")
            .settings(Settings.builder().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all"))
            .waitForActiveShards(ActiveShardCount.NONE))
            .actionGet();
        final ClusterHealthResponse healthResponse1 = client().admin().cluster().prepareHealth()
            .setWaitForEvents(Priority.LANGUID).execute().actionGet();
        assertThat(healthResponse1.isTimedOut(), equalTo(false));
        assertThat(healthResponse1.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse1.getActiveShards(), equalTo(0));

        internalCluster().startNode();
        internalCluster().startNode();
        client().admin().cluster().prepareReroute().setRetryFailed(true).get();
    }

}
