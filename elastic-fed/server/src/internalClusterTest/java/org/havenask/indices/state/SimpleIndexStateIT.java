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

package org.havenask.indices.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.cluster.state.ClusterStateResponse;
import org.havenask.action.admin.indices.create.CreateIndexResponse;
import org.havenask.action.admin.indices.open.OpenIndexResponse;
import org.havenask.action.support.ActiveShardCount;
import org.havenask.cluster.health.ClusterHealthStatus;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.routing.ShardRoutingState;
import org.havenask.common.settings.Settings;
import org.havenask.index.IndexNotFoundException;
import org.havenask.indices.IndexClosedException;
import org.havenask.test.HavenaskIntegTestCase;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@HavenaskIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SimpleIndexStateIT extends HavenaskIntegTestCase {
    private final Logger logger = LogManager.getLogger(SimpleIndexStateIT.class);

    public void testSimpleOpenClose() {
        logger.info("--> creating test index");
        createIndex("test");

        logger.info("--> waiting for green status");
        ensureGreen();

        NumShards numShards = getNumShards("test");

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(numShards.numPrimaries));
        assertEquals(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size()
            , numShards.totalNumShards);

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();

        logger.info("--> closing test index...");
        assertAcked(client().admin().indices().prepareClose("test"));

        stateResponse = client().admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), notNullValue());

        logger.info("--> trying to index into a closed index ...");
        try {
            client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
            fail();
        } catch (IndexClosedException e) {
            // all is well
        }

        logger.info("--> opening index...");
        OpenIndexResponse openIndexResponse = client().admin().indices().prepareOpen("test").get();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));

        logger.info("--> waiting for green status");
        ensureGreen();

        stateResponse = client().admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.OPEN));

        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(numShards.numPrimaries));
        assertEquals(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(),
            numShards.totalNumShards);

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
    }

    public void testFastCloseAfterCreateContinuesCreateAfterOpen() {
        logger.info("--> creating test index that cannot be allocated");
        client().admin().indices().prepareCreate("test").setWaitForActiveShards(ActiveShardCount.NONE).setSettings(Settings.builder()
                .put("index.routing.allocation.include.tag", "no_such_node").build()).get();

        ClusterHealthResponse health = client().admin().cluster().prepareHealth("test").setWaitForNodes(">=2").get();
        assertThat(health.isTimedOut(), equalTo(false));
        assertThat(health.getStatus(), equalTo(ClusterHealthStatus.RED));

        assertAcked(client().admin().indices().prepareClose("test").setWaitForActiveShards(ActiveShardCount.NONE));

        logger.info("--> updating test index settings to allow allocation");
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder()
                .put("index.routing.allocation.include.tag", "").build()).get();

        client().admin().indices().prepareOpen("test").get();

        logger.info("--> waiting for green status");
        ensureGreen();

        NumShards numShards = getNumShards("test");

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(numShards.numPrimaries));
        assertEquals(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(),
            numShards.totalNumShards);

        logger.info("--> indexing a simple document");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
    }

    public void testConsistencyAfterIndexCreationFailure() {
        logger.info("--> deleting test index....");
        try {
            client().admin().indices().prepareDelete("test").get();
        } catch (IndexNotFoundException ex) {
            // Ignore
        }

        logger.info("--> creating test index with invalid settings ");
        try {
            client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("number_of_shards", "bad")).get();
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [bad] for setting [index.number_of_shards]", ex.getMessage());
            // Expected
        }

        logger.info("--> creating test index with valid settings ");
        CreateIndexResponse response = client().admin().indices().prepareCreate("test")
            .setSettings(Settings.builder().put("number_of_shards", 1)).get();
        assertThat(response.isAcknowledged(), equalTo(true));
    }
}
