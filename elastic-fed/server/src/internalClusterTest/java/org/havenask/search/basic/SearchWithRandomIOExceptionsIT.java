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

package org.havenask.search.basic;

import org.apache.lucene.util.English;

import org.havenask.HavenaskException;
import org.havenask.action.DocWriteResponse;
import org.havenask.action.admin.cluster.health.ClusterHealthResponse;
import org.havenask.action.admin.indices.refresh.RefreshResponse;
import org.havenask.action.index.IndexResponse;
import org.havenask.action.search.SearchPhaseExecutionException;
import org.havenask.action.search.SearchResponse;
import org.havenask.client.Requests;
import org.havenask.common.Strings;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.query.QueryBuilders;
import org.havenask.plugins.Plugin;
import org.havenask.search.sort.SortOrder;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.store.MockFSDirectoryFactory;
import org.havenask.test.store.MockFSIndexStore;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertHitCount;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertNoFailures;

public class SearchWithRandomIOExceptionsIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockFSIndexStore.TestPlugin.class);
    }

    public void testRandomDirectoryIOExceptions() throws IOException, InterruptedException, ExecutionException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().
            startObject().
            startObject("type").
            startObject("properties").
            startObject("test")
            .field("type", "keyword")
            .endObject().
                endObject().
                endObject()
            .endObject());
        final double exceptionRate;
        final double exceptionOnOpenRate;
        if (frequently()) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    exceptionOnOpenRate = 1.0 / between(5, 100);
                    exceptionRate = 0.0d;
                } else {
                    exceptionRate = 1.0 / between(5, 100);
                    exceptionOnOpenRate = 0.0d;
                }
            } else {
                exceptionOnOpenRate = 1.0 / between(5, 100);
                exceptionRate = 1.0 / between(5, 100);
            }
        } else {
            // rarely no exception
            exceptionRate = 0d;
            exceptionOnOpenRate = 0d;
        }
        final boolean createIndexWithoutErrors = randomBoolean();
        int numInitialDocs = 0;

        if (createIndexWithoutErrors) {
            Settings.Builder settings = Settings.builder()
                .put("index.number_of_replicas", numberOfReplicas());
            logger.info("creating index: [test] using settings: [{}]", settings.build());
            client().admin().indices().prepareCreate("test")
                .setSettings(settings)
                .addMapping("type", mapping, XContentType.JSON).get();
            numInitialDocs = between(10, 100);
            ensureGreen();
            for (int i = 0; i < numInitialDocs; i++) {
                client().prepareIndex("test", "type", "init" + i).setSource("test", "init").get();
            }
            client().admin().indices().prepareRefresh("test").execute().get();
            client().admin().indices().prepareFlush("test").execute().get();
            client().admin().indices().prepareClose("test").execute().get();
            client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder()
                .put(MockFSDirectoryFactory.RANDOM_IO_EXCEPTION_RATE_SETTING.getKey(), exceptionRate)
                .put(MockFSDirectoryFactory.RANDOM_IO_EXCEPTION_RATE_ON_OPEN_SETTING.getKey(), exceptionOnOpenRate));
            client().admin().indices().prepareOpen("test").execute().get();
        } else {
            Settings.Builder settings = Settings.builder()
                .put("index.number_of_replicas", randomIntBetween(0, 1))
                .put(MockFSIndexStore.INDEX_CHECK_INDEX_ON_CLOSE_SETTING.getKey(), false)
                .put(MockFSDirectoryFactory.RANDOM_IO_EXCEPTION_RATE_SETTING.getKey(), exceptionRate)
                // we cannot expect that the index will be valid
                .put(MockFSDirectoryFactory.RANDOM_IO_EXCEPTION_RATE_ON_OPEN_SETTING.getKey(), exceptionOnOpenRate);
            logger.info("creating index: [test] using settings: [{}]", settings.build());
            client().admin().indices().prepareCreate("test")
                .setSettings(settings)
                .addMapping("type", mapping, XContentType.JSON).get();
        }
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster()
            // it's OK to timeout here
            .health(Requests.clusterHealthRequest().waitForYellowStatus().timeout(TimeValue.timeValueSeconds(5))).get();
        final int numDocs;
        final boolean expectAllShardsFailed;
        if (clusterHealthResponse.isTimedOut()) {
            /* some seeds just won't let you create the index at all and we enter a ping-pong mode
             * trying one node after another etc. that is ok but we need to make sure we don't wait
             * forever when indexing documents so we set numDocs = 1 and expecte all shards to fail
             * when we search below.*/
            logger.info("ClusterHealth timed out - only index one doc and expect searches to fail");
            numDocs = 1;
            expectAllShardsFailed = true;
        } else {
            numDocs = between(10, 100);
            expectAllShardsFailed = false;
        }
        int numCreated = 0;
        boolean[] added = new boolean[numDocs];
        for (int i = 0; i < numDocs; i++) {
            added[i] = false;
            try {
                IndexResponse indexResponse = client().prepareIndex("test", "type", Integer.toString(i))
                        .setTimeout(TimeValue.timeValueSeconds(1)).setSource("test", English.intToEnglish(i)).get();
                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    numCreated++;
                    added[i] = true;
                }
            } catch (HavenaskException ex) {
            }

        }
        HavenaskIntegTestCase.NumShards numShards = getNumShards("test");
        logger.info("Start Refresh");
        // don't assert on failures here
        final RefreshResponse refreshResponse = client().admin().indices().prepareRefresh("test").execute().get();
        final boolean refreshFailed = refreshResponse.getShardFailures().length != 0 || refreshResponse.getFailedShards() != 0;
        logger.info("Refresh failed [{}] numShardsFailed: [{}], shardFailuresLength: [{}], successfulShards: [{}], totalShards: [{}] ",
                refreshFailed, refreshResponse.getFailedShards(), refreshResponse.getShardFailures().length,
                refreshResponse.getSuccessfulShards(), refreshResponse.getTotalShards());
        final int numSearches = scaledRandomIntBetween(10, 20);
        // we don't check anything here really just making sure we don't leave any open files or a broken index behind.
        for (int i = 0; i < numSearches; i++) {
            try {
                int docToQuery = between(0, numDocs - 1);
                int expectedResults = added[docToQuery] ? 1 : 0;
                logger.info("Searching for [test:{}]", English.intToEnglish(docToQuery));
                SearchResponse searchResponse = client().prepareSearch().setTypes("type")
                    .setQuery(QueryBuilders.matchQuery("test", English.intToEnglish(docToQuery)))
                    .setSize(expectedResults).get();
                logger.info("Successful shards: [{}]  numShards: [{}]", searchResponse.getSuccessfulShards(), numShards.numPrimaries);
                if (searchResponse.getSuccessfulShards() == numShards.numPrimaries && !refreshFailed) {
                    assertResultsAndLogOnFailure(expectedResults, searchResponse);
                }
                // check match all
                searchResponse = client().prepareSearch().setTypes("type").setQuery(QueryBuilders.matchAllQuery())
                    .setSize(numCreated + numInitialDocs).addSort("_uid", SortOrder.ASC).get();
                logger.info("Match all Successful shards: [{}]  numShards: [{}]", searchResponse.getSuccessfulShards(),
                        numShards.numPrimaries);
                if (searchResponse.getSuccessfulShards() == numShards.numPrimaries && !refreshFailed) {
                    assertResultsAndLogOnFailure(numCreated + numInitialDocs, searchResponse);
                }
            } catch (SearchPhaseExecutionException ex) {
                logger.info("SearchPhaseException: [{}]", ex.getMessage());
                // if a scheduled refresh or flush fails all shards we see all shards failed here
                if (!(expectAllShardsFailed || refreshResponse.getSuccessfulShards() == 0 ||
                        ex.getMessage().contains("all shards failed"))) {
                    throw ex;
                }
            }
        }

        if (createIndexWithoutErrors) {
            // check the index still contains the records that we indexed without errors
            client().admin().indices().prepareClose("test").execute().get();
            client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder()
                .put(MockFSDirectoryFactory.RANDOM_IO_EXCEPTION_RATE_SETTING.getKey(), 0)
                .put(MockFSDirectoryFactory.RANDOM_IO_EXCEPTION_RATE_ON_OPEN_SETTING.getKey(), 0));
            client().admin().indices().prepareOpen("test").execute().get();
            ensureGreen();
            SearchResponse searchResponse = client().prepareSearch().setTypes("type")
                    .setQuery(QueryBuilders.matchQuery("test", "init")).get();
            assertNoFailures(searchResponse);
            assertHitCount(searchResponse, numInitialDocs);
        }
    }
}
