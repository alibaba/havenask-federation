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

package org.havenask.indices.memory.breaker;

import org.havenask.ExceptionsHelper;
import org.havenask.action.admin.cluster.node.stats.NodeStats;
import org.havenask.action.admin.cluster.node.stats.NodesStatsResponse;
import org.havenask.action.bulk.BulkItemResponse;
import org.havenask.action.bulk.BulkRequest;
import org.havenask.action.bulk.BulkResponse;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.index.IndexRequestBuilder;
import org.havenask.action.search.SearchRequestBuilder;
import org.havenask.action.search.SearchResponse;
import org.havenask.client.Client;
import org.havenask.client.Requests;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.havenask.common.breaker.CircuitBreaker;
import org.havenask.common.breaker.CircuitBreakingException;
import org.havenask.common.breaker.NoopCircuitBreaker;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.ByteSizeUnit;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.xcontent.XContentType;
import org.havenask.indices.breaker.CircuitBreakerStats;
import org.havenask.indices.breaker.HierarchyCircuitBreakerService;
import org.havenask.rest.RestStatus;
import org.havenask.search.sort.SortOrder;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.HavenaskIntegTestCase.ClusterScope;

import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.havenask.action.admin.cluster.node.stats.NodesStatsRequest.Metric.BREAKER;
import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.havenask.index.query.QueryBuilders.matchAllQuery;
import static org.havenask.search.aggregations.AggregationBuilders.cardinality;
import static org.havenask.search.aggregations.AggregationBuilders.terms;
import static org.havenask.test.HavenaskIntegTestCase.Scope.TEST;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertFailures;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for InternalCircuitBreakerService
 */
@ClusterScope(scope = TEST, numClientNodes = 0, maxNumDataNodes = 1)
public class CircuitBreakerServiceIT extends HavenaskIntegTestCase {
    /** Reset all breaker settings back to their defaults */
    private void reset() {
        logger.info("--> resetting breaker settings");
        // clear all caches, we could be very close (or even above) the limit and then we will not be able to reset the breaker settings
        client().admin().indices().prepareClearCache().setFieldDataCache(true).setQueryCache(true).setRequestCache(true).get();

        Settings.Builder resetSettings = Settings.builder();
        Stream.of(
            HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING,
            HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING,
            HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            HierarchyCircuitBreakerService.ACCOUNTING_CIRCUIT_BREAKER_LIMIT_SETTING,
            HierarchyCircuitBreakerService.ACCOUNTING_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING,
            HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING,
            HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING).forEach(s -> resetSettings.putNull(s.getKey()));
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(resetSettings));
    }

    @Before
    public void setup() {
        reset();
    }

    @After
    public void teardown() {
        reset();
    }

    /** Returns true if any of the nodes used a noop breaker */
    private boolean noopBreakerUsed() {
        NodesStatsResponse stats = client().admin().cluster().prepareNodesStats()
            .addMetric(BREAKER.metricName())
            .get();
        for (NodeStats nodeStats : stats.getNodes()) {
            if (nodeStats.getBreaker().getStats(CircuitBreaker.REQUEST).getLimit() == NoopCircuitBreaker.LIMIT) {
                return true;
            }
            if (nodeStats.getBreaker().getStats(CircuitBreaker.IN_FLIGHT_REQUESTS).getLimit() == NoopCircuitBreaker.LIMIT) {
                return true;
            }
            if (nodeStats.getBreaker().getStats(CircuitBreaker.FIELDDATA).getLimit() == NoopCircuitBreaker.LIMIT) {
                return true;
            }
        }
        return false;
    }

    public void testMemoryBreaker() throws Exception {
        if (noopBreakerUsed()) {
            logger.info("--> noop breakers used, skipping test");
            return;
        }
        assertAcked(prepareCreate("cb-test", 1, Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, between(0, 1)))
                .addMapping("type", "test", "type=text,fielddata=true"));
        final Client client = client();

        // index some different terms so we have some field data for loading
        int docCount = scaledRandomIntBetween(300, 1000);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (long id = 0; id < docCount; id++) {
            reqs.add(client.prepareIndex("cb-test", "type", Long.toString(id)).setSource("test", "value" + id));
        }
        indexRandom(true, false, true, reqs);

        // clear field data cache (thus setting the loaded field data back to 0)
        clearFieldData();

        // Update circuit breaker settings
        Settings settings = Settings.builder()
                .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100b")
                .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.05)
                .build();
        assertAcked(client.admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

        // execute a search that loads field data (sorting on the "test" field)
        // again, this time it should trip the breaker
        SearchRequestBuilder searchRequest = client.prepareSearch("cb-test").setQuery(matchAllQuery()).addSort("test", SortOrder.DESC);

        String errMsg = "Data too large, data for [test] would be";
        assertFailures(searchRequest, RestStatus.INTERNAL_SERVER_ERROR, containsString(errMsg));
        errMsg = "which is larger than the limit of [100/100b]";
        assertFailures(searchRequest, RestStatus.INTERNAL_SERVER_ERROR, containsString(errMsg));

        NodesStatsResponse stats = client.admin().cluster().prepareNodesStats()
            .addMetric(BREAKER.metricName())
            .get();
        int breaks = 0;
        for (NodeStats stat : stats.getNodes()) {
            CircuitBreakerStats breakerStats = stat.getBreaker().getStats(CircuitBreaker.FIELDDATA);
            breaks += breakerStats.getTrippedCount();
        }
        assertThat(breaks, greaterThanOrEqualTo(1));
    }

    public void testRamAccountingTermsEnum() throws Exception {
        if (noopBreakerUsed()) {
            logger.info("--> noop breakers used, skipping test");
            return;
        }
        final Client client = client();

        // Create an index where the mappings have a field data filter
        assertAcked(prepareCreate("ramtest").setSource("{\"mappings\": {\"type\": {\"properties\": {\"test\": " +
                "{\"type\": \"text\",\"fielddata\": true,\"fielddata_frequency_filter\": {\"max\": 10000}}}}}}", XContentType.JSON));

        ensureGreen("ramtest");

        // index some different terms so we have some field data for loading
        int docCount = scaledRandomIntBetween(300, 1000);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (long id = 0; id < docCount; id++) {
            reqs.add(client.prepareIndex("ramtest", "type", Long.toString(id)).setSource("test", "value" + id));
        }
        indexRandom(true, false, true, reqs);

        // execute a search that loads field data (sorting on the "test" field)
        client.prepareSearch("ramtest").setQuery(matchAllQuery()).addSort("test", SortOrder.DESC).get();

        // clear field data cache (thus setting the loaded field data back to 0)
        clearFieldData();

        // Update circuit breaker settings
        Settings settings = Settings.builder()
                .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100b")
                .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.05)
                .build();
        assertAcked(client.admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

        // execute a search that loads field data (sorting on the "test" field)
        // again, this time it should trip the breaker
        SearchRequestBuilder searchRequest = client.prepareSearch("ramtest").setQuery(matchAllQuery()).addSort("test", SortOrder.DESC);

        String errMsg = "Data too large, data for [test] would be";
        assertFailures(searchRequest, RestStatus.INTERNAL_SERVER_ERROR, containsString(errMsg));
        errMsg = "which is larger than the limit of [100/100b]";
        assertFailures(searchRequest, RestStatus.INTERNAL_SERVER_ERROR, containsString(errMsg));

        NodesStatsResponse stats = client.admin().cluster().prepareNodesStats()
            .addMetric(BREAKER.metricName())
            .get();
        int breaks = 0;
        for (NodeStats stat : stats.getNodes()) {
            CircuitBreakerStats breakerStats = stat.getBreaker().getStats(CircuitBreaker.FIELDDATA);
            breaks += breakerStats.getTrippedCount();
        }
        assertThat(breaks, greaterThanOrEqualTo(1));
    }

    public void testRequestBreaker() throws Exception {
        if (noopBreakerUsed()) {
            logger.info("--> noop breakers used, skipping test");
            return;
        }
        assertAcked(prepareCreate("cb-test", 1, Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, between(0, 1))));
        Client client = client();

        // Make request breaker limited to a small amount
        Settings resetSettings = Settings.builder()
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10b")
                .build();
        assertAcked(client.admin().cluster().prepareUpdateSettings().setTransientSettings(resetSettings));

        // index some different terms so we have some field data for loading
        int docCount = scaledRandomIntBetween(300, 1000);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (long id = 0; id < docCount; id++) {
            reqs.add(client.prepareIndex("cb-test", "type", Long.toString(id)).setSource("test", id));
        }
        indexRandom(true, reqs);

        // A cardinality aggregation uses BigArrays and thus the REQUEST breaker
        try {
            client.prepareSearch("cb-test").setQuery(matchAllQuery()).addAggregation(cardinality("card").field("test")).get();
            fail("aggregation should have tripped the breaker");
        } catch (Exception e) {
            String errMsg = "CircuitBreakingException[[request] Data too large";
            assertThat("Exception: [" + e.toString() + "] should contain a CircuitBreakingException", e.toString(), containsString(errMsg));
            errMsg = "which is larger than the limit of [10/10b]]";
            assertThat("Exception: [" + e.toString() + "] should contain a CircuitBreakingException", e.toString(), containsString(errMsg));
        }
    }

    public void testBucketBreaker() throws Exception {
        if (noopBreakerUsed()) {
            logger.info("--> noop breakers used, skipping test");
            return;
        }
        assertAcked(prepareCreate("cb-test", 1, Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, between(0, 1))));
        Client client = client();

        // Make request breaker limited to a small amount
        Settings resetSettings = Settings.builder()
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100b")
                .build();
        assertAcked(client.admin().cluster().prepareUpdateSettings().setTransientSettings(resetSettings));

        // index some different terms so we have some field data for loading
        int docCount = scaledRandomIntBetween(100, 1000);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (long id = 0; id < docCount; id++) {
            reqs.add(client.prepareIndex("cb-test", "type", Long.toString(id)).setSource("test", id));
        }
        indexRandom(true, reqs);

        // A terms aggregation on the "test" field should trip the bucket circuit breaker
        try {
            SearchResponse resp = client.prepareSearch("cb-test")
                    .setQuery(matchAllQuery())
                    .addAggregation(terms("my_terms").field("test"))
                    .get();
            assertTrue("there should be shard failures", resp.getFailedShards() > 0);
            fail("aggregation should have tripped the breaker");
        } catch (Exception e) {
            String errMsg = "CircuitBreakingException[[request] Data too large, data for [<agg [my_terms]>] would be";
            assertThat("Exception: [" + e.toString() + "] should contain a CircuitBreakingException",
                    e.toString(), containsString(errMsg));
            errMsg = "which is larger than the limit of [100/100b]]";
            assertThat("Exception: [" + e.toString() + "] should contain a CircuitBreakingException",
                    e.toString(), containsString(errMsg));
        }
    }

    /** Issues a cache clear and waits 30 seconds for the field data breaker to be cleared */
    public void clearFieldData() throws Exception {
        client().admin().indices().prepareClearCache().setFieldDataCache(true).execute().actionGet();
        assertBusy(() -> {
            NodesStatsResponse resp = client().admin().cluster().prepareNodesStats().clear()
                .addMetric(BREAKER.metricName())
                .get(new TimeValue(15, TimeUnit.SECONDS));
            for (NodeStats nStats : resp.getNodes()) {
                assertThat("fielddata breaker never reset back to 0",
                        nStats.getBreaker().getStats(CircuitBreaker.FIELDDATA).getEstimated(),
                        equalTo(0L));
            }
        }, 30, TimeUnit.SECONDS);
    }

    public void testCanResetUnreasonableSettings() {
        if (noopBreakerUsed()) {
            logger.info("--> noop breakers used, skipping test");
            return;
        }
        Settings insane = Settings.builder()
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "5b")
            .build();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(insane).get();

        // calls updates settings to reset everything to default, checking that the request
        // is not blocked by the above inflight circuit breaker
        reset();

        assertThat(client().admin().cluster().prepareState().get()
            .getState().metadata().transientSettings().get(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey()),
            nullValue());

    }

    public void testLimitsRequestSize() {
        ByteSizeValue inFlightRequestsLimit = new ByteSizeValue(8, ByteSizeUnit.KB);
        if (noopBreakerUsed()) {
            logger.info("--> noop breakers used, skipping test");
            return;
        }

        internalCluster().ensureAtLeastNumDataNodes(2);

        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().get();
        List<NodeStats> dataNodeStats = new ArrayList<>();
        for (NodeStats stat : nodeStats.getNodes()) {
            if (stat.getNode().isDataNode()) {
                dataNodeStats.add(stat);
            }
        }

        assertThat(dataNodeStats.size(), greaterThanOrEqualTo(2));
        Collections.shuffle(dataNodeStats, random());

        // send bulk request from source node to target node later. The sole shard is bound to the target node.
        NodeStats targetNode = dataNodeStats.get(0);
        NodeStats sourceNode = dataNodeStats.get(1);

        assertAcked(prepareCreate("index").setSettings(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put("index.routing.allocation.include._name", targetNode.getNode().getName())
            .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
        ));

        Client client = client(sourceNode.getNode().getName());

        // we use the limit size as a (very) rough indication on how many requests we should sent to hit the limit
        int numRequests = inFlightRequestsLimit.bytesAsInt();
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numRequests; i++) {
            IndexRequest indexRequest = new IndexRequest("index", "type", Integer.toString(i));
            indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field", "value", "num", i);
            bulkRequest.add(indexRequest);
        }

        Settings limitSettings = Settings.builder()
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), inFlightRequestsLimit)
            .build();

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(limitSettings));

        // can either fail directly with an exception or the response contains exceptions (depending on client)
        try {
            BulkResponse response = client.bulk(bulkRequest).actionGet();
            if (!response.hasFailures()) {
                fail("Should have thrown CircuitBreakingException");
            } else {
                // each item must have failed with CircuitBreakingException
                for (BulkItemResponse bulkItemResponse : response) {
                    Throwable cause = ExceptionsHelper.unwrapCause(bulkItemResponse.getFailure().getCause());
                    assertThat(cause, instanceOf(CircuitBreakingException.class));
                    assertEquals(((CircuitBreakingException) cause).getByteLimit(), inFlightRequestsLimit.getBytes());
                }
            }
        } catch (CircuitBreakingException ex) {
            assertEquals(ex.getByteLimit(), inFlightRequestsLimit.getBytes());
        }
    }
}
