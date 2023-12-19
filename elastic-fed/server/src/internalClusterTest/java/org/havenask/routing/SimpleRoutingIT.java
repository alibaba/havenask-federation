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

package org.havenask.routing;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;

import org.havenask.HavenaskException;
import org.havenask.action.DocWriteRequest;
import org.havenask.action.RoutingMissingException;
import org.havenask.action.admin.indices.alias.Alias;
import org.havenask.action.bulk.BulkItemResponse;
import org.havenask.action.bulk.BulkResponse;
import org.havenask.action.explain.ExplainResponse;
import org.havenask.action.get.GetResponse;
import org.havenask.action.get.MultiGetRequest;
import org.havenask.action.get.MultiGetResponse;
import org.havenask.action.support.WriteRequest.RefreshPolicy;
import org.havenask.action.termvectors.MultiTermVectorsResponse;
import org.havenask.action.termvectors.TermVectorsRequest;
import org.havenask.action.termvectors.TermVectorsResponse;
import org.havenask.action.update.UpdateRequest;
import org.havenask.action.update.UpdateResponse;
import org.havenask.client.Requests;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.routing.IndexRouting;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.index.query.QueryBuilders;
import org.havenask.rest.RestStatus;
import org.havenask.test.HavenaskIntegTestCase;

public class SimpleRoutingIT extends HavenaskIntegTestCase {

    @Override
    protected int minimumNumberOfShards() {
        return 2;
    }

    public String findNonMatchingRoutingValue(String index, String id) throws IOException {
        ClusterState state = client().admin().cluster().prepareState().all().get().getState();
        IndexMetadata metadata = state.metadata().index(index);
        IndexMetadata withoutRoutingRequired = IndexMetadata.builder(metadata).putMapping("_doc", "{}").build();
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(withoutRoutingRequired);
        int routing = -1;
        int idShard;
        int routingShard;
        do {
            idShard = indexRouting.getShard(id, null);
            routingShard = indexRouting.getShard(id, Integer.toString(++routing));
        } while (idShard == routingShard);

        return Integer.toString(routing);
    }

    public void testSimpleCrudRouting() throws Exception {
        createIndex("test");
        ensureGreen();
        String routingValue = findNonMatchingRoutingValue("test", "1");
        logger.info("--> indexing with id [1], and routing [{}]", routingValue);
        client().prepareIndex("test", "_doc", "1")
                .setRouting(routingValue)
                .setSource("field", "value1")
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "_doc", "1").execute().actionGet().isExists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "_doc", "1").setRouting(routingValue).execute().actionGet().isExists(), equalTo(true));
        }

        logger.info("--> deleting with no routing, should not delete anything");
        client().prepareDelete("test", "_doc", "1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "_doc", "1").execute().actionGet().isExists(), equalTo(false));
            assertThat(client().prepareGet("test", "_doc", "1").setRouting(routingValue).execute().actionGet().isExists(), equalTo(true));
        }

        logger.info("--> deleting with routing, should delete");
        client().prepareDelete("test", "_doc", "1").setRouting(routingValue).setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "_doc", "1").execute().actionGet().isExists(), equalTo(false));
            assertThat(client().prepareGet("test", "_doc", "1").setRouting(routingValue).execute().actionGet().isExists(), equalTo(false));
        }

        logger.info("--> indexing with id [1], and routing [0]");
        client().prepareIndex("test", "_doc", "1")
                .setRouting(routingValue)
                .setSource("field", "value1")
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "_doc", "1").execute().actionGet().isExists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "_doc", "1").setRouting(routingValue).execute().actionGet().isExists(), equalTo(true));
        }
    }

    public void testSimpleSearchRouting() throws IOException {
        createIndex("test");
        ensureGreen();
        String routingValue = findNonMatchingRoutingValue("test", "1");

        logger.info("--> indexing with id [1], and routing [{}]", routingValue);
        client().prepareIndex("test", "_doc", "1")
                .setRouting(routingValue)
                .setSource("field", "value1")
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "_doc", "1").execute().actionGet().isExists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "_doc", "1").setRouting(routingValue).execute().actionGet().isExists(), equalTo(true));
        }

        logger.info("--> search with no routing, should fine one");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch()
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(1L));
        }

        logger.info("--> search with wrong routing, should not find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch()
                    .setRouting("1")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(0L));
            assertThat(client().prepareSearch()
                    .setSize(0)
                    .setRouting("1")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(0L));
        }

        logger.info("--> search with correct routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch()
                    .setRouting(routingValue)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(1L));
            assertThat(client().prepareSearch()
                    .setSize(0)
                    .setRouting(routingValue)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(1L));
        }

        String secondRoutingValue = "1";
        logger.info("--> indexing with id [{}], and routing [{}]", routingValue, secondRoutingValue);
        client().prepareIndex("test", "_doc", routingValue)
                .setRouting(secondRoutingValue)
                .setSource("field", "value1")
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();

        logger.info("--> search with no routing, should fine two");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch()
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(2L));
            assertThat(client().prepareSearch()
                    .setSize(0)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(2L));
        }

        logger.info("--> search with {} routing, should find one", routingValue);
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch()
                    .setRouting(routingValue)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(1L));
            assertThat(client().prepareSearch()
                    .setSize(0)
                    .setRouting(routingValue)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(1L));
        }

        logger.info("--> search with {} routing, should find one", secondRoutingValue);
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch()
                    .setRouting("1")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(1L));
            assertThat(client().prepareSearch()
                    .setSize(0)
                    .setRouting(secondRoutingValue)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(1L));
        }

        logger.info("--> search with {},{} indexRoutings , should find two", routingValue, "1");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch()
                    .setRouting(routingValue, secondRoutingValue)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(2L));
            assertThat(client().prepareSearch()
                    .setSize(0)
                    .setRouting(routingValue, secondRoutingValue)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(2L));
        }

        logger.info("--> search with {},{},{} indexRoutings , should find two", routingValue, secondRoutingValue, routingValue);
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch()
                    .setRouting(routingValue, secondRoutingValue, routingValue)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(2L));
            assertThat(client().prepareSearch()
                    .setSize(0)
                    .setRouting(routingValue, secondRoutingValue, routingValue)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute()
                    .actionGet()
                    .getHits()
                    .getTotalHits()
                    .value, equalTo(2L));
        }
    }

    public void testRequiredRoutingCrudApis() throws Exception {
        client().admin()
                .indices()
                .prepareCreate("test")
                .addAlias(new Alias("alias"))
                .addMapping(
                        "_doc",
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("_doc")
                                .startObject("_routing")
                                .field("required", true)
                                .endObject()
                                .endObject()
                                .endObject()
                )
                .execute()
                .actionGet();
        ensureGreen();
        String routingValue = findNonMatchingRoutingValue("test", "1");

        logger.info("--> indexing with id [1], and routing [{}]", routingValue);
        client().prepareIndex(indexOrAlias(), "_doc", "1")
                .setRouting(routingValue)
                .setSource("field", "value1")
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();
        logger.info("--> verifying get with no routing, should fail");

        logger.info("--> indexing with id [1], with no routing, should fail");
        try {
            client().prepareIndex(indexOrAlias(), "_doc", "1").setSource("field", "value1").get();
            fail("index with missing routing when routing is required should fail");
        } catch (HavenaskException e) {
            assertThat(e.unwrapCause(), instanceOf(RoutingMissingException.class));
        }

        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(
                    client().prepareGet(indexOrAlias(), "_doc", "1").setRouting(routingValue).execute().actionGet().isExists(),
                    equalTo(true)
            );
        }

        logger.info("--> deleting with no routing, should fail");
        try {
            client().prepareDelete(indexOrAlias(), "_doc", "1").get();
            fail("delete with missing routing when routing is required should fail");
        } catch (HavenaskException e) {
            assertThat(e.unwrapCause(), instanceOf(RoutingMissingException.class));
        }

        for (int i = 0; i < 5; i++) {
            try {
                client().prepareGet(indexOrAlias(), "_doc", "1").execute().actionGet().isExists();
                fail("get with missing routing when routing is required should fail");
            } catch (RoutingMissingException e) {
                assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(e.getMessage(), equalTo("routing is required for [test]/[_doc]/[1]"));
            }
            assertThat(
                    client().prepareGet(indexOrAlias(), "_doc", "1").setRouting(routingValue).execute().actionGet().isExists(),
                    equalTo(true)
            );
        }

        try {
            client().prepareUpdate(indexOrAlias(), "_doc", "1")
                    .setDoc(Requests.INDEX_CONTENT_TYPE, "field", "value2")
                    .execute()
                    .actionGet();
            fail("update with missing routing when routing is required should fail");
        } catch (HavenaskException e) {
            assertThat(e.unwrapCause(), instanceOf(RoutingMissingException.class));
        }

        client().prepareUpdate(indexOrAlias(), "_doc", "1")
                .setRouting(routingValue)
                .setDoc(Requests.INDEX_CONTENT_TYPE, "field", "value2")
                .get();
        client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < 5; i++) {
            try {
                client().prepareGet(indexOrAlias(), "_doc", "1").execute().actionGet().isExists();
                fail();
            } catch (RoutingMissingException e) {
                assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(e.getMessage(), equalTo("routing is required for [test]/[_doc]/[1]"));
            }
            GetResponse getResponse = client().prepareGet(indexOrAlias(), "_doc", "1").setRouting(routingValue).execute().actionGet();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getSourceAsMap().get("field"), equalTo("value2"));
        }

        client().prepareDelete(indexOrAlias(), "_doc", "1").setRouting(routingValue).setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();

        for (int i = 0; i < 5; i++) {
            try {
                client().prepareGet(indexOrAlias(), "_doc", "1").execute().actionGet().isExists();
                fail();
            } catch (RoutingMissingException e) {
                assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(e.getMessage(), equalTo("routing is required for [test]/[_doc]/[1]"));
            }
            assertThat(
                    client().prepareGet(indexOrAlias(), "_doc", "1").setRouting(routingValue).execute().actionGet().isExists(),
                    equalTo(false)
            );
        }
    }

    public void testRequiredRoutingBulk() throws Exception {
        client().admin()
                .indices()
                .prepareCreate("test")
                .addAlias(new Alias("alias"))
                .addMapping(
                        "_doc",
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("_doc")
                                .startObject("_routing")
                                .field("required", true)
                                .endObject()
                                .endObject()
                                .endObject()
                )
                .execute()
                .actionGet();
        ensureGreen();
        {
            BulkResponse bulkResponse = client().prepareBulk()
                    .add(Requests.indexRequest(indexOrAlias()).type("_doc").id("1").source(Requests.INDEX_CONTENT_TYPE, "field", "value"))
                    .execute()
                    .actionGet();
            assertThat(bulkResponse.getItems().length, equalTo(1));
            assertThat(bulkResponse.hasFailures(), equalTo(true));

            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                assertThat(bulkItemResponse.isFailed(), equalTo(true));
                assertThat(bulkItemResponse.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
                assertThat(bulkItemResponse.getFailure().getStatus(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(bulkItemResponse.getFailure().getCause(), instanceOf(RoutingMissingException.class));
                assertThat(bulkItemResponse.getFailureMessage(), containsString("routing is required for [test]/[_doc]/[1]"));
            }
        }

        {
            BulkResponse bulkResponse = client().prepareBulk()
                    .add(
                            Requests.indexRequest(indexOrAlias())
                                    .type("_doc")
                                    .id("1")
                                    .routing("0")
                                    .source(Requests.INDEX_CONTENT_TYPE, "field", "value")
                    )
                    .execute()
                    .actionGet();
            assertThat(bulkResponse.hasFailures(), equalTo(false));
        }

        {
            BulkResponse bulkResponse = client().prepareBulk()
                    .add(new UpdateRequest(indexOrAlias(), "_doc", "1").doc(Requests.INDEX_CONTENT_TYPE, "field", "value2"))
                    .execute()
                    .actionGet();
            assertThat(bulkResponse.getItems().length, equalTo(1));
            assertThat(bulkResponse.hasFailures(), equalTo(true));

            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                assertThat(bulkItemResponse.isFailed(), equalTo(true));
                assertThat(bulkItemResponse.getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
                assertThat(bulkItemResponse.getFailure().getStatus(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(bulkItemResponse.getFailure().getCause(), instanceOf(RoutingMissingException.class));
                assertThat(bulkItemResponse.getFailureMessage(), containsString("routing is required for [test]/[_doc]/[1]"));
            }
        }

        {
            BulkResponse bulkResponse = client().prepareBulk()
                    .add(new UpdateRequest(indexOrAlias(), "_doc", "1").doc(Requests.INDEX_CONTENT_TYPE, "field", "value2").routing("0"))
                    .execute()
                    .actionGet();
            assertThat(bulkResponse.hasFailures(), equalTo(false));
        }

        {
            BulkResponse bulkResponse = client().prepareBulk()
                    .add(Requests.deleteRequest(indexOrAlias()).type("_doc").id("1"))
                    .execute()
                    .actionGet();
            assertThat(bulkResponse.getItems().length, equalTo(1));
            assertThat(bulkResponse.hasFailures(), equalTo(true));

            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                assertThat(bulkItemResponse.isFailed(), equalTo(true));
                assertThat(bulkItemResponse.getOpType(), equalTo(DocWriteRequest.OpType.DELETE));
                assertThat(bulkItemResponse.getFailure().getStatus(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(bulkItemResponse.getFailure().getCause(), instanceOf(RoutingMissingException.class));
                assertThat(bulkItemResponse.getFailureMessage(), containsString("routing is required for [test]/[_doc]/[1]"));
            }
        }

        {
            BulkResponse bulkResponse = client().prepareBulk()
                    .add(Requests.deleteRequest(indexOrAlias()).type("_doc").id("1").routing("0"))
                    .execute()
                    .actionGet();
            assertThat(bulkResponse.getItems().length, equalTo(1));
            assertThat(bulkResponse.hasFailures(), equalTo(false));
        }
    }

    public void testRequiredRoutingMappingVariousAPIs() throws Exception {

        client().admin()
                .indices()
                .prepareCreate("test")
                .addAlias(new Alias("alias"))
                .addMapping(
                        "_doc",
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .startObject("_doc")
                                .startObject("_routing")
                                .field("required", true)
                                .endObject()
                                .endObject()
                                .endObject()
                )
                .execute()
                .actionGet();
        ensureGreen();
        String routingValue = findNonMatchingRoutingValue("test", "1");
        logger.info("--> indexing with id [1], and routing [{}]", routingValue);
        client().prepareIndex(indexOrAlias(), "_doc", "1").setRouting(routingValue).setSource("field", "value1").get();
        logger.info("--> indexing with id [2], and routing [{}]", routingValue);
        client().prepareIndex(indexOrAlias(), "_doc", "2")
                .setRouting(routingValue)
                .setSource("field", "value2")
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();

        logger.info("--> verifying get with id [1] with routing [0], should succeed");
        assertThat(
                client().prepareGet(indexOrAlias(), "_doc", "1").setRouting(routingValue).execute().actionGet().isExists(),
                equalTo(true)
        );

        logger.info("--> verifying get with id [1], with no routing, should fail");
        try {
            client().prepareGet(indexOrAlias(), "_doc", "1").get();
            fail();
        } catch (RoutingMissingException e) {
            assertThat(e.getMessage(), equalTo("routing is required for [test]/[_doc]/[1]"));
        }

        logger.info("--> verifying explain with id [2], with routing [0], should succeed");
        ExplainResponse explainResponse = client().prepareExplain(indexOrAlias(), "_doc", "2")
                .setQuery(QueryBuilders.matchAllQuery())
                .setRouting(routingValue)
                .get();
        assertThat(explainResponse.isExists(), equalTo(true));
        assertThat(explainResponse.isMatch(), equalTo(true));

        logger.info("--> verifying explain with id [2], with no routing, should fail");
        try {
            client().prepareExplain(indexOrAlias(), "_doc", "2").setQuery(QueryBuilders.matchAllQuery()).get();
            fail();
        } catch (RoutingMissingException e) {
            assertThat(e.getMessage(), equalTo("routing is required for [test]/[_doc]/[2]"));
        }

        logger.info("--> verifying term vector with id [1], with routing [0], should succeed");
        TermVectorsResponse termVectorsResponse = client().prepareTermVectors(indexOrAlias(), "_doc", "1").setRouting(routingValue).get();
        assertThat(termVectorsResponse.isExists(), equalTo(true));
        assertThat(termVectorsResponse.getId(), equalTo("1"));

        try {
            client().prepareTermVectors(indexOrAlias(), "_doc", "1").get();
            fail();
        } catch (RoutingMissingException e) {
            assertThat(e.getMessage(), equalTo("routing is required for [test]/[_doc]/[1]"));
        }

        UpdateResponse updateResponse = client().prepareUpdate(indexOrAlias(), "_doc", "1")
                .setRouting(routingValue)
                .setDoc(Requests.INDEX_CONTENT_TYPE, "field1", "value1")
                .get();
        assertThat(updateResponse.getId(), equalTo("1"));
        assertThat(updateResponse.getVersion(), equalTo(2L));

        try {
            client().prepareUpdate(indexOrAlias(), "_doc", "1").setDoc(Requests.INDEX_CONTENT_TYPE, "field1", "value1").get();
            fail();
        } catch (RoutingMissingException e) {
            assertThat(e.getMessage(), equalTo("routing is required for [test]/[_doc]/[1]"));
        }

        logger.info("--> verifying mget with ids [1,2], with routing [0], should succeed");
        MultiGetResponse multiGetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item(indexOrAlias(), "_doc", "1").routing("0"))
                .add(new MultiGetRequest.Item(indexOrAlias(), "_doc", "2").routing("0"))
                .get();
        assertThat(multiGetResponse.getResponses().length, equalTo(2));
        assertThat(multiGetResponse.getResponses()[0].isFailed(), equalTo(false));
        assertThat(multiGetResponse.getResponses()[0].getResponse().getId(), equalTo("1"));
        assertThat(multiGetResponse.getResponses()[1].isFailed(), equalTo(false));
        assertThat(multiGetResponse.getResponses()[1].getResponse().getId(), equalTo("2"));

        logger.info("--> verifying mget with ids [1,2], with no routing, should fail");
        multiGetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item(indexOrAlias(), "_doc", "1"))
                .add(new MultiGetRequest.Item(indexOrAlias(), "_doc", "2"))
                .get();
        assertThat(multiGetResponse.getResponses().length, equalTo(2));
        assertThat(multiGetResponse.getResponses()[0].isFailed(), equalTo(true));
        assertThat(multiGetResponse.getResponses()[0].getFailure().getId(), equalTo("1"));
        assertThat(multiGetResponse.getResponses()[0].getFailure().getMessage(), equalTo("routing is required for [test]/[_doc]/[1]"));
        assertThat(multiGetResponse.getResponses()[1].isFailed(), equalTo(true));
        assertThat(multiGetResponse.getResponses()[1].getFailure().getId(), equalTo("2"));
        assertThat(multiGetResponse.getResponses()[1].getFailure().getMessage(), equalTo("routing is required for [test]/[_doc]/[2]"));

        MultiTermVectorsResponse multiTermVectorsResponse = client().prepareMultiTermVectors()
                .add(new TermVectorsRequest(indexOrAlias(), "_doc", "1").routing(routingValue))
                .add(new TermVectorsRequest(indexOrAlias(), "_doc", "2").routing(routingValue))
                .get();
        assertThat(multiTermVectorsResponse.getResponses().length, equalTo(2));
        assertThat(multiTermVectorsResponse.getResponses()[0].getId(), equalTo("1"));
        assertThat(multiTermVectorsResponse.getResponses()[0].isFailed(), equalTo(false));
        assertThat(multiTermVectorsResponse.getResponses()[0].getResponse().getId(), equalTo("1"));
        assertThat(multiTermVectorsResponse.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(multiTermVectorsResponse.getResponses()[1].getId(), equalTo("2"));
        assertThat(multiTermVectorsResponse.getResponses()[1].isFailed(), equalTo(false));
        assertThat(multiTermVectorsResponse.getResponses()[1].getResponse().getId(), equalTo("2"));
        assertThat(multiTermVectorsResponse.getResponses()[1].getResponse().isExists(), equalTo(true));

        multiTermVectorsResponse = client().prepareMultiTermVectors()
                .add(new TermVectorsRequest(indexOrAlias(), "_doc", "1"))
                .add(new TermVectorsRequest(indexOrAlias(), "_doc", "2"))
                .get();
        assertThat(multiTermVectorsResponse.getResponses().length, equalTo(2));
        assertThat(multiTermVectorsResponse.getResponses()[0].getId(), equalTo("1"));
        assertThat(multiTermVectorsResponse.getResponses()[0].isFailed(), equalTo(true));
        assertThat(
                multiTermVectorsResponse.getResponses()[0].getFailure().getCause().getMessage(),
                equalTo("routing is required for [test]/[_doc]/[1]")
        );
        assertThat(multiTermVectorsResponse.getResponses()[0].getResponse(), nullValue());
        assertThat(multiTermVectorsResponse.getResponses()[1].getId(), equalTo("2"));
        assertThat(multiTermVectorsResponse.getResponses()[1].isFailed(), equalTo(true));
        assertThat(multiTermVectorsResponse.getResponses()[1].getResponse(), nullValue());
        assertThat(
                multiTermVectorsResponse.getResponses()[1].getFailure().getCause().getMessage(),
                equalTo("routing is required for [test]/[_doc]/[2]")
        );
    }

    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }
}