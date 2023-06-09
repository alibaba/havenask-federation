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

package org.havenask.mget;

import org.havenask.HavenaskException;
import org.havenask.action.admin.indices.alias.Alias;
import org.havenask.action.get.MultiGetItemResponse;
import org.havenask.action.get.MultiGetRequest;
import org.havenask.action.get.MultiGetRequestBuilder;
import org.havenask.action.get.MultiGetResponse;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.search.fetch.subphase.FetchSourceContext;
import org.havenask.test.HavenaskIntegTestCase;

import java.io.IOException;
import java.util.Map;

import static org.havenask.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.havenask.common.xcontent.XContentFactory.jsonBuilder;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SimpleMgetIT extends HavenaskIntegTestCase {

    public void testThatMgetShouldWorkWithOneIndexMissing() throws IOException {
        createIndex("test");

        client().prepareIndex("test", "test", "1").setSource(jsonBuilder().startObject().field("foo", "bar").endObject())
                .setRefreshPolicy(IMMEDIATE).get();

        MultiGetResponse mgetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("test", "test", "1"))
                .add(new MultiGetRequest.Item("nonExistingIndex", "test", "1"))
                .get();
        assertThat(mgetResponse.getResponses().length, is(2));

        assertThat(mgetResponse.getResponses()[0].getIndex(), is("test"));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(false));

        assertThat(mgetResponse.getResponses()[1].getIndex(), is("nonExistingIndex"));
        assertThat(mgetResponse.getResponses()[1].isFailed(), is(true));
        assertThat(mgetResponse.getResponses()[1].getFailure().getMessage(), is("no such index [nonExistingIndex]"));
        assertThat(((HavenaskException) mgetResponse.getResponses()[1].getFailure().getFailure()).getIndex().getName(),
            is("nonExistingIndex"));

        mgetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("nonExistingIndex", "test", "1"))
                .get();
        assertThat(mgetResponse.getResponses().length, is(1));
        assertThat(mgetResponse.getResponses()[0].getIndex(), is("nonExistingIndex"));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(true));
        assertThat(mgetResponse.getResponses()[0].getFailure().getMessage(), is("no such index [nonExistingIndex]"));
        assertThat(((HavenaskException) mgetResponse.getResponses()[0].getFailure().getFailure()).getIndex().getName(),
            is("nonExistingIndex"));
    }

    public void testThatMgetShouldWorkWithMultiIndexAlias() throws IOException {
        assertAcked(prepareCreate("test").addAlias(new Alias("multiIndexAlias")));
        assertAcked(prepareCreate("test2").addAlias(new Alias("multiIndexAlias")));

        client().prepareIndex("test", "test", "1").setSource(jsonBuilder().startObject().field("foo", "bar").endObject())
            .setRefreshPolicy(IMMEDIATE).get();

        MultiGetResponse mgetResponse = client().prepareMultiGet()
            .add(new MultiGetRequest.Item("test", "test", "1"))
            .add(new MultiGetRequest.Item("multiIndexAlias", "test", "1"))
            .get();
        assertThat(mgetResponse.getResponses().length, is(2));

        assertThat(mgetResponse.getResponses()[0].getIndex(), is("test"));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(false));

        assertThat(mgetResponse.getResponses()[1].getIndex(), is("multiIndexAlias"));
        assertThat(mgetResponse.getResponses()[1].isFailed(), is(true));
        assertThat(mgetResponse.getResponses()[1].getFailure().getMessage(), containsString("more than one index"));

        mgetResponse = client().prepareMultiGet()
            .add(new MultiGetRequest.Item("multiIndexAlias", "test", "1"))
            .get();
        assertThat(mgetResponse.getResponses().length, is(1));
        assertThat(mgetResponse.getResponses()[0].getIndex(), is("multiIndexAlias"));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(true));
        assertThat(mgetResponse.getResponses()[0].getFailure().getMessage(), containsString("more than one index"));
    }

    public void testThatMgetShouldWorkWithAliasRouting() throws IOException {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias1").routing("abc"))
            .addMapping("test", jsonBuilder()
                .startObject().startObject("test").startObject("_routing").field("required", true).endObject().endObject().endObject()));

        client().prepareIndex("alias1", "test", "1").setSource(jsonBuilder().startObject().field("foo", "bar").endObject())
            .setRefreshPolicy(IMMEDIATE).get();

        MultiGetResponse mgetResponse = client().prepareMultiGet()
            .add(new MultiGetRequest.Item("alias1", "test", "1"))
            .get();
        assertEquals(1, mgetResponse.getResponses().length);

        assertEquals("test", mgetResponse.getResponses()[0].getIndex());
        assertFalse(mgetResponse.getResponses()[0].isFailed());
    }

    @SuppressWarnings("unchecked")
    public void testThatSourceFilteringIsSupported() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        BytesReference sourceBytesRef = BytesReference.bytes(jsonBuilder().startObject()
                .array("field", "1", "2")
                .startObject("included").field("field", "should be seen").field("hidden_field", "should not be seen").endObject()
                .field("excluded", "should not be seen")
                .endObject());
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource(sourceBytesRef, XContentType.JSON).get();
        }

        MultiGetRequestBuilder request = client().prepareMultiGet();
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                request.add(new MultiGetRequest.Item(indexOrAlias(), "type", Integer.toString(i))
                    .fetchSourceContext(new FetchSourceContext(true, new String[] {"included"}, new String[] {"*.hidden_field"})));
            } else {
                request.add(new MultiGetRequest.Item(indexOrAlias(), "type", Integer.toString(i))
                    .fetchSourceContext(new FetchSourceContext(false)));
            }
        }

        MultiGetResponse response = request.get();

        assertThat(response.getResponses().length, equalTo(100));
        for (int i = 0; i < 100; i++) {
            MultiGetItemResponse responseItem = response.getResponses()[i];
            assertThat(responseItem.getIndex(), equalTo("test"));
            if (i % 2 == 0) {
                Map<String, Object> source = responseItem.getResponse().getSourceAsMap();
                assertThat(source.size(), equalTo(1));
                assertThat(source, hasKey("included"));
                assertThat(((Map<String, Object>) source.get("included")).size(), equalTo(1));
                assertThat(((Map<String, Object>) source.get("included")), hasKey("field"));
            } else {
                assertThat(responseItem.getResponse().getSourceAsBytes(), nullValue());
            }
        }
    }

    public void testThatRoutingPerDocumentIsSupported() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias"))
                .setSettings(Settings.builder()
                        .put(indexSettings())
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(2, DEFAULT_MAX_NUM_SHARDS))));

        final String id = routingKeyForShard("test", 0);
        final String routingOtherShard = routingKeyForShard("test", 1);

        client().prepareIndex("test", "test", id).setRefreshPolicy(IMMEDIATE).setRouting(routingOtherShard)
                .setSource(jsonBuilder().startObject().field("foo", "bar").endObject())
                .get();

        MultiGetResponse mgetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item(indexOrAlias(), "test", id).routing(routingOtherShard))
                .add(new MultiGetRequest.Item(indexOrAlias(), "test", id))
                .get();

        assertThat(mgetResponse.getResponses().length, is(2));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(false));
        assertThat(mgetResponse.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(mgetResponse.getResponses()[0].getResponse().getIndex(), is("test"));

        assertThat(mgetResponse.getResponses()[1].isFailed(), is(false));
        assertThat(mgetResponse.getResponses()[1].getResponse().isExists(), is(false));
        assertThat(mgetResponse.getResponses()[1].getResponse().getIndex(), is("test"));
    }

    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }
}
