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

package org.havenask.action.search;

import org.havenask.client.Client;
import org.havenask.common.settings.Settings;
import org.havenask.env.Environment;
import org.havenask.index.query.QueryBuilders;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.test.HavenaskTestCase;
import org.havenask.transport.MockTransportClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.hamcrest.CoreMatchers.equalTo;

public class SearchRequestBuilderTests extends HavenaskTestCase {
    private static Client client;

    @BeforeClass
    public static void initClient() {
        //this client will not be hit by any request, but it needs to be a non null proper client
        //that is why we create it but we don't add any transport address to it
        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        client = new MockTransportClient(settings);
    }

    @AfterClass
    public static void closeClient() {
        client.close();
        client = null;
    }

    public void testEmptySourceToString() {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder().toString()));
    }

    public void testQueryBuilderQueryToString() {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()));
    }

    public void testSearchSourceBuilderToString() {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
        searchRequestBuilder.setSource(new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")));
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder()
            .query(QueryBuilders.termQuery("field", "value")).toString()));
    }

    public void testThatToStringDoesntWipeRequestSource() {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch()
            .setSource(new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")));
        String preToString = searchRequestBuilder.request().toString();
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder()
            .query(QueryBuilders.termQuery("field", "value")).toString()));
        String postToString = searchRequestBuilder.request().toString();
        assertThat(preToString, equalTo(postToString));
    }
}
