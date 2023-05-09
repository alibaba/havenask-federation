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

package org.havenask.index;

import org.havenask.action.index.IndexRequestBuilder;
import org.havenask.action.search.SearchResponse;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.query.QueryBuilders;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.hamcrest.HavenaskAssertions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;

public class IndexRequestBuilderIT extends HavenaskIntegTestCase {
    public void testSetSource() throws InterruptedException, ExecutionException {
        createIndex("test");
        Map<String, Object> map = new HashMap<>();
        map.put("test_field", "foobar");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[] {
                client().prepareIndex("test", "test").setSource((Object)"test_field", (Object)"foobar"),
                client().prepareIndex("test", "test").setSource("{\"test_field\" : \"foobar\"}", XContentType.JSON),
                client().prepareIndex("test", "test").setSource(new BytesArray("{\"test_field\" : \"foobar\"}"), XContentType.JSON),
                client().prepareIndex("test", "test").setSource(new BytesArray("{\"test_field\" : \"foobar\"}"), XContentType.JSON),
                client().prepareIndex("test", "test")
                    .setSource(BytesReference.toBytes(new BytesArray("{\"test_field\" : \"foobar\"}")), XContentType.JSON),
                client().prepareIndex("test", "test").setSource(map)
        };
        indexRandom(true, builders);
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.termQuery("test_field", "foobar")).get();
        HavenaskAssertions.assertHitCount(searchResponse, builders.length);
    }

    public void testOddNumberOfSourceObjects() {
        try {
            client().prepareIndex("test", "test").setSource("test_field", "foobar", new Object());
            fail ("Expected IllegalArgumentException");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("The number of object passed must be even but was [3]"));
        }
    }
}
