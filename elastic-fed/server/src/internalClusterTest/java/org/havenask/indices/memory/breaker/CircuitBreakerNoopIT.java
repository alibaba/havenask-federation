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

import org.havenask.action.index.IndexRequestBuilder;
import org.havenask.client.Client;
import org.havenask.common.settings.Settings;
import org.havenask.indices.breaker.HierarchyCircuitBreakerService;
import org.havenask.search.sort.SortOrder;
import org.havenask.test.HavenaskIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.havenask.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.havenask.index.query.QueryBuilders.matchAllQuery;
import static org.havenask.search.aggregations.AggregationBuilders.cardinality;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;

/** Tests for the noop breakers, which are non-dynamic settings */
@HavenaskIntegTestCase.ClusterScope(scope= HavenaskIntegTestCase.Scope.SUITE, numDataNodes=0)
public class CircuitBreakerNoopIT extends HavenaskIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "noop")
                // This is set low, because if the "noop" is not a noop, it will break
                .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10b")
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "noop")
                // This is set low, because if the "noop" is not a noop, it will break
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10b")
                .build();
    }

    public void testNoopRequestBreaker() throws Exception {
        assertAcked(prepareCreate("cb-test", 1, Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, between(0, 1))));
        Client client = client();

        // index some different terms so we have some field data for loading
        int docCount = scaledRandomIntBetween(300, 1000);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (long id = 0; id < docCount; id++) {
            reqs.add(client.prepareIndex("cb-test", "type", Long.toString(id)).setSource("test", id));
        }
        indexRandom(true, reqs);

        // A cardinality aggregation uses BigArrays and thus the REQUEST breaker
        client.prepareSearch("cb-test").setQuery(matchAllQuery()).addAggregation(cardinality("card").field("test")).get();
        // no exception because the breaker is a noop
    }

    public void testNoopFielddataBreaker() throws Exception {
        assertAcked(prepareCreate("cb-test", 1, Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, between(0, 1))));
        Client client = client();

        // index some different terms so we have some field data for loading
        int docCount = scaledRandomIntBetween(300, 1000);
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (long id = 0; id < docCount; id++) {
            reqs.add(client.prepareIndex("cb-test", "type", Long.toString(id)).setSource("test", id));
        }
        indexRandom(true, reqs);

        // Sorting using fielddata and thus the FIELDDATA breaker
        client.prepareSearch("cb-test").setQuery(matchAllQuery()).addSort("test", SortOrder.DESC).get();
        // no exception because the breaker is a noop
    }
}
