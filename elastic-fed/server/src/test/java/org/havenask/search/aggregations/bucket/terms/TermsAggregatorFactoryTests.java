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

package org.havenask.search.aggregations.bucket.terms;

import org.havenask.search.aggregations.Aggregator;
import org.havenask.search.aggregations.AggregatorFactories;
import org.havenask.test.HavenaskTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TermsAggregatorFactoryTests extends HavenaskTestCase {
    public void testPickEmpty() throws Exception {
        AggregatorFactories empty = mock(AggregatorFactories.class);
        when(empty.countAggregators()).thenReturn(0);
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(empty, randomInt(), randomInt()),
            equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST));
    }

    public void testPickNonEempty() {
        AggregatorFactories nonEmpty = mock(AggregatorFactories.class);
        when(nonEmpty.countAggregators()).thenReturn(1);
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, Integer.MAX_VALUE, -1),
            equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 10, -1),
            equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 10, 5),
            equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 10, 10),
            equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 10, 100),
            equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 1, 2),
            equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST));
        assertThat(TermsAggregatorFactory.pickSubAggColectMode(nonEmpty, 1, 100),
            equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST));
    }
}
