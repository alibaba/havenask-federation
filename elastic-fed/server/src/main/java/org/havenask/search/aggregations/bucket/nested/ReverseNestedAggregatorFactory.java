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

package org.havenask.search.aggregations.bucket.nested;

import org.havenask.index.mapper.ObjectMapper;
import org.havenask.index.query.QueryShardContext;
import org.havenask.search.aggregations.Aggregator;
import org.havenask.search.aggregations.AggregatorFactories;
import org.havenask.search.aggregations.AggregatorFactory;
import org.havenask.search.aggregations.CardinalityUpperBound;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.NonCollectingAggregator;
import org.havenask.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public class ReverseNestedAggregatorFactory extends AggregatorFactory {

    private final boolean unmapped;
    private final ObjectMapper parentObjectMapper;

    public ReverseNestedAggregatorFactory(String name, boolean unmapped, ObjectMapper parentObjectMapper,
                                          QueryShardContext queryShardContext, AggregatorFactory parent,
                                          AggregatorFactories.Builder subFactories,
                                          Map<String, Object> metadata) throws IOException {
        super(name, queryShardContext, parent, subFactories, metadata);
        this.unmapped = unmapped;
        this.parentObjectMapper = parentObjectMapper;
    }

    @Override
    public Aggregator createInternal(SearchContext searchContext,
                                        Aggregator parent,
                                        CardinalityUpperBound cardinality,
                                        Map<String, Object> metadata) throws IOException {
        if (unmapped) {
            return new Unmapped(name, searchContext, parent, factories, metadata);
        } else {
            return new ReverseNestedAggregator(name, factories, parentObjectMapper,
                searchContext, parent, cardinality, metadata);
        }
    }

    private static final class Unmapped extends NonCollectingAggregator {

        Unmapped(String name,
                    SearchContext context,
                    Aggregator parent,
                    AggregatorFactories factories,
                    Map<String, Object> metadata) throws IOException {
            super(name, context, parent, factories, metadata);
        }

        @Override
        public InternalAggregation buildEmptyAggregation() {
            return new InternalReverseNested(name, 0, buildEmptySubAggregations(), metadata());
        }
    }
}
