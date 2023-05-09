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

package org.havenask.search.aggregations.metrics;

import org.havenask.index.query.QueryShardContext;
import org.havenask.search.DocValueFormat;
import org.havenask.search.aggregations.Aggregator;
import org.havenask.search.aggregations.AggregatorFactories;
import org.havenask.search.aggregations.AggregatorFactory;
import org.havenask.search.aggregations.CardinalityUpperBound;
import org.havenask.search.aggregations.support.MultiValuesSource;
import org.havenask.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.havenask.search.aggregations.support.ValuesSourceConfig;
import org.havenask.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

import static org.havenask.search.aggregations.metrics.WeightedAvgAggregationBuilder.VALUE_FIELD;

class WeightedAvgAggregatorFactory extends MultiValuesSourceAggregatorFactory {

    WeightedAvgAggregatorFactory(String name, Map<String, ValuesSourceConfig> configs,
                                 DocValueFormat format, QueryShardContext queryShardContext, AggregatorFactory parent,
                                 AggregatorFactories.Builder subFactoriesBuilder,
                                 Map<String, Object> metadata) throws IOException {
        super(name, configs, format, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            Map<String, Object> metadata) throws IOException {
        return new WeightedAvgAggregator(name, null, format, searchContext, parent, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext,
                                            Map<String, ValuesSourceConfig> configs,
                                            DocValueFormat format,
                                            Aggregator parent,
                                            CardinalityUpperBound cardinality,
                                            Map<String, Object> metadata) throws IOException {
        MultiValuesSource.NumericMultiValuesSource numericMultiVS
            = new MultiValuesSource.NumericMultiValuesSource(configs, queryShardContext);
        if (numericMultiVS.areValuesSourcesEmpty()) {
            return createUnmapped(searchContext, parent, metadata);
        }
        return new WeightedAvgAggregator(name, numericMultiVS, format, searchContext, parent, metadata);
    }

    @Override
    public String getStatsSubtype() {
        return configs.get(VALUE_FIELD.getPreferredName()).valueSourceType().typeName();
    }
}
