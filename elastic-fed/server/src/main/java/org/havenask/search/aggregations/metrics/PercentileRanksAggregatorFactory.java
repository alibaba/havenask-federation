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
import org.havenask.search.aggregations.Aggregator;
import org.havenask.search.aggregations.AggregatorFactories;
import org.havenask.search.aggregations.AggregatorFactory;
import org.havenask.search.aggregations.CardinalityUpperBound;
import org.havenask.search.aggregations.support.CoreValuesSourceType;
import org.havenask.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.havenask.search.aggregations.support.ValuesSourceConfig;
import org.havenask.search.aggregations.support.ValuesSourceRegistry;
import org.havenask.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

class PercentileRanksAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final double[] percents;
    private final PercentilesConfig percentilesConfig;
    private final boolean keyed;

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            PercentileRanksAggregationBuilder.REGISTRY_KEY,
            Arrays.asList(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN),
            (name, valuesSource, context, parent, percents, percentilesConfig, keyed,  formatter,
                                          metadata) -> percentilesConfig
                .createPercentileRanksAggregator(name, valuesSource, context, parent, percents, keyed, formatter, metadata),
                true);
    }

    PercentileRanksAggregatorFactory(String name,
                                     ValuesSourceConfig config,
                                     double[] percents,
                                     PercentilesConfig percentilesConfig,
                                     boolean keyed,
                                     QueryShardContext queryShardContext,
                                     AggregatorFactory parent,
                                     AggregatorFactories.Builder subFactoriesBuilder,
                                     Map<String, Object> metadata) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.percents = percents;
        this.percentilesConfig = percentilesConfig;
        this.keyed = keyed;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                        Aggregator parent,
                                        Map<String, Object> metadata) throws IOException {

        return percentilesConfig.createPercentileRanksAggregator(name, null, searchContext, parent, percents, keyed,
                config.format(), metadata);
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(PercentileRanksAggregationBuilder.REGISTRY_KEY, config)
            .build(name, config.getValuesSource(), searchContext, parent, percents, percentilesConfig, keyed, config.format(), metadata);
    }
}
