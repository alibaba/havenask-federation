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

package org.havenask.search.aggregations.bucket.geogrid;

import org.havenask.common.geo.GeoBoundingBox;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.xcontent.ObjectParser;
import org.havenask.index.query.QueryShardContext;
import org.havenask.search.aggregations.AggregationBuilder;
import org.havenask.search.aggregations.AggregatorFactories;
import org.havenask.search.aggregations.AggregatorFactory;
import org.havenask.search.aggregations.metrics.GeoGridAggregatorSupplier;
import org.havenask.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.havenask.search.aggregations.support.ValuesSourceConfig;
import org.havenask.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.Map;

public class GeoTileGridAggregationBuilder extends GeoGridAggregationBuilder {
    public static final String NAME = "geotile_grid";
    public static final int DEFAULT_PRECISION = 7;
    private static final int DEFAULT_MAX_NUM_CELLS = 10000;
    public static final ValuesSourceRegistry.RegistryKey<GeoGridAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        GeoGridAggregatorSupplier.class
    );

    public static final ObjectParser<GeoTileGridAggregationBuilder, String> PARSER =
        createParser(NAME, GeoTileUtils::parsePrecision, GeoTileGridAggregationBuilder::new);

    public GeoTileGridAggregationBuilder(String name) {
        super(name);
        precision(DEFAULT_PRECISION);
        size(DEFAULT_MAX_NUM_CELLS);
        shardSize = -1;
    }

    public GeoTileGridAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        GeoTileGridAggregatorFactory.registerAggregators(builder);
    }

    @Override
    public GeoGridAggregationBuilder precision(int precision) {
        this.precision = GeoTileUtils.checkPrecisionRange(precision);
        return this;
    }

    @Override
    protected ValuesSourceAggregatorFactory createFactory(
            String name, ValuesSourceConfig config, int precision, int requiredSize, int shardSize,
            GeoBoundingBox geoBoundingBox, QueryShardContext queryShardContext, AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metadata) throws IOException {
        return new GeoTileGridAggregatorFactory(name, config, precision, requiredSize, shardSize, geoBoundingBox,
            queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    private GeoTileGridAggregationBuilder(GeoTileGridAggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder,
                                          Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new GeoTileGridAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }
}
