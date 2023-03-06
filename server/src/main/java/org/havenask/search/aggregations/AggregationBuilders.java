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

package org.havenask.search.aggregations;

import org.havenask.common.geo.GeoDistance;
import org.havenask.common.geo.GeoPoint;
import org.havenask.index.query.QueryBuilder;
import org.havenask.search.aggregations.bucket.adjacency.AdjacencyMatrix;
import org.havenask.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;
import org.havenask.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.havenask.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.havenask.search.aggregations.bucket.filter.Filter;
import org.havenask.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.havenask.search.aggregations.bucket.filter.Filters;
import org.havenask.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.havenask.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.havenask.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.havenask.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.havenask.search.aggregations.bucket.geogrid.InternalGeoHashGrid;
import org.havenask.search.aggregations.bucket.geogrid.InternalGeoTileGrid;
import org.havenask.search.aggregations.bucket.global.Global;
import org.havenask.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.havenask.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.havenask.search.aggregations.bucket.histogram.Histogram;
import org.havenask.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.havenask.search.aggregations.bucket.missing.Missing;
import org.havenask.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.havenask.search.aggregations.bucket.nested.Nested;
import org.havenask.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.havenask.search.aggregations.bucket.nested.ReverseNested;
import org.havenask.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.havenask.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.havenask.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.havenask.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.havenask.search.aggregations.bucket.range.Range;
import org.havenask.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.havenask.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.havenask.search.aggregations.bucket.sampler.Sampler;
import org.havenask.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
import org.havenask.search.aggregations.bucket.terms.SignificantTerms;
import org.havenask.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.havenask.search.aggregations.bucket.terms.SignificantTextAggregationBuilder;
import org.havenask.search.aggregations.bucket.terms.Terms;
import org.havenask.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.havenask.search.aggregations.metrics.Avg;
import org.havenask.search.aggregations.metrics.AvgAggregationBuilder;
import org.havenask.search.aggregations.metrics.Cardinality;
import org.havenask.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.havenask.search.aggregations.metrics.ExtendedStats;
import org.havenask.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.havenask.search.aggregations.metrics.GeoBounds;
import org.havenask.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.havenask.search.aggregations.metrics.GeoCentroid;
import org.havenask.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.havenask.search.aggregations.metrics.Max;
import org.havenask.search.aggregations.metrics.MaxAggregationBuilder;
import org.havenask.search.aggregations.metrics.MedianAbsoluteDeviation;
import org.havenask.search.aggregations.metrics.MedianAbsoluteDeviationAggregationBuilder;
import org.havenask.search.aggregations.metrics.Min;
import org.havenask.search.aggregations.metrics.MinAggregationBuilder;
import org.havenask.search.aggregations.metrics.PercentileRanks;
import org.havenask.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.havenask.search.aggregations.metrics.Percentiles;
import org.havenask.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.havenask.search.aggregations.metrics.ScriptedMetric;
import org.havenask.search.aggregations.metrics.ScriptedMetricAggregationBuilder;
import org.havenask.search.aggregations.metrics.Stats;
import org.havenask.search.aggregations.metrics.StatsAggregationBuilder;
import org.havenask.search.aggregations.metrics.Sum;
import org.havenask.search.aggregations.metrics.SumAggregationBuilder;
import org.havenask.search.aggregations.metrics.TopHits;
import org.havenask.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.havenask.search.aggregations.metrics.ValueCount;
import org.havenask.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.havenask.search.aggregations.metrics.WeightedAvgAggregationBuilder;

import java.util.List;
import java.util.Map;

/**
 * Utility class to create aggregations.
 */
public class AggregationBuilders {

    private AggregationBuilders() {
    }

    /**
     * Create a new {@link ValueCount} aggregation with the given name.
     */
    public static ValueCountAggregationBuilder count(String name) {
        return new ValueCountAggregationBuilder(name);
    }

    /**
     * Create a new {@link Avg} aggregation with the given name.
     */
    public static AvgAggregationBuilder avg(String name) {
        return new AvgAggregationBuilder(name);
    }

    /**
     * Create a new {@link Avg} aggregation with the given name.
     */
    public static WeightedAvgAggregationBuilder weightedAvg(String name) {
        return new WeightedAvgAggregationBuilder(name);
    }

    /**
     * Create a new {@link Max} aggregation with the given name.
     */
    public static MaxAggregationBuilder max(String name) {
        return new MaxAggregationBuilder(name);
    }

    /**
     * Create a new {@link Min} aggregation with the given name.
     */
    public static MinAggregationBuilder min(String name) {
        return new MinAggregationBuilder(name);
    }

    /**
     * Create a new {@link Sum} aggregation with the given name.
     */
    public static SumAggregationBuilder sum(String name) {
        return new SumAggregationBuilder(name);
    }

    /**
     * Create a new {@link Stats} aggregation with the given name.
     */
    public static StatsAggregationBuilder stats(String name) {
        return new StatsAggregationBuilder(name);
    }

    /**
     * Create a new {@link ExtendedStats} aggregation with the given name.
     */
    public static ExtendedStatsAggregationBuilder extendedStats(String name) {
        return new ExtendedStatsAggregationBuilder(name);
    }

    /**
     * Create a new {@link Filter} aggregation with the given name.
     */
    public static FilterAggregationBuilder filter(String name, QueryBuilder filter) {
        return new FilterAggregationBuilder(name, filter);
    }

    /**
     * Create a new {@link Filters} aggregation with the given name.
     */
    public static FiltersAggregationBuilder filters(String name, KeyedFilter... filters) {
        return new FiltersAggregationBuilder(name, filters);
    }

    /**
     * Create a new {@link Filters} aggregation with the given name.
     */
    public static FiltersAggregationBuilder filters(String name, QueryBuilder... filters) {
        return new FiltersAggregationBuilder(name, filters);
    }

    /**
     * Create a new {@link AdjacencyMatrix} aggregation with the given name.
     */
    public static AdjacencyMatrixAggregationBuilder adjacencyMatrix(String name, Map<String, QueryBuilder> filters) {
        return new AdjacencyMatrixAggregationBuilder(name, filters);
    }

    /**
     * Create a new {@link AdjacencyMatrix} aggregation with the given name and separator
     */
    public static AdjacencyMatrixAggregationBuilder adjacencyMatrix(String name, String separator,  Map<String, QueryBuilder> filters) {
        return new AdjacencyMatrixAggregationBuilder(name, separator, filters);
    }

    /**
     * Create a new {@link Sampler} aggregation with the given name.
     */
    public static SamplerAggregationBuilder sampler(String name) {
        return new SamplerAggregationBuilder(name);
    }

    /**
     * Create a new {@link Sampler} aggregation with the given name.
     */
    public static DiversifiedAggregationBuilder diversifiedSampler(String name) {
        return new DiversifiedAggregationBuilder(name);
    }

    /**
     * Create a new {@link Global} aggregation with the given name.
     */
    public static GlobalAggregationBuilder global(String name) {
        return new GlobalAggregationBuilder(name);
    }

    /**
     * Create a new {@link Missing} aggregation with the given name.
     */
    public static MissingAggregationBuilder missing(String name) {
        return new MissingAggregationBuilder(name);
    }

    /**
     * Create a new {@link Nested} aggregation with the given name.
     */
    public static NestedAggregationBuilder nested(String name, String path) {
        return new NestedAggregationBuilder(name, path);
    }

    /**
     * Create a new {@link ReverseNested} aggregation with the given name.
     */
    public static ReverseNestedAggregationBuilder reverseNested(String name) {
        return new ReverseNestedAggregationBuilder(name);
    }

    /**
     * Create a new {@link GeoDistance} aggregation with the given name.
     */
    public static GeoDistanceAggregationBuilder geoDistance(String name, GeoPoint origin) {
        return new GeoDistanceAggregationBuilder(name, origin);
    }

    /**
     * Create a new {@link Histogram} aggregation with the given name.
     */
    public static HistogramAggregationBuilder histogram(String name) {
        return new HistogramAggregationBuilder(name);
    }

    /**
     * Create a new {@link InternalGeoHashGrid} aggregation with the given name.
     */
    public static GeoHashGridAggregationBuilder geohashGrid(String name) {
        return new GeoHashGridAggregationBuilder(name);
    }

    /**
     * Create a new {@link InternalGeoTileGrid} aggregation with the given name.
     */
    public static GeoTileGridAggregationBuilder geotileGrid(String name) {
        return new GeoTileGridAggregationBuilder(name);
    }

    /**
     * Create a new {@link SignificantTerms} aggregation with the given name.
     */
    public static SignificantTermsAggregationBuilder significantTerms(String name) {
        return new SignificantTermsAggregationBuilder(name);
    }


    /**
     * Create a new {@link SignificantTextAggregationBuilder} aggregation with the given name and text field name
     */
    public static SignificantTextAggregationBuilder significantText(String name, String fieldName) {
        return new SignificantTextAggregationBuilder(name, fieldName);
    }


    /**
     * Create a new {@link DateHistogramAggregationBuilder} aggregation with the given
     * name.
     */
    public static DateHistogramAggregationBuilder dateHistogram(String name) {
        return new DateHistogramAggregationBuilder(name);
    }

    /**
     * Create a new {@link Range} aggregation with the given name.
     */
    public static RangeAggregationBuilder range(String name) {
        return new RangeAggregationBuilder(name);
    }

    /**
     * Create a new {@link DateRangeAggregationBuilder} aggregation with the
     * given name.
     */
    public static DateRangeAggregationBuilder dateRange(String name) {
        return new DateRangeAggregationBuilder(name);
    }

    /**
     * Create a new {@link IpRangeAggregationBuilder} aggregation with the
     * given name.
     */
    public static IpRangeAggregationBuilder ipRange(String name) {
        return new IpRangeAggregationBuilder(name);
    }

    /**
     * Create a new {@link Terms} aggregation with the given name.
     */
    public static TermsAggregationBuilder terms(String name) {
        return new TermsAggregationBuilder(name);
    }

    /**
     * Create a new {@link Percentiles} aggregation with the given name.
     */
    public static PercentilesAggregationBuilder percentiles(String name) {
        return new PercentilesAggregationBuilder(name);
    }

    /**
     * Create a new {@link PercentileRanks} aggregation with the given name.
     */
    public static PercentileRanksAggregationBuilder percentileRanks(String name, double[] values) {
        return new PercentileRanksAggregationBuilder(name, values);
    }

    /**
     * Create a new {@link MedianAbsoluteDeviation} aggregation with the given name
     */
    public static MedianAbsoluteDeviationAggregationBuilder medianAbsoluteDeviation(String name) {
        return new MedianAbsoluteDeviationAggregationBuilder(name);
    }

    /**
     * Create a new {@link Cardinality} aggregation with the given name.
     */
    public static CardinalityAggregationBuilder cardinality(String name) {
        return new CardinalityAggregationBuilder(name);
    }

    /**
     * Create a new {@link TopHits} aggregation with the given name.
     */
    public static TopHitsAggregationBuilder topHits(String name) {
        return new TopHitsAggregationBuilder(name);
    }

    /**
     * Create a new {@link GeoBounds} aggregation with the given name.
     */
    public static GeoBoundsAggregationBuilder geoBounds(String name) {
        return new GeoBoundsAggregationBuilder(name);
    }

    /**
     * Create a new {@link GeoCentroid} aggregation with the given name.
     */
    public static GeoCentroidAggregationBuilder geoCentroid(String name) {
        return new GeoCentroidAggregationBuilder(name);
    }

    /**
     * Create a new {@link ScriptedMetric} aggregation with the given name.
     */
    public static ScriptedMetricAggregationBuilder scriptedMetric(String name) {
        return new ScriptedMetricAggregationBuilder(name);
    }

    /**
     * Create a new {@link CompositeAggregationBuilder} aggregation with the given name.
     */
    public static CompositeAggregationBuilder composite(String name, List<CompositeValuesSourceBuilder<?>> sources) {
        return new CompositeAggregationBuilder(name, sources);
    }
}
