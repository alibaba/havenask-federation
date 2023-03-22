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

import org.havenask.common.ParsingException;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.rest.action.search.RestSearchAction;
import org.havenask.search.aggregations.Aggregation.CommonFields;
import org.havenask.search.aggregations.bucket.adjacency.InternalAdjacencyMatrixTests;
import org.havenask.search.aggregations.bucket.composite.InternalCompositeTests;
import org.havenask.search.aggregations.bucket.filter.InternalFilterTests;
import org.havenask.search.aggregations.bucket.filter.InternalFiltersTests;
import org.havenask.search.aggregations.bucket.geogrid.GeoHashGridTests;
import org.havenask.search.aggregations.bucket.geogrid.GeoTileGridTests;
import org.havenask.search.aggregations.bucket.global.InternalGlobalTests;
import org.havenask.search.aggregations.bucket.histogram.InternalAutoDateHistogramTests;
import org.havenask.search.aggregations.bucket.histogram.InternalDateHistogramTests;
import org.havenask.search.aggregations.bucket.histogram.InternalHistogramTests;
import org.havenask.search.aggregations.bucket.histogram.InternalVariableWidthHistogramTests;
import org.havenask.search.aggregations.bucket.missing.InternalMissingTests;
import org.havenask.search.aggregations.bucket.nested.InternalNestedTests;
import org.havenask.search.aggregations.bucket.nested.InternalReverseNestedTests;
import org.havenask.search.aggregations.bucket.range.InternalBinaryRangeTests;
import org.havenask.search.aggregations.bucket.range.InternalDateRangeTests;
import org.havenask.search.aggregations.bucket.range.InternalGeoDistanceTests;
import org.havenask.search.aggregations.bucket.range.InternalRangeTests;
import org.havenask.search.aggregations.bucket.sampler.InternalSamplerTests;
import org.havenask.search.aggregations.bucket.terms.DoubleTermsTests;
import org.havenask.search.aggregations.bucket.terms.LongRareTermsTests;
import org.havenask.search.aggregations.bucket.terms.LongTermsTests;
import org.havenask.search.aggregations.bucket.terms.SignificantLongTermsTests;
import org.havenask.search.aggregations.bucket.terms.SignificantStringTermsTests;
import org.havenask.search.aggregations.bucket.terms.StringRareTermsTests;
import org.havenask.search.aggregations.bucket.terms.StringTermsTests;
import org.havenask.search.aggregations.metrics.InternalExtendedStatsTests;
import org.havenask.search.aggregations.metrics.InternalMaxTests;
import org.havenask.search.aggregations.metrics.InternalMedianAbsoluteDeviationTests;
import org.havenask.search.aggregations.metrics.InternalMinTests;
import org.havenask.search.aggregations.metrics.InternalStatsBucketTests;
import org.havenask.search.aggregations.metrics.InternalStatsTests;
import org.havenask.search.aggregations.metrics.InternalSumTests;
import org.havenask.search.aggregations.metrics.InternalAvgTests;
import org.havenask.search.aggregations.metrics.InternalCardinalityTests;
import org.havenask.search.aggregations.metrics.InternalGeoBoundsTests;
import org.havenask.search.aggregations.metrics.InternalGeoCentroidTests;
import org.havenask.search.aggregations.metrics.InternalHDRPercentilesRanksTests;
import org.havenask.search.aggregations.metrics.InternalHDRPercentilesTests;
import org.havenask.search.aggregations.metrics.InternalTDigestPercentilesRanksTests;
import org.havenask.search.aggregations.metrics.InternalTDigestPercentilesTests;
import org.havenask.search.aggregations.metrics.InternalScriptedMetricTests;
import org.havenask.search.aggregations.metrics.InternalTopHitsTests;
import org.havenask.search.aggregations.metrics.InternalValueCountTests;
import org.havenask.search.aggregations.metrics.InternalWeightedAvgTests;
import org.havenask.search.aggregations.pipeline.InternalSimpleValueTests;
import org.havenask.search.aggregations.pipeline.InternalBucketMetricValueTests;
import org.havenask.search.aggregations.pipeline.InternalPercentilesBucketTests;
import org.havenask.search.aggregations.pipeline.InternalExtendedStatsBucketTests;
import org.havenask.search.aggregations.pipeline.InternalDerivativeTests;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.InternalAggregationTestCase;
import org.havenask.test.InternalMultiBucketAggregationTestCase;
import org.havenask.test.hamcrest.HavenaskAssertions;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.havenask.test.XContentTestUtils.insertRandomFields;

/**
 * This class tests that aggregations parsing works properly. It checks that we can parse
 * different aggregations and adds sub-aggregations where applicable.
 *
 */
public class AggregationsTests extends HavenaskTestCase {
    private static final List<InternalAggregationTestCase<?>> aggsTests = getAggsTests();

    private static List<InternalAggregationTestCase<?>> getAggsTests() {
        List<InternalAggregationTestCase<?>> aggsTests = new ArrayList<>();
        aggsTests.add(new InternalCardinalityTests());
        aggsTests.add(new InternalTDigestPercentilesTests());
        aggsTests.add(new InternalTDigestPercentilesRanksTests());
        aggsTests.add(new InternalHDRPercentilesTests());
        aggsTests.add(new InternalHDRPercentilesRanksTests());
        aggsTests.add(new InternalPercentilesBucketTests());
        aggsTests.add(new InternalMinTests());
        aggsTests.add(new InternalMaxTests());
        aggsTests.add(new InternalAvgTests());
        aggsTests.add(new InternalWeightedAvgTests());
        aggsTests.add(new InternalSumTests());
        aggsTests.add(new InternalValueCountTests());
        aggsTests.add(new InternalSimpleValueTests());
        aggsTests.add(new InternalDerivativeTests());
        aggsTests.add(new InternalBucketMetricValueTests());
        aggsTests.add(new InternalStatsTests());
        aggsTests.add(new InternalStatsBucketTests());
        aggsTests.add(new InternalExtendedStatsTests());
        aggsTests.add(new InternalExtendedStatsBucketTests());
        aggsTests.add(new InternalGeoBoundsTests());
        aggsTests.add(new InternalGeoCentroidTests());
        aggsTests.add(new InternalHistogramTests());
        aggsTests.add(new InternalDateHistogramTests());
        aggsTests.add(new InternalAutoDateHistogramTests());
        aggsTests.add(new InternalVariableWidthHistogramTests());
        aggsTests.add(new LongTermsTests());
        aggsTests.add(new DoubleTermsTests());
        aggsTests.add(new StringTermsTests());
        aggsTests.add(new LongRareTermsTests());
        aggsTests.add(new StringRareTermsTests());
        aggsTests.add(new InternalMissingTests());
        aggsTests.add(new InternalNestedTests());
        aggsTests.add(new InternalReverseNestedTests());
        aggsTests.add(new InternalGlobalTests());
        aggsTests.add(new InternalFilterTests());
        aggsTests.add(new InternalSamplerTests());
        aggsTests.add(new GeoHashGridTests());
        aggsTests.add(new GeoTileGridTests());
        aggsTests.add(new InternalRangeTests());
        aggsTests.add(new InternalDateRangeTests());
        aggsTests.add(new InternalGeoDistanceTests());
        aggsTests.add(new InternalFiltersTests());
        aggsTests.add(new InternalAdjacencyMatrixTests());
        aggsTests.add(new SignificantLongTermsTests());
        aggsTests.add(new SignificantStringTermsTests());
        aggsTests.add(new InternalScriptedMetricTests());
        aggsTests.add(new InternalBinaryRangeTests());
        aggsTests.add(new InternalTopHitsTests());
        aggsTests.add(new InternalCompositeTests());
        aggsTests.add(new InternalMedianAbsoluteDeviationTests());
        return Collections.unmodifiableList(aggsTests);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(InternalAggregationTestCase.getDefaultNamedXContents());
    }

    @Before
    public void init() throws Exception {
        for (InternalAggregationTestCase<?> aggsTest : aggsTests) {
            if (aggsTest instanceof InternalMultiBucketAggregationTestCase) {
                // Lower down the number of buckets generated by multi bucket aggregation tests in
                // order to avoid too many aggregations to be created.
                ((InternalMultiBucketAggregationTestCase<?>) aggsTest).setMaxNumberOfBuckets(3);
            }
            aggsTest.setUp();
        }
    }

    @After
    public void cleanUp() throws Exception {
        for (InternalAggregationTestCase<?> aggsTest : aggsTests) {
            aggsTest.tearDown();
        }
    }

    public void testAllAggsAreBeingTested() {
        assertEquals(InternalAggregationTestCase.getDefaultNamedXContents().size(), aggsTests.size());
        Set<String> aggs = aggsTests.stream().map((testCase) -> testCase.createTestInstance().getType()).collect(Collectors.toSet());
        for (NamedXContentRegistry.Entry entry : InternalAggregationTestCase.getDefaultNamedXContents()) {
            assertTrue(aggs.contains(entry.name.getPreferredName()));
        }
    }

    public void testFromXContent() throws IOException {
        parseAndAssert(false);
    }

    public void testFromXContentWithRandomFields() throws IOException {
        parseAndAssert(true);
    }

    /**
     * Test that parsing works for a randomly created Aggregations object with a
     * randomized aggregation tree. The test randomly chooses an
     * {@link XContentType}, randomizes the order of the {@link XContent} fields
     * and randomly sets the `humanReadable` flag when rendering the
     * {@link XContent}.
     *
     * @param addRandomFields
     *            if set, this will also add random {@link XContent} fields to
     *            tests that the parsers are lenient to future additions to rest
     *            responses
     */
    private void parseAndAssert(boolean addRandomFields) throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        Aggregations aggregations = createTestInstance();
        BytesReference originalBytes = toShuffledXContent(aggregations, xContentType, params, randomBoolean());
        BytesReference mutated;
        if (addRandomFields) {
            /*
             * - don't insert into the root object because it should only contain the named aggregations to test
             *
             * - don't insert into the "meta" object, because we pass on everything we find there
             *
             * - we don't want to directly insert anything random into "buckets"  objects, they are used with
             * "keyed" aggregations and contain named bucket objects. Any new named object on this level should
             * also be a bucket and be parsed as such.
             *
             * - we cannot insert randomly into VALUE or VALUES objects e.g. in Percentiles, the keys need to be numeric there
             *
             * - we cannot insert into ExtendedMatrixStats "covariance" or "correlation" fields, their syntax is strict
             *
             * - we cannot insert random values in top_hits, as all unknown fields
             * on a root level of SearchHit are interpreted as meta-fields and will be kept
             *
             * - exclude "key", it can be an array of objects and we need strict values
             */
            Predicate<String> excludes = path -> (path.isEmpty() || path.endsWith("aggregations")
                    || path.endsWith(Aggregation.CommonFields.META.getPreferredName())
                    || path.endsWith(Aggregation.CommonFields.BUCKETS.getPreferredName())
                    || path.endsWith(CommonFields.VALUES.getPreferredName()) || path.endsWith("covariance") || path.endsWith("correlation")
                    || path.contains(CommonFields.VALUE.getPreferredName())
                    || path.endsWith(CommonFields.KEY.getPreferredName()))
                    || path.contains("top_hits");
            mutated = insertRandomFields(xContentType, originalBytes, excludes, random());
        } else {
            mutated = originalBytes;
        }
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals(Aggregations.AGGREGATIONS_FIELD, parser.currentName());
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            Aggregations parsedAggregations = Aggregations.fromXContent(parser);
            BytesReference parsedBytes = XContentHelper.toXContent(parsedAggregations, xContentType, randomBoolean());
            HavenaskAssertions.assertToXContentEquivalent(originalBytes, parsedBytes, xContentType);
        }
    }

    public void testParsingExceptionOnUnknownAggregation() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("unknownAggregation");
            builder.endObject();
        }
        builder.endObject();
        BytesReference originalBytes = BytesReference.bytes(builder);
        try (XContentParser parser = createParser(builder.contentType().xContent(), originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            ParsingException ex = expectThrows(ParsingException.class, () -> Aggregations.fromXContent(parser));
            assertEquals("Could not parse aggregation keyed as [unknownAggregation]", ex.getMessage());
        }
    }

    public final InternalAggregations createTestInstance() {
        return createTestInstance(1, 0, 5);
    }

    private static InternalAggregations createTestInstance(final int minNumAggs, final int currentDepth, final int maxDepth) {
        int numAggs = randomIntBetween(minNumAggs, 4);
        List<InternalAggregation> aggs = new ArrayList<>(numAggs);
        for (int i = 0; i < numAggs; i++) {
            InternalAggregationTestCase<?> testCase = randomFrom(aggsTests);
            if (testCase instanceof InternalMultiBucketAggregationTestCase) {
                InternalMultiBucketAggregationTestCase<?> multiBucketAggTestCase = (InternalMultiBucketAggregationTestCase<?>) testCase;
                if (currentDepth < maxDepth) {
                    multiBucketAggTestCase.setSubAggregationsSupplier(
                        () -> createTestInstance(0, currentDepth + 1, maxDepth)
                    );
                } else {
                    multiBucketAggTestCase.setSubAggregationsSupplier(
                        () -> InternalAggregations.EMPTY
                    );
                }
            } else if (testCase instanceof InternalSingleBucketAggregationTestCase) {
                InternalSingleBucketAggregationTestCase<?> singleBucketAggTestCase = (InternalSingleBucketAggregationTestCase<?>) testCase;
                if (currentDepth < maxDepth) {
                    singleBucketAggTestCase.subAggregationsSupplier = () -> createTestInstance(0, currentDepth + 1, maxDepth);
                } else {
                    singleBucketAggTestCase.subAggregationsSupplier = () -> InternalAggregations.EMPTY;
                }
            }
            aggs.add(testCase.createTestInstanceForXContent());
        }
        return InternalAggregations.from(aggs);
    }
}
