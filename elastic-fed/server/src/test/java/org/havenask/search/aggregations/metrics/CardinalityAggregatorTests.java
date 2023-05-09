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

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.havenask.common.CheckedConsumer;
import org.havenask.common.geo.GeoPoint;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.mapper.NumberFieldMapper;
import org.havenask.index.mapper.RangeFieldMapper;
import org.havenask.index.mapper.RangeType;
import org.havenask.search.aggregations.AggregationBuilder;
import org.havenask.search.aggregations.AggregatorTestCase;
import org.havenask.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Collections.singleton;

public class CardinalityAggregatorTests extends AggregatorTestCase {

    public void testNoDocs() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, card -> {
            assertEquals(0.0, card.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testRangeFieldValues() throws IOException {
        RangeType rangeType = RangeType.DOUBLE;
        final RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true);
        final RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(rangeType, 6.0D, 10.0D, true, true);
        final String fieldName = "rangeField";
        MappedFieldType fieldType = new RangeFieldMapper.RangeFieldType(fieldName, rangeType);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field(fieldName);
        Set<RangeFieldMapper.Range> multiRecord = new HashSet<>(2);
        multiRecord.add(range1);
        multiRecord.add(range2);
        testAggregation(aggregationBuilder,  new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range1)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range1)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range2)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(multiRecord))));
        }, card -> {
            assertEquals(3.0, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, fieldType);
    }

    public void testNoMatchingField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
        }, card -> {
            assertEquals(0.0, card.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testAggregation(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(2, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testAggregation(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(2, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testQueryFiltering() throws IOException {
        testAggregation(IntPoint.newRangeQuery("number", 0, 5), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7),
                    new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1),
                    new SortedNumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testQueryFiltersAll() throws IOException {
        testAggregation(IntPoint.newRangeQuery("number", -1, 0), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7),
                    new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1),
                    new SortedNumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(0.0, card.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testUnmappedMissingString() throws IOException {
        CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name")
            .field("number").missing("🍌🍌🍌");

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 9)));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, null);
    }

    public void testUnmappedMissingNumber() throws IOException {
        CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name")
            .field("number").missing(1234);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 9)));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, null);
    }

    public void testUnmappedMissingGeoPoint() throws IOException {
        CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name")
            .field("number").missing(new GeoPoint(42.39561, -71.13051));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 9)));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, null);
    }

    private void testAggregation(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalCardinality> verify) throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field("number");
        testAggregation(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void testAggregation(AggregationBuilder aggregationBuilder, Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalCardinality> verify,
        MappedFieldType fieldType) throws IOException {
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }
}
