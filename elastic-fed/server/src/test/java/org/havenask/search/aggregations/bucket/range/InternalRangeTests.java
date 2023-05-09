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

package org.havenask.search.aggregations.bucket.range;

import org.havenask.common.collect.Tuple;
import org.havenask.search.DocValueFormat;
import org.havenask.search.aggregations.InternalAggregations;
import org.havenask.search.aggregations.InternalMultiBucketAggregation;
import org.havenask.search.aggregations.ParsedMultiBucketAggregation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalRangeTests extends InternalRangeTestCase<InternalRange> {

    private DocValueFormat format;
    private List<Tuple<Double, Double>> ranges;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        format = randomNumericDocValueFormat();

        List<Tuple<Double, Double>> listOfRanges = new ArrayList<>();
        if (rarely()) {
            listOfRanges.add(Tuple.tuple(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
        }
        if (rarely()) {
            listOfRanges.add(Tuple.tuple(Double.NEGATIVE_INFINITY, randomDouble()));
        }
        if (rarely()) {
            listOfRanges.add(Tuple.tuple(randomDouble(), Double.POSITIVE_INFINITY));
        }

        final int interval = randomFrom(1, 5, 10, 25, 50, 100);
        final int numRanges = Math.max(0, randomNumberOfBuckets() - listOfRanges.size());
        final double max = (double) numRanges * interval;

        for (int i = 0; numRanges - listOfRanges.size() > 0; i++) {
            double from = i * interval;
            double to = from + interval;

            Tuple<Double, Double> range;
            if (randomBoolean()) {
                range = Tuple.tuple(from, to);
            } else {
                // Add some overlapping range
                range = Tuple.tuple(randomFrom(0.0, max / 3), randomFrom(max, max / 2, max / 3 * 2));
            }
            listOfRanges.add(range);
        }
        Collections.shuffle(listOfRanges, random());
        ranges = Collections.unmodifiableList(listOfRanges);
    }

    @Override
    protected InternalRange createTestInstance(String name,
                                               Map<String, Object> metadata,
                                               InternalAggregations aggregations,
                                               boolean keyed) {
        final List<InternalRange.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < ranges.size(); ++i) {
            Tuple<Double, Double> range = ranges.get(i);
            int docCount = randomIntBetween(0, 1000);
            double from = range.v1();
            double to = range.v2();
            buckets.add(new InternalRange.Bucket("range_" + i, from, to, docCount, aggregations, keyed, format));
        }
        return new InternalRange<>(name, buckets, format, keyed, metadata);
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedRange.class;
    }

    @Override
    protected Class<? extends InternalMultiBucketAggregation.InternalBucket> internalRangeBucketClass() {
        return InternalRange.Bucket.class;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation.ParsedBucket> parsedRangeBucketClass() {
        return ParsedRange.ParsedBucket.class;
    }

    @Override
    protected InternalRange mutateInstance(InternalRange instance) {
        String name = instance.getName();
        DocValueFormat format = instance.format;
        boolean keyed = instance.keyed;
        List<InternalRange.Bucket> buckets = instance.getBuckets();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            keyed = keyed == false;
            break;
        case 2:
            buckets = new ArrayList<>(buckets);
            double from = randomDouble();
            buckets.add(new InternalRange.Bucket("range_a", from, from + randomDouble(), randomNonNegativeLong(),
                    InternalAggregations.EMPTY, false, format));
            break;
        case 3:
            if (metadata == null) {
                metadata = new HashMap<>(1);
            } else {
                metadata = new HashMap<>(instance.getMetadata());
            }
            metadata.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalRange<>(name, buckets, format, keyed, metadata);
    }
}
