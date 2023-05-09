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

import org.apache.lucene.util.BytesRef;
import org.havenask.search.DocValueFormat;
import org.havenask.search.aggregations.InternalAggregations;
import org.havenask.search.aggregations.ParsedMultiBucketAggregation;
import org.havenask.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SignificantStringTermsTests extends InternalSignificantTermsTestCase {

    @Override
    protected InternalSignificantTerms createTestInstance(String name,
                                                          Map<String, Object> metadata,
                                                          InternalAggregations aggs,
                                                          int requiredSize, int numBuckets,
                                                          long subsetSize, int[] subsetDfs,
                                                          long supersetSize, int[] supersetDfs,
                                                          SignificanceHeuristic significanceHeuristic) {
        DocValueFormat format = DocValueFormat.RAW;
        List<SignificantStringTerms.Bucket> buckets = new ArrayList<>(numBuckets);
        Set<BytesRef> terms = new HashSet<>();
        for (int i = 0; i < numBuckets; ++i) {
            BytesRef term = randomValueOtherThanMany(b -> terms.add(b) == false, () -> new BytesRef(randomAlphaOfLength(10)));
            SignificantStringTerms.Bucket bucket = new SignificantStringTerms.Bucket(term, subsetDfs[i], subsetSize,
                    supersetDfs[i], supersetSize, aggs, format, 0);
            bucket.updateScore(significanceHeuristic);
            buckets.add(bucket);
        }
        return new SignificantStringTerms(name, requiredSize, 1L, metadata, format, subsetSize,
                supersetSize, significanceHeuristic, buckets);
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedSignificantStringTerms.class;
    }

    @Override
    protected InternalSignificantTerms<?, ?> mutateInstance(InternalSignificantTerms<?, ?> instance) {
        if (instance instanceof SignificantStringTerms) {
            SignificantStringTerms stringTerms = (SignificantStringTerms) instance;
            String name = stringTerms.getName();
            int requiredSize = stringTerms.requiredSize;
            long minDocCount = stringTerms.minDocCount;
            DocValueFormat format = stringTerms.format;
            long subsetSize = stringTerms.getSubsetSize();
            long supersetSize = stringTerms.getSupersetSize();
            List<SignificantStringTerms.Bucket> buckets = stringTerms.getBuckets();
            SignificanceHeuristic significanceHeuristic = stringTerms.significanceHeuristic;
            Map<String, Object> metadata = stringTerms.getMetadata();
            switch (between(0, 5)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                requiredSize += between(1, 100);
                break;
            case 2:
                minDocCount += between(1, 100);
                break;
            case 3:
                subsetSize += between(1, 100);
                break;
            case 4:
                supersetSize += between(1, 100);
                break;
            case 5:
                buckets = new ArrayList<>(buckets);
                buckets.add(new SignificantStringTerms.Bucket(new BytesRef(randomAlphaOfLengthBetween(1, 10)),
                        randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
                        InternalAggregations.EMPTY, format, 0));
                break;
            case 8:
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
            return new SignificantStringTerms(name, requiredSize, minDocCount, metadata, format, subsetSize,
                    supersetSize, significanceHeuristic, buckets);
        } else {
            String name = instance.getName();
            int requiredSize = instance.requiredSize;
            long minDocCount = instance.minDocCount;
            Map<String, Object> metadata = instance.getMetadata();
            switch (between(0, 3)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                requiredSize += between(1, 100);
                break;
            case 2:
                minDocCount += between(1, 100);
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
            return new UnmappedSignificantTerms(name, requiredSize, minDocCount, metadata);
        }
    }
}
