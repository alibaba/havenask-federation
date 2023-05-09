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

import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.util.SetBackedScalingCuckooFilter;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.search.DocValueFormat;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

/**
 * Result of the RareTerms aggregation when the field is unmapped.
 */
public class UnmappedRareTerms extends InternalRareTerms<UnmappedRareTerms, UnmappedRareTerms.Bucket> {
    public static final String NAME = "umrareterms";

    protected abstract static class Bucket extends InternalRareTerms.Bucket<Bucket> {
        private Bucket(long docCount, InternalAggregations aggregations, DocValueFormat formatter) {
            super(docCount, aggregations, formatter);
        }
    }

    UnmappedRareTerms(String name, Map<String, Object> metadata) {
        super(name, LongRareTermsAggregator.ORDER, 0, metadata);
    }

    /**
     * Read from a stream.
     */
    public UnmappedRareTerms(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void writeTermTypeInfoTo(StreamOutput out) throws IOException {
        // Nothing to write
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getType() {
        return StringTerms.NAME;
    }

    @Override
    public UnmappedRareTerms create(List<UnmappedRareTerms.Bucket> buckets) {
        return new UnmappedRareTerms(name, metadata);
    }

    @Override
    public UnmappedRareTerms.Bucket createBucket(InternalAggregations aggregations, UnmappedRareTerms.Bucket prototype) {
        throw new UnsupportedOperationException("not supported for UnmappedRareTerms");
    }

    @Override
    UnmappedRareTerms.Bucket createBucket(long docCount, InternalAggregations aggs, Bucket prototype) {
        throw new UnsupportedOperationException("not supported for UnmappedRareTerms");
    }

    @Override
    protected UnmappedRareTerms createWithFilter(String name, List<UnmappedRareTerms.Bucket> buckets, SetBackedScalingCuckooFilter filter) {
        throw new UnsupportedOperationException("not supported for UnmappedRareTerms");
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        return new UnmappedRareTerms(name, metadata);
    }

    @Override
    public boolean isMapped() {
        return false;
    }

    @Override
    public final XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return doXContentCommon(builder, params, Collections.emptyList());
    }

    @Override
    public List<UnmappedRareTerms.Bucket> getBuckets() {
        return emptyList();
    }

    @Override
    public UnmappedRareTerms.Bucket getBucketByKey(String term) {
        return null;
    }

    @Override
    protected UnmappedRareTerms.Bucket[] createBucketsArray(int size) {
        return new UnmappedRareTerms.Bucket[size];
    }
}
