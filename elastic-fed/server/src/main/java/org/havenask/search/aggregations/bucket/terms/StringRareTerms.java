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
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.util.SetBackedScalingCuckooFilter;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.search.DocValueFormat;
import org.havenask.search.aggregations.BucketOrder;
import org.havenask.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StringRareTerms extends InternalMappedRareTerms<StringRareTerms, StringRareTerms.Bucket> {
    public static final String NAME = "srareterms";

    public static class Bucket extends InternalRareTerms.Bucket<Bucket> {
        BytesRef termBytes;

        public Bucket(BytesRef term, long docCount, InternalAggregations aggregations, DocValueFormat format) {
            super(docCount, aggregations, format);
            this.termBytes = term;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, DocValueFormat format) throws IOException {
            super(in, format);
            termBytes = in.readBytesRef();
        }

        @Override
        protected void writeTermTo(StreamOutput out) throws IOException {
            out.writeBytesRef(termBytes);
        }

        @Override
        public Object getKey() {
            return getKeyAsString();
        }

        // this method is needed for scripted numeric aggs
        @Override
        public Number getKeyAsNumber() {
            /*
             * If the term is a long greater than 2^52 then parsing as a double would lose accuracy. Therefore, we first parse as a long and
             * if this fails then we attempt to parse the term as a double.
             */
            try {
                return Long.parseLong(termBytes.utf8ToString());
            } catch (final NumberFormatException ignored) {
                return Double.parseDouble(termBytes.utf8ToString());
            }
        }

        @Override
        public String getKeyAsString() {
            return format.format(termBytes).toString();
        }

        @Override
        public int compareKey(Bucket other) {
            return termBytes.compareTo(other.termBytes);
        }

        @Override
        protected final XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), getKeyAsString());
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj) && Objects.equals(termBytes, ((Bucket) obj).termBytes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), termBytes);
        }
    }

    StringRareTerms(String name, BucketOrder order, Map<String, Object> metadata, DocValueFormat format,
                           List<StringRareTerms.Bucket> buckets, long maxDocCount, SetBackedScalingCuckooFilter filter) {
        super(name, order, metadata, format, buckets, maxDocCount, filter);
    }

    /**
     * Read from a stream.
     */
    public StringRareTerms(StreamInput in) throws IOException {
        super(in, StringRareTerms.Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public StringRareTerms create(List<StringRareTerms.Bucket> buckets) {
        return new StringRareTerms(name, order, metadata, format, buckets, maxDocCount, filter);
    }

    @Override
    public StringRareTerms.Bucket createBucket(InternalAggregations aggregations, StringRareTerms.Bucket prototype) {
        return new StringRareTerms.Bucket(prototype.termBytes, prototype.getDocCount(), aggregations, prototype.format);
    }

    @Override
    protected StringRareTerms createWithFilter(String name, List<StringRareTerms.Bucket> buckets,
                                               SetBackedScalingCuckooFilter filterFilter) {
        return new StringRareTerms(name, order, metadata, format, buckets, maxDocCount, filterFilter);
    }

    @Override
    protected StringRareTerms.Bucket[] createBucketsArray(int size) {
        return new StringRareTerms.Bucket[size];
    }

    @Override
    public boolean containsTerm(SetBackedScalingCuckooFilter filter, StringRareTerms.Bucket bucket) {
        return filter.mightContain(bucket.termBytes);
    }

    @Override
    public void addToFilter(SetBackedScalingCuckooFilter filter, StringRareTerms.Bucket bucket) {
        filter.add(bucket.termBytes);
    }

    @Override
    Bucket createBucket(long docCount, InternalAggregations aggs, StringRareTerms.Bucket prototype) {
        return new Bucket(prototype.termBytes, docCount, aggs, format);
    }

}
