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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.lease.Releasable;
import org.havenask.common.util.BigArrays;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Base class for HLL++ algorithms.
 *
 * It contains methods for cloning and serializing the data structure.
 */
public abstract class AbstractHyperLogLogPlusPlus extends AbstractCardinalityAlgorithm implements Releasable {

    public static final boolean LINEAR_COUNTING = false;
    public static final boolean HYPERLOGLOG = true;

    public AbstractHyperLogLogPlusPlus(int precision) {
        super(precision);
    }

    /** Algorithm used in the given bucket */
    protected abstract boolean getAlgorithm(long bucketOrd);

    /** Get linear counting algorithm */
    protected abstract AbstractLinearCounting.HashesIterator getLinearCounting(long bucketOrd);

    /** Get HyperLogLog algorithm */
    protected abstract AbstractHyperLogLog.RunLenIterator getHyperLogLog(long bucketOrd);

    /** Get the number of data structures */
    public abstract long maxOrd();

    /** Collect a value in the given bucket */
    public abstract void collect(long bucketOrd, long hash);


    /** Clone the data structure at the given bucket */
    public AbstractHyperLogLogPlusPlus clone(long bucketOrd, BigArrays bigArrays) {
        if (getAlgorithm(bucketOrd) == LINEAR_COUNTING) {
            // we use a sparse structure for linear counting
            AbstractLinearCounting.HashesIterator iterator = getLinearCounting(bucketOrd);
            int size = Math.toIntExact(iterator.size());
            HyperLogLogPlusPlusSparse clone = new HyperLogLogPlusPlusSparse(precision(), bigArrays, size, 1);
            while (iterator.next()) {
                clone.addEncoded(0, iterator.value());
            }
            return clone;
        } else {
            HyperLogLogPlusPlus clone = new HyperLogLogPlusPlus(precision(), bigArrays, 1);
            clone.merge(0, this, bucketOrd);
            return clone;
        }
    }

    private Object getComparableData(long bucketOrd) {
        if (getAlgorithm(bucketOrd) == LINEAR_COUNTING) {
            Set<Integer> values = new HashSet<>();
            AbstractLinearCounting.HashesIterator iteratorValues = getLinearCounting(bucketOrd);
            while (iteratorValues.next()) {
                values.add(iteratorValues.value());
            }
            return values;
        } else {
            Map<Byte, Integer> values = new HashMap<>();
            AbstractHyperLogLog.RunLenIterator iterator = getHyperLogLog(bucketOrd);
            while (iterator.next()) {
                byte runLength = iterator.value();
                Integer numOccurances = values.get(runLength);
                if (numOccurances == null) {
                    values.put(runLength, 1);
                } else {
                    values.put(runLength, numOccurances + 1);
                }
            }
            return values;
        }
    }

    public void writeTo(long bucket, StreamOutput out) throws IOException {
        out.writeVInt(precision());
        if (getAlgorithm(bucket) == LINEAR_COUNTING) {
            out.writeBoolean(LINEAR_COUNTING);
            AbstractLinearCounting.HashesIterator hashes = getLinearCounting(bucket);
            out.writeVLong(hashes.size());
            while (hashes.next()) {
                out.writeInt(hashes.value());
            }
        } else {
            out.writeBoolean(HYPERLOGLOG);
            AbstractHyperLogLog.RunLenIterator iterator = getHyperLogLog(bucket);
            while (iterator.next()){
                out.writeByte(iterator.value());
            }
        }
    }

    public static AbstractHyperLogLogPlusPlus readFrom(StreamInput in, BigArrays bigArrays) throws IOException {
        final int precision = in.readVInt();
        final boolean algorithm = in.readBoolean();
        if (algorithm == LINEAR_COUNTING) {
            // we use a sparse structure for linear counting
            final long size = in.readVLong();
            HyperLogLogPlusPlusSparse counts  = new HyperLogLogPlusPlusSparse(precision, bigArrays, Math.toIntExact(size), 1);
            for (long i = 0; i < size; ++i) {
                counts.addEncoded(0, in.readInt());
            }
            return counts;
        } else {
            HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(precision, bigArrays, 1);
            final int registers = 1 << precision;
            for (int i = 0; i < registers; ++i) {
                counts.addRunLen(0, i, in.readByte());
            }
            return counts;
        }
    }

    public boolean equals(long thisBucket, AbstractHyperLogLogPlusPlus other, long otherBucket) {
        return Objects.equals(precision(), other.precision())
            && Objects.equals(getAlgorithm(thisBucket), other.getAlgorithm(otherBucket))
            && Objects.equals(getComparableData(thisBucket), other.getComparableData(otherBucket));
    }

    public int hashCode(long bucket) {
        return Objects.hash(precision(), getAlgorithm(bucket), getComparableData(bucket));
    }
}
