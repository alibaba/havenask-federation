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

import com.carrotsearch.hppc.BitMixer;
import org.havenask.common.breaker.CircuitBreaker;
import org.havenask.common.breaker.CircuitBreakingException;
import org.havenask.common.breaker.NoopCircuitBreaker;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.util.BigArrays;
import org.havenask.indices.breaker.CircuitBreakerService;
import org.havenask.test.HavenaskTestCase;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.havenask.search.aggregations.metrics.AbstractHyperLogLog.MAX_PRECISION;
import static org.havenask.search.aggregations.metrics.AbstractHyperLogLog.MIN_PRECISION;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HyperLogLogPlusPlusSparseTests extends HavenaskTestCase {

    public void testBasic()  {
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        HyperLogLogPlusPlusSparse sparse  = new HyperLogLogPlusPlusSparse(p, BigArrays.NON_RECYCLING_INSTANCE, 10, 1);
        AbstractLinearCounting.HashesIterator iterator = sparse.getLinearCounting(randomIntBetween(1, 10));
        assertEquals(0, iterator.size());
        IllegalArgumentException ex =
            expectThrows(IllegalArgumentException.class, () -> sparse.getHyperLogLog(randomIntBetween(1, 10)));
        assertThat(ex.getMessage(), Matchers.containsString("Implementation does not support HLL structures"));
    }

    public void testEquivalence() throws IOException {
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final HyperLogLogPlusPlus single = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);
        final int numBuckets = randomIntBetween(2, 100);
        final int numValues = randomIntBetween(1, 100000);
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000: 1000000);
        for (int i = 0; i < numValues; ++i) {
            final int n = randomInt(maxValue);
            final long hash = BitMixer.mix64(n);
            single.collect(randomInt(numBuckets), hash);
        }
        for (int i = 0; i < numBuckets; i++) {
            // test clone
            AbstractHyperLogLogPlusPlus clone = single.clone(i, BigArrays.NON_RECYCLING_INSTANCE);
            if (single.getAlgorithm(i) == AbstractHyperLogLogPlusPlus.LINEAR_COUNTING) {
                assertTrue(clone instanceof HyperLogLogPlusPlusSparse);
            } else {
                assertTrue(clone instanceof HyperLogLogPlusPlus);
            }
            checkEquivalence(single, i, clone, 0);
            // test serialize
            BytesStreamOutput out = new BytesStreamOutput();
            single.writeTo(i, out);
            clone = AbstractHyperLogLogPlusPlus.readFrom(out.bytes().streamInput(), BigArrays.NON_RECYCLING_INSTANCE);
            if (single.getAlgorithm(i) == AbstractHyperLogLogPlusPlus.LINEAR_COUNTING) {
                assertTrue(clone instanceof HyperLogLogPlusPlusSparse);
            } else {
                assertTrue(clone instanceof HyperLogLogPlusPlus);
            }
            checkEquivalence(single, i, clone, 0);
            // test merge
            final HyperLogLogPlusPlus merge = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);
            merge.merge(0, clone, 0);
            checkEquivalence(merge, 0, clone, 0);
        }
    }

    private void checkEquivalence(AbstractHyperLogLogPlusPlus first, int firstBucket,
                                  AbstractHyperLogLogPlusPlus second, int secondBucket) {
        assertEquals(first.hashCode(firstBucket), second.hashCode(secondBucket));
        assertEquals(first.cardinality(firstBucket), second.cardinality(0));
        assertTrue(first.equals(firstBucket, second, secondBucket));
        assertTrue(second.equals(secondBucket, first, firstBucket));
    }

    public void testCircuitBreakerOnConstruction() {
        int whenToBreak = randomInt(10);
        AtomicLong total = new AtomicLong();
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(new NoopCircuitBreaker(CircuitBreaker.REQUEST) {
            private int countDown = whenToBreak;
            @Override
            public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                if (countDown-- == 0) {
                    throw new CircuitBreakingException("test error", bytes, Long.MAX_VALUE, Durability.TRANSIENT);
                }
                total.addAndGet(bytes);
                return total.get();
            }

            @Override
            public long addWithoutBreaking(long bytes) {
                total.addAndGet(bytes);
                return total.get();
            }
        });
        BigArrays bigArrays = new BigArrays(null, breakerService, CircuitBreaker.REQUEST).withCircuitBreaking();
        final int p = randomIntBetween(AbstractCardinalityAlgorithm.MIN_PRECISION, AbstractCardinalityAlgorithm.MAX_PRECISION);
        try {
            for (int i = 0; i < whenToBreak + 1; ++i) {
                final HyperLogLogPlusPlusSparse subject = new HyperLogLogPlusPlusSparse(p, bigArrays, 1, 1);
                subject.close();
            }
            fail("Must fail");
        } catch (CircuitBreakingException e) {
            // OK
        }

        assertThat(total.get(), CoreMatchers.equalTo(0L));
    }

}
