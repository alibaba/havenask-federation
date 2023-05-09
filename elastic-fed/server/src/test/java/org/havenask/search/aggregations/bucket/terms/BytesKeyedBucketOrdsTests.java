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
import org.havenask.common.settings.Settings;
import org.havenask.common.util.MockBigArrays;
import org.havenask.common.util.MockPageCacheRecycler;
import org.havenask.indices.breaker.NoneCircuitBreakerService;
import org.havenask.search.aggregations.CardinalityUpperBound;
import org.havenask.test.HavenaskTestCase;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class BytesKeyedBucketOrdsTests extends HavenaskTestCase {
    private static final BytesRef SHIP_1 = new BytesRef("Just Read The Instructions");
    private static final BytesRef SHIP_2 = new BytesRef("Of Course I Still Love You");

    private final MockBigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

    public void testExplicitCollectsFromSingleBucket() {
        collectsFromSingleBucketCase(BytesKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.ONE));
    }

    public void testSurpriseCollectsFromSingleBucket() {
        collectsFromSingleBucketCase(BytesKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.MANY));
    }

    private void collectsFromSingleBucketCase(BytesKeyedBucketOrds ords) {
        try {
            // Test a few explicit values
            assertThat(ords.add(0, SHIP_1), equalTo(0L));
            assertThat(ords.add(0, SHIP_2), equalTo(1L));
            assertThat(ords.add(0, SHIP_1), equalTo(-1L));
            assertThat(ords.add(0, SHIP_2), equalTo(-2L));

            // And some random values
            Set<BytesRef> seen = new HashSet<>();
            seen.add(SHIP_1);
            seen.add(SHIP_2);
            assertThat(ords.size(), equalTo(2L));
            BytesRef[] values = new BytesRef[scaledRandomIntBetween(1, 10000)];
            for (int i = 0; i < values.length; i++) {
                values[i] = randomValueOtherThanMany(seen::contains, () -> new BytesRef(Long.toString(randomLong())));
                seen.add(values[i]);
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.add(0, values[i]), equalTo(i + 2L));
                assertThat(ords.size(), equalTo(i + 3L));
                if (randomBoolean()) {
                    assertThat(ords.add(0, SHIP_1), equalTo(-1L));
                }
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.add(0, values[i]), equalTo(-1 - (i + 2L)));
            }

            // And the explicit values are still ok
            assertThat(ords.add(0, SHIP_1), equalTo(-1L));
            assertThat(ords.add(0, SHIP_2), equalTo(-2L));

            // Check counting values
            assertThat(ords.bucketsInOrd(0), equalTo(values.length + 2L));

            // Check iteration
            BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = ords.ordsEnum(0);
            BytesRef scratch = new BytesRef();
            assertTrue(ordsEnum.next());
            assertThat(ordsEnum.ord(), equalTo(0L));
            ordsEnum.readValue(scratch);
            assertThat(scratch, equalTo(SHIP_1));
            assertTrue(ordsEnum.next());
            assertThat(ordsEnum.ord(), equalTo(1L));
            ordsEnum.readValue(scratch);
            assertThat(scratch, equalTo(SHIP_2));
            for (int i = 0; i < values.length; i++) {
                assertTrue(ordsEnum.next());
                assertThat(ordsEnum.ord(), equalTo(i + 2L));
                ordsEnum.readValue(scratch);
                assertThat(scratch, equalTo(values[i]));
            }
            assertFalse(ordsEnum.next());
        } finally {
            ords.close();
        }
    }

    public void testCollectsFromManyBuckets() {
        try (BytesKeyedBucketOrds ords = BytesKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.MANY)) {
            // Test a few explicit values
            assertThat(ords.add(0, SHIP_1), equalTo(0L));
            assertThat(ords.add(1, SHIP_1), equalTo(1L));
            assertThat(ords.add(0, SHIP_1), equalTo(-1L));
            assertThat(ords.add(1, SHIP_1), equalTo(-2L));
            assertThat(ords.size(), equalTo(2L));

            // And some random values
            Set<OwningBucketOrdAndValue> seen = new HashSet<>();
            seen.add(new OwningBucketOrdAndValue(0, SHIP_1));
            seen.add(new OwningBucketOrdAndValue(1, SHIP_1));
            OwningBucketOrdAndValue[] values = new OwningBucketOrdAndValue[scaledRandomIntBetween(1, 10000)];
            long maxOwningBucketOrd = scaledRandomIntBetween(0, values.length);
            for (int i = 0; i < values.length; i++) {
                values[i] = randomValueOtherThanMany(seen::contains, () ->
                        new OwningBucketOrdAndValue(randomLongBetween(0, maxOwningBucketOrd), new BytesRef(Long.toString(randomLong()))));
                seen.add(values[i]);
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.add(values[i].owningBucketOrd, values[i].value), equalTo(i + 2L));
                assertThat(ords.size(), equalTo(i + 3L));
                if (randomBoolean()) {
                    assertThat(ords.add(0, SHIP_1), equalTo(-1L));
                }
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.add(values[i].owningBucketOrd, values[i].value), equalTo(-1 - (i + 2L)));
            }

            // And the explicit values are still ok
            assertThat(ords.add(0, SHIP_1), equalTo(-1L));
            assertThat(ords.add(1, SHIP_1), equalTo(-2L));

            BytesRef scratch = new BytesRef();
            for (long owningBucketOrd = 0; owningBucketOrd <= maxOwningBucketOrd; owningBucketOrd++) {
                long expectedCount = 0;
                BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = ords.ordsEnum(owningBucketOrd);
                if (owningBucketOrd <= 1) {
                    expectedCount++;
                    assertTrue(ordsEnum.next());
                    assertThat(ordsEnum.ord(), equalTo(owningBucketOrd));
                    ordsEnum.readValue(scratch);
                    assertThat(scratch, equalTo(SHIP_1));
                }
                for (int i = 0; i < values.length; i++) {
                    if (values[i].owningBucketOrd == owningBucketOrd) {
                        expectedCount++;
                        assertTrue(ordsEnum.next());
                        assertThat(ordsEnum.ord(), equalTo(i + 2L));
                        ordsEnum.readValue(scratch);
                        assertThat(scratch, equalTo(values[i].value));
                    }
                }
                assertFalse(ordsEnum.next());

                assertThat(ords.bucketsInOrd(owningBucketOrd), equalTo(expectedCount));
            }
            assertFalse(ords.ordsEnum(randomLongBetween(maxOwningBucketOrd + 1, Long.MAX_VALUE)).next());
            assertThat(ords.bucketsInOrd(randomLongBetween(maxOwningBucketOrd + 1, Long.MAX_VALUE)), equalTo(0L));
        }
    }

    private class OwningBucketOrdAndValue {
        private final long owningBucketOrd;
        private final BytesRef value;

        OwningBucketOrdAndValue(long owningBucketOrd, BytesRef value) {
            this.owningBucketOrd = owningBucketOrd;
            this.value = value;
        }

        @Override
        public String toString() {
            return owningBucketOrd + "/" + value;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            OwningBucketOrdAndValue other = (OwningBucketOrdAndValue) obj;
            return owningBucketOrd == other.owningBucketOrd && value == other.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(owningBucketOrd, value);
        }
    }
}
