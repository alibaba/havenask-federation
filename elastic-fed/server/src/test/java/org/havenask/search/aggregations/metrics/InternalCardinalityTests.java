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
import org.havenask.common.lease.Releasables;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.MockBigArrays;
import org.havenask.common.util.MockPageCacheRecycler;
import org.havenask.indices.breaker.NoneCircuitBreakerService;
import org.havenask.search.aggregations.ParsedAggregation;
import org.havenask.test.InternalAggregationTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalCardinalityTests extends InternalAggregationTestCase<InternalCardinality> {
    private static List<HyperLogLogPlusPlus> algos;
    private static int p;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        algos = new ArrayList<>();
        p = randomIntBetween(AbstractHyperLogLog.MIN_PRECISION, AbstractHyperLogLog.MAX_PRECISION);
    }

    @After //we force @After to have it run before HavenaskTestCase#after otherwise it fails
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Releasables.close(algos);
        algos.clear();
        algos = null;
    }

    @Override
    protected InternalCardinality createTestInstance(String name, Map<String, Object> metadata) {
        HyperLogLogPlusPlus hllpp = new HyperLogLogPlusPlus(p,
                new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService()), 1);
        algos.add(hllpp);
        for (int i = 0; i < 100; i++) {
            hllpp.collect(0, BitMixer.mix64(randomIntBetween(1, 100)));
        }
        return new InternalCardinality(name, hllpp, metadata);
    }

    @Override
    protected void assertReduced(InternalCardinality reduced, List<InternalCardinality> inputs) {
        HyperLogLogPlusPlus[] algos = inputs.stream().map(InternalCardinality::getState)
                .toArray(size -> new HyperLogLogPlusPlus[size]);
        if (algos.length > 0) {
            HyperLogLogPlusPlus result = algos[0];
            for (int i = 1; i < algos.length; i++) {
                result.merge(0, algos[i], 0);
            }
            assertEquals(result.cardinality(0), reduced.value(), 0);
        }
    }

    @Override
    protected void assertFromXContent(InternalCardinality aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedCardinality);
        ParsedCardinality parsed = (ParsedCardinality) parsedAggregation;

        assertEquals(aggregation.getValue(), parsed.getValue(), Double.MIN_VALUE);
        assertEquals(aggregation.getValueAsString(), parsed.getValueAsString());
    }

    @Override
    protected InternalCardinality mutateInstance(InternalCardinality instance) {
        String name = instance.getName();
        AbstractHyperLogLogPlusPlus state = instance.getState();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            HyperLogLogPlusPlus newState = new HyperLogLogPlusPlus(state.precision(),
                    new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService()), 0);
            for (int i = 0; i < 10; i++) {
                newState.collect(0, BitMixer.mix64(randomIntBetween(500, 10000)));
            }
            algos.add(newState);
            state = newState;
            break;
        case 2:
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
        return new InternalCardinality(name, state, metadata);
    }
}
