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

package org.havenask.indices.memory.breaker;

import org.havenask.common.breaker.CircuitBreaker;
import org.havenask.common.settings.Settings;
import org.havenask.indices.breaker.BreakerSettings;
import org.havenask.indices.breaker.HierarchyCircuitBreakerService;
import org.havenask.test.HavenaskTestCase;

import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for the circuit breaker
 */
public class CircuitBreakerUnitTests extends HavenaskTestCase {
    public static long pctBytes(String percentString) {
        return Settings.EMPTY.getAsMemory("", percentString).getBytes();
    }

    public void testBreakerSettingsValidationWithValidSettings() {
        // parent: {:limit 70}, fd: {:limit 50}, request: {:limit 20}
        BreakerSettings fd = new BreakerSettings(CircuitBreaker.FIELDDATA, pctBytes("50%"), 1.0);
        BreakerSettings request = new BreakerSettings(CircuitBreaker.REQUEST, pctBytes("20%"), 1.0);
        HierarchyCircuitBreakerService.validateSettings(new BreakerSettings[]{fd, request});

        // parent: {:limit 70}, fd: {:limit 40}, request: {:limit 30}
        fd = new BreakerSettings(CircuitBreaker.FIELDDATA, pctBytes("40%"), 1.0);
        request = new BreakerSettings(CircuitBreaker.REQUEST, pctBytes("30%"), 1.0);
        HierarchyCircuitBreakerService.validateSettings(new BreakerSettings[]{fd, request});
    }

    public void testBreakerSettingsValidationNegativeOverhead() {
        // parent: {:limit 70}, fd: {:limit 50}, request: {:limit 20}
        BreakerSettings fd = new BreakerSettings(CircuitBreaker.FIELDDATA, pctBytes("50%"), -0.1);
        BreakerSettings request = new BreakerSettings(CircuitBreaker.REQUEST, pctBytes("20%"), 1.0);
        try {
            HierarchyCircuitBreakerService.validateSettings(new BreakerSettings[]{fd, request});
            fail("settings are invalid but validate settings did not throw an exception");
        } catch (Exception e) {
            assertThat("Incorrect message: " + e.getMessage(),
                    e.getMessage().contains("must be non-negative"), equalTo(true));
        }
    }

}
