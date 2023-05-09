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

package org.havenask.cluster.routing.allocation.decider;

import org.havenask.cluster.routing.allocation.decider.Decision;
import org.havenask.cluster.routing.allocation.decider.Decision.Type;
import org.havenask.test.HavenaskTestCase;

import static org.havenask.cluster.routing.allocation.decider.Decision.Type.NO;
import static org.havenask.cluster.routing.allocation.decider.Decision.Type.THROTTLE;
import static org.havenask.cluster.routing.allocation.decider.Decision.Type.YES;

/**
 * A class for unit testing the {@link Decision} class.
 */
public class DecisionTests extends HavenaskTestCase {

    /**
     * Tests {@link Type#higherThan(Type)}
     */
    public void testHigherThan() {
        // test YES type
        assertTrue(YES.higherThan(NO));
        assertTrue(YES.higherThan(THROTTLE));
        assertFalse(YES.higherThan(YES));

        // test THROTTLE type
        assertTrue(THROTTLE.higherThan(NO));
        assertFalse(THROTTLE.higherThan(THROTTLE));
        assertFalse(THROTTLE.higherThan(YES));

        // test NO type
        assertFalse(NO.higherThan(NO));
        assertFalse(NO.higherThan(THROTTLE));
        assertFalse(NO.higherThan(YES));
    }
}
