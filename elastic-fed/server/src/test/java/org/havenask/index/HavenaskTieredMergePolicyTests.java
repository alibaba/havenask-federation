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

package org.havenask.index;

import org.apache.lucene.index.TieredMergePolicy;
import org.havenask.test.HavenaskTestCase;

public class HavenaskTieredMergePolicyTests extends HavenaskTestCase {

    public void testDefaults() {
        HavenaskTieredMergePolicy policy = new HavenaskTieredMergePolicy();
        assertEquals(
                new TieredMergePolicy().getMaxMergedSegmentMB(),
                policy.regularMergePolicy.getMaxMergedSegmentMB(), 0d);
        assertEquals(Long.MAX_VALUE / 1024.0 / 1024.0, policy.forcedMergePolicy.getMaxMergedSegmentMB(), 0d);
    }

    public void testSetMaxMergedSegmentMB() {
        HavenaskTieredMergePolicy policy = new HavenaskTieredMergePolicy();
        policy.setMaxMergedSegmentMB(10 * 1024);
        assertEquals(10 * 1024, policy.regularMergePolicy.getMaxMergedSegmentMB(), 0d);
        assertEquals(Long.MAX_VALUE / 1024.0 / 1024.0, policy.forcedMergePolicy.getMaxMergedSegmentMB(), 0d);
    }

    public void testSetForceMergeDeletesPctAllowed() {
        HavenaskTieredMergePolicy policy = new HavenaskTieredMergePolicy();
        policy.setForceMergeDeletesPctAllowed(42);
        assertEquals(42, policy.forcedMergePolicy.getForceMergeDeletesPctAllowed(), 0);
    }

    public void testSetFloorSegmentMB() {
        HavenaskTieredMergePolicy policy = new HavenaskTieredMergePolicy();
        policy.setFloorSegmentMB(42);
        assertEquals(42, policy.regularMergePolicy.getFloorSegmentMB(), 0);
        assertEquals(42, policy.forcedMergePolicy.getFloorSegmentMB(), 0);
    }

    public void testSetMaxMergeAtOnce() {
        HavenaskTieredMergePolicy policy = new HavenaskTieredMergePolicy();
        policy.setMaxMergeAtOnce(42);
        assertEquals(42, policy.regularMergePolicy.getMaxMergeAtOnce());
    }

    public void testSetMaxMergeAtOnceExplicit() {
        HavenaskTieredMergePolicy policy = new HavenaskTieredMergePolicy();
        policy.setMaxMergeAtOnceExplicit(42);
        assertEquals(42, policy.forcedMergePolicy.getMaxMergeAtOnceExplicit());
    }

    public void testSetSegmentsPerTier() {
        HavenaskTieredMergePolicy policy = new HavenaskTieredMergePolicy();
        policy.setSegmentsPerTier(42);
        assertEquals(42, policy.regularMergePolicy.getSegmentsPerTier(), 0);
    }

    public void testSetDeletesPctAllowed() {
        HavenaskTieredMergePolicy policy = new HavenaskTieredMergePolicy();
        policy.setDeletesPctAllowed(42);
        assertEquals(42, policy.regularMergePolicy.getDeletesPctAllowed(), 0);
    }
}
