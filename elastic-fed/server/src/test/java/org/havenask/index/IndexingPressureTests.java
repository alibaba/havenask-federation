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

import org.havenask.common.lease.Releasable;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.concurrent.HavenaskRejectedExecutionException;
import org.havenask.index.stats.IndexingPressureStats;
import org.havenask.test.HavenaskTestCase;

public class IndexingPressureTests extends HavenaskTestCase {

    private final Settings settings = Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB").build();

    public void testMemoryBytesMarkedAndReleased() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        try (Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(10, false);
             Releasable coordinating2 = indexingPressure.markCoordinatingOperationStarted(50, false);
             Releasable primary = indexingPressure.markPrimaryOperationStarted(15, true);
             Releasable primary2 = indexingPressure.markPrimaryOperationStarted(5, false);
             Releasable replica = indexingPressure.markReplicaOperationStarted(25, true);
             Releasable replica2 = indexingPressure.markReplicaOperationStarted(10, false)) {
            IndexingPressureStats stats = indexingPressure.stats();
            assertEquals(60, stats.getCurrentCoordinatingBytes());
            assertEquals(20, stats.getCurrentPrimaryBytes());
            assertEquals(80, stats.getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(35, stats.getCurrentReplicaBytes());
        }
        IndexingPressureStats stats = indexingPressure.stats();
        assertEquals(0, stats.getCurrentCoordinatingBytes());
        assertEquals(0, stats.getCurrentPrimaryBytes());
        assertEquals(0, stats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(0, stats.getCurrentReplicaBytes());
        assertEquals(60, stats.getTotalCoordinatingBytes());
        assertEquals(20, stats.getTotalPrimaryBytes());
        assertEquals(80, stats.getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(35, stats.getTotalReplicaBytes());
    }

    public void testAvoidDoubleAccounting() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        try (Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(10, false);
             Releasable primary = indexingPressure.markPrimaryOperationLocalToCoordinatingNodeStarted(15)) {
            IndexingPressureStats stats = indexingPressure.stats();
            assertEquals(10, stats.getCurrentCoordinatingBytes());
            assertEquals(15, stats.getCurrentPrimaryBytes());
            assertEquals(10, stats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        }
        IndexingPressureStats stats = indexingPressure.stats();
        assertEquals(0, stats.getCurrentCoordinatingBytes());
        assertEquals(0, stats.getCurrentPrimaryBytes());
        assertEquals(0, stats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, stats.getTotalCoordinatingBytes());
        assertEquals(15, stats.getTotalPrimaryBytes());
        assertEquals(10, stats.getTotalCombinedCoordinatingAndPrimaryBytes());
    }

    public void testCoordinatingPrimaryRejections() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        try (Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(1024 * 3, false);
             Releasable primary = indexingPressure.markPrimaryOperationStarted(1024 * 3, false);
             Releasable replica = indexingPressure.markReplicaOperationStarted(1024 * 3, false)) {
            if (randomBoolean()) {
                expectThrows(HavenaskRejectedExecutionException.class,
                    () -> indexingPressure.markCoordinatingOperationStarted(1024 * 2, false));
                IndexingPressureStats stats = indexingPressure.stats();
                assertEquals(1, stats.getCoordinatingRejections());
                assertEquals(1024 * 6, stats.getCurrentCombinedCoordinatingAndPrimaryBytes());
            } else {
                expectThrows(HavenaskRejectedExecutionException.class,
                    () -> indexingPressure.markPrimaryOperationStarted(1024 * 2, false));
                IndexingPressureStats stats = indexingPressure.stats();
                assertEquals(1, stats.getPrimaryRejections());
                assertEquals(1024 * 6, stats.getCurrentCombinedCoordinatingAndPrimaryBytes());
            }
            long preForceRejections = indexingPressure.stats().getPrimaryRejections();
            // Primary can be forced
            Releasable forced = indexingPressure.markPrimaryOperationStarted(1024 * 2, true);
            assertEquals(preForceRejections, indexingPressure.stats().getPrimaryRejections());
            assertEquals(1024 * 8, indexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            forced.close();

            // Local to coordinating node primary actions not rejected
            IndexingPressureStats preLocalStats = indexingPressure.stats();
            Releasable local = indexingPressure.markPrimaryOperationLocalToCoordinatingNodeStarted(1024 * 2);
            assertEquals(preLocalStats.getPrimaryRejections(), indexingPressure.stats().getPrimaryRejections());
            assertEquals(1024 * 6, indexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(preLocalStats.getCurrentPrimaryBytes() + 1024 * 2, indexingPressure.stats().getCurrentPrimaryBytes());
            local.close();
        }

        assertEquals(1024 * 8, indexingPressure.stats().getTotalCombinedCoordinatingAndPrimaryBytes());
    }

    public void testReplicaRejections() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        try (Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(1024 * 3, false);
             Releasable primary = indexingPressure.markPrimaryOperationStarted(1024 * 3, false);
             Releasable replica = indexingPressure.markReplicaOperationStarted(1024 * 3, false)) {
            // Replica will not be rejected until replica bytes > 15KB
            Releasable replica2 = indexingPressure.markReplicaOperationStarted(1024 * 11, false);
            assertEquals(1024 * 14, indexingPressure.stats().getCurrentReplicaBytes());
            // Replica will be rejected once we cross 15KB
            expectThrows(HavenaskRejectedExecutionException.class, () -> indexingPressure.markReplicaOperationStarted(1024 * 2, false));
            IndexingPressureStats stats = indexingPressure.stats();
            assertEquals(1, stats.getReplicaRejections());
            assertEquals(1024 * 14, stats.getCurrentReplicaBytes());

            // Replica can be forced
            Releasable forced = indexingPressure.markPrimaryOperationStarted(1024 * 2, true);
            assertEquals(1, indexingPressure.stats().getReplicaRejections());
            assertEquals(1024 * 14, indexingPressure.stats().getCurrentReplicaBytes());
            forced.close();
            replica2.close();
        }

        assertEquals(1024 * 14, indexingPressure.stats().getTotalReplicaBytes());
    }

    public void testForceExecutionOnCoordinating() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        expectThrows(HavenaskRejectedExecutionException.class, () -> indexingPressure.markCoordinatingOperationStarted(1024 * 11, false));
        try (Releasable ignore = indexingPressure.markCoordinatingOperationStarted(1024 * 11, true)) {
            assertEquals(1024 * 11, indexingPressure.stats().getCurrentCoordinatingBytes());
        }
        assertEquals(0, indexingPressure.stats().getCurrentCoordinatingBytes());
    }
}
