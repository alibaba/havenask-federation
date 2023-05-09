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

package org.havenask.repositories;

import org.havenask.common.UUIDs;
import org.havenask.common.unit.TimeValue;
import org.havenask.test.HavenaskTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

public class RepositoriesStatsArchiveTests extends HavenaskTestCase {
    public void testStatsAreEvictedOnceTheyAreOlderThanRetentionPeriod() {
        int retentionTimeInMillis = randomIntBetween(100, 1000);

        AtomicLong fakeRelativeClock = new AtomicLong();
        RepositoriesStatsArchive repositoriesStatsArchive =
            new RepositoriesStatsArchive(TimeValue.timeValueMillis(retentionTimeInMillis),
                100,
                fakeRelativeClock::get);

        for (int i = 0; i < randomInt(10); i++) {
            RepositoryStatsSnapshot repoStats = createRepositoryStats(RepositoryStats.EMPTY_STATS);
            repositoriesStatsArchive.archive(repoStats);
        }

        fakeRelativeClock.set(retentionTimeInMillis * 2);
        int statsToBeRetainedCount = randomInt(10);
        for (int i = 0; i < statsToBeRetainedCount; i++) {
            RepositoryStatsSnapshot repoStats =
                createRepositoryStats(new RepositoryStats(org.havenask.common.collect.Map.of("GET", 10L)));
            repositoriesStatsArchive.archive(repoStats);
        }

        List<RepositoryStatsSnapshot> archivedStats = repositoriesStatsArchive.getArchivedStats();
        assertThat(archivedStats.size(), equalTo(statsToBeRetainedCount));
        for (RepositoryStatsSnapshot repositoryStatsSnapshot : archivedStats) {
            assertThat(repositoryStatsSnapshot.getRepositoryStats().requestCounts,
                equalTo(org.havenask.common.collect.Map.of("GET", 10L)));
        }
    }

    public void testStatsAreRejectedIfTheArchiveIsFull() {
        int retentionTimeInMillis = randomIntBetween(100, 1000);

        AtomicLong fakeRelativeClock = new AtomicLong();
        RepositoriesStatsArchive repositoriesStatsArchive =
            new RepositoriesStatsArchive(TimeValue.timeValueMillis(retentionTimeInMillis),
                1,
                fakeRelativeClock::get);

        assertTrue(repositoriesStatsArchive.archive(createRepositoryStats(RepositoryStats.EMPTY_STATS)));

        fakeRelativeClock.set(retentionTimeInMillis * 2);
        // Now there's room since the previous stats should be evicted
        assertTrue(repositoriesStatsArchive.archive(createRepositoryStats(RepositoryStats.EMPTY_STATS)));
        // There's no room for stats with the same creation time
        assertFalse(repositoriesStatsArchive.archive(createRepositoryStats(RepositoryStats.EMPTY_STATS)));
    }

    public void testClearArchive() {
        int retentionTimeInMillis = randomIntBetween(100, 1000);
        AtomicLong fakeRelativeClock = new AtomicLong();
        RepositoriesStatsArchive repositoriesStatsArchive =
            new RepositoriesStatsArchive(TimeValue.timeValueMillis(retentionTimeInMillis),
                100,
                fakeRelativeClock::get);

        int archivedStatsWithVersionZero = randomIntBetween(1, 20);
        for (int i = 0; i < archivedStatsWithVersionZero; i++) {
            repositoriesStatsArchive.archive(createRepositoryStats(RepositoryStats.EMPTY_STATS, 0));
        }

        int archivedStatsWithNewerVersion = randomIntBetween(1, 20);
        for (int i = 0; i < archivedStatsWithNewerVersion; i++) {
            repositoriesStatsArchive.archive(createRepositoryStats(RepositoryStats.EMPTY_STATS, 1));
        }

        List<RepositoryStatsSnapshot> removedStats = repositoriesStatsArchive.clear(0L);
        assertThat(removedStats.size(), equalTo(archivedStatsWithVersionZero));

        assertThat(repositoriesStatsArchive.getArchivedStats().size(), equalTo(archivedStatsWithNewerVersion));
    }

    private RepositoryStatsSnapshot createRepositoryStats(RepositoryStats repositoryStats) {
        return createRepositoryStats(repositoryStats, 0);
    }

    private RepositoryStatsSnapshot createRepositoryStats(RepositoryStats repositoryStats, long clusterVersion) {
        RepositoryInfo repositoryInfo = new RepositoryInfo(UUIDs.randomBase64UUID(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            org.havenask.common.collect.Map.of("bucket", randomAlphaOfLength(10)),
            System.currentTimeMillis(),
            null);
        return new RepositoryStatsSnapshot(repositoryInfo, repositoryStats, clusterVersion, true);
    }

}
