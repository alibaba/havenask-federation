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

package org.havenask.action.search;

import org.havenask.action.OriginalIndices;
import org.havenask.common.UUIDs;
import org.havenask.common.util.concurrent.AtomicArray;
import org.havenask.index.shard.ShardId;
import org.havenask.search.SearchPhaseResult;
import org.havenask.search.SearchShardTarget;
import org.havenask.search.dfs.DfsSearchResult;
import org.havenask.search.internal.ShardSearchContextId;
import org.havenask.test.HavenaskTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

public class CountedCollectorTests extends HavenaskTestCase {
    public void testCollect() throws InterruptedException {
        ArraySearchPhaseResults<SearchPhaseResult> consumer = new ArraySearchPhaseResults<>(randomIntBetween(1, 100));
        List<Integer> state = new ArrayList<>();
        int numResultsExpected = randomIntBetween(1, consumer.getAtomicArray().length());
        MockSearchPhaseContext context = new MockSearchPhaseContext(consumer.getAtomicArray().length());
        CountDownLatch latch = new CountDownLatch(1);
        boolean maybeFork = randomBoolean();
        Executor executor = (runnable) -> {
            if (randomBoolean() && maybeFork) {
                new Thread(runnable).start();

            } else {
                runnable.run();
            }
        };
        CountedCollector<SearchPhaseResult> collector = new CountedCollector<>(consumer, numResultsExpected,
            latch::countDown, context);
        for (int i = 0; i < numResultsExpected; i++) {
            int shardID = i;
            switch (randomIntBetween(0, 2)) {
                case 0:
                    state.add(0);
                    executor.execute(() -> collector.countDown());
                    break;
                case 1:
                    state.add(1);
                    executor.execute(() -> {
                        DfsSearchResult dfsSearchResult = new DfsSearchResult(
                            new ShardSearchContextId(UUIDs.randomBase64UUID(), shardID), null, null);
                        dfsSearchResult.setShardIndex(shardID);
                        dfsSearchResult.setSearchShardTarget(new SearchShardTarget("foo",
                            new ShardId("bar", "baz", shardID), null, OriginalIndices.NONE));
                        collector.onResult(dfsSearchResult);});
                    break;
                case 2:
                    state.add(2);
                    executor.execute(() -> collector.onFailure(shardID, new SearchShardTarget("foo", new ShardId("bar", "baz", shardID),
                        null, OriginalIndices.NONE), new RuntimeException("boom")));
                    break;
                default:
                    fail("unknown state");
            }
        }
        latch.await();
        assertEquals(numResultsExpected, state.size());
        AtomicArray<SearchPhaseResult> results = consumer.getAtomicArray();
        for (int i = 0; i < numResultsExpected; i++) {
            switch (state.get(i)) {
                case 0:
                    assertNull(results.get(i));
                    break;
                case 1:
                    assertNotNull(results.get(i));
                    assertEquals(i, results.get(i).getContextId().getId());
                    break;
                case 2:
                    final int shardId = i;
                    assertEquals(1, context.failures.stream().filter(f -> f.shardId() == shardId).count());
                    break;
                default:
                    fail("unknown state");
            }
        }

        for (int i = numResultsExpected; i < results.length(); i++) {
            assertNull("index: " + i, results.get(i));
        }
    }
}
