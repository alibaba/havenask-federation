/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.search.dsl.expression;

import org.havenask.search.slice.SliceBuilder;
import org.havenask.test.HavenaskTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class SliceExpressionTests extends HavenaskTestCase {
    public void testSliceExpression() {
        {
            List<String> resPartitionIdsList = List.of("0,1,2", "3,4", "5,6");
            int max = 3;
            int shardNum = 7;
            testSliceExpression(max, shardNum, resPartitionIdsList);
        }

        {
            List<String> resPartitionIdsList = List.of("0,1,2,3", "4,5,6,7");
            int max = 2;
            int shardNum = 8;
            testSliceExpression(max, shardNum, resPartitionIdsList);
        }

        int loopCount = 5;
        for (int i = 0; i < loopCount; i++) {
            int max = randomIntBetween(2, 10);
            int shardNum = randomIntBetween(10, 20);
            List<String> resPartitionIdsList = computeResPartitionIdsList(max, shardNum);
            testSliceExpression(max, shardNum, resPartitionIdsList);
        }
    }

    private List<String> computeResPartitionIdsList(int max, int shardNum) {
        List<String> res = new ArrayList<>();
        for (int id = 0; id < max; id++) {
            int basePortionSize = shardNum / max;
            int remainder = shardNum % max;

            int start = id * basePortionSize + Math.min(id, remainder);
            int end = start + basePortionSize + (id < remainder ? 1 : 0);

            StringBuilder sb = new StringBuilder();
            for (int i = start; i < end; i++) {
                sb.append(i);
                if (i < end - 1) {
                    sb.append(",");
                }
            }

            res.add(sb.toString());
        }
        return res;
    }

    private void testSliceExpression(int max, int shardNum, List<String> resPartitionIdsList) {
        for (int i = 0; i < max; i++) {
            SliceBuilder slice = new SliceBuilder(i, max);
            SliceExpression sliceExpression = new SliceExpression(slice, shardNum);
            String expectedRes = String.format(Locale.ROOT, "partitionIds='%s'", resPartitionIdsList.get(i));
            assertEquals(expectedRes, sliceExpression.translate());
        }
    }

    public void testIllegalSliceExpression() {
        {
            SliceExpression sliceExpression = new SliceExpression(null, -1);
            assertEquals(null, sliceExpression.translate());
        }

        {
            SliceBuilder slice = new SliceBuilder(0, 3);
            SliceExpression sliceExpression = new SliceExpression(slice, -1);
            assertEquals(null, sliceExpression.translate());
        }

        {
            int shardNum = randomIntBetween(1, 10);
            int max = randomIntBetween(shardNum + 1, shardNum + 10);
            SliceBuilder slice = new SliceBuilder(0, max);
            SliceExpression sliceExpression = new SliceExpression(slice, shardNum);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, sliceExpression::translate);
            assertEquals("max must be less than or equal to the number of shard", e.getMessage());
        }
    }
}
