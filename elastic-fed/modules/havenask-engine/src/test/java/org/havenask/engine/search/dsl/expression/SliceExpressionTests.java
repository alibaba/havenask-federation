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

import java.util.List;
import java.util.Locale;

public class SliceExpressionTests extends HavenaskTestCase {
    public void testSliceExpression() {
        List<String> resPartitionIdsList = List.of("0,1,2", "3,4", "5,6");
        int max = 3;
        int shardNum = 7;
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
            SliceBuilder slice = new SliceBuilder(0, 9);
            SliceExpression sliceExpression = new SliceExpression(slice, 7);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, sliceExpression::translate);
            assertEquals("max must be less than or equal to the number of shard", e.getMessage());
        }
    }
}
