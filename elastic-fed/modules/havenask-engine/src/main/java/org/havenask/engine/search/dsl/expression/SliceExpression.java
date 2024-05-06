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

import java.util.Locale;
import java.util.Objects;

public class SliceExpression extends Expression {
    SliceBuilder slice;
    int shardNum;

    public SliceExpression(SliceBuilder slice, int shardNum) {
        this.slice = slice;
        this.shardNum = shardNum;
    }

    @Override
    public String translate() {
        if (shardNum == -1 || Objects.isNull(slice)) {
            return null;
        }
        int id = slice.getId();
        int max = slice.getMax();
        if (max > shardNum) {
            throw new IllegalArgumentException("max must be less than or equal to the number of shard");
        }

        String sliceStr = String.format(Locale.ROOT, "partitionIds='%s'", generatePartitionIds(shardNum, id, max));

        return sliceStr;
    }

    public SliceBuilder getSlice() {
        return slice;
    }

    private String generatePartitionIds(int shardNum, int id, int max) {
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

        return sb.toString();
    }
}
