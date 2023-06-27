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

package org.havenask.engine.index.engine;

import java.util.HashMap;
import java.util.Map;

public class CheckpointCalc {
    private long minTimestamp = -1;
    private Map<Long, CheckpointRange> checkpointMap = new HashMap<>();

    public void addCheckpoint(long checkpoint) {
        long now = System.currentTimeMillis();
        // 获取now的分钟值
        long nowMinute = now / 60000 + 1;
        CheckpointRange checkpointValue = checkpointMap.get(nowMinute);
        if (checkpointValue == null) {
            checkpointValue = new CheckpointRange(checkpoint, checkpoint);
            checkpointMap.put(nowMinute, checkpointValue);
        } else {
            if (checkpointValue.getMin() < checkpoint) {
                checkpointValue.setMin(checkpoint);
            }
            if (checkpointValue.getMax() > checkpoint) {
                checkpointValue.setMax(checkpoint);
            }
        }

        // 过期历史checkpoint
    }

    public long getCheckpoint(long timestamp) {
        long minute = timestamp / 60000 + 1;
        CheckpointRange checkpointValue = checkpointMap.get(minute);
        if (checkpointValue == null) {
            return -1;
        }
        return checkpointValue.getMin();
    }

    public static class CheckpointRange {
        private long min;
        private long max;

        public CheckpointRange(long min, long max) {
            this.min = min;
            this.max = max;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        public void setMin(long min) {
            this.min = min;
        }

        public void setMax(long max) {
            this.max = max;
        }
    }
}
