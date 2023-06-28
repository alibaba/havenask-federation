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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class CheckpointCalc {
    private Deque<CheckpointRange> checkpointStack = new ArrayDeque<>(10);
    private long currentCkTimestamp = -1;
    private long currentCheckpoint = -1;

    /**
     * 添加checkpoint
     * @param checkpoint checkpoint
     */
    public void addCheckpoint(long checkpoint) {
        long now = System.currentTimeMillis();
        // 获取now的分钟值
        long nowMinute = now / 60000 + 1;
        CheckpointRange checkpointValue = checkpointStack.peek();
        if (checkpointValue == null) {
            checkpointValue = new CheckpointRange(nowMinute, checkpoint, checkpoint);
            checkpointStack.push(checkpointValue);
        } else {
            if (checkpointValue.getMin() < checkpoint) {
                checkpointValue.setMin(checkpoint);
            }
            if (checkpointValue.getMax() > checkpoint) {
                checkpointValue.setMax(checkpoint);
            }
        }
    }

    /**
     * 获取指定时间的checkpoint
     * @param timestamp 指定的时间
     * @return checkpoint
     */
    public long getCheckpoint(long timestamp) {
        if (timestamp == currentCkTimestamp) {
            return currentCheckpoint;
        }

        long minute = timestamp / 60000;
        CheckpointRange prev = null;
        for (CheckpointRange checkpointValue : checkpointStack) {
            if (checkpointValue.minute > minute) {
                prev = checkpointValue;
            } else {
                break;
            }
        }

        if (prev == null) {
            return currentCheckpoint;
        }

        currentCheckpoint = prev.getMin();
        currentCkTimestamp = timestamp;
        return currentCheckpoint;
    }

    /**
     * 获取当前的checkpoint
     * @return 当前的checkpoint
     */
    public long getCurrentCheckpoint() {
        return currentCheckpoint;
    }

    public static class CheckpointRange {
        private final long minute;
        private long min;
        private long max;

        public CheckpointRange(long minute, long min, long max) {
            this.minute = minute;
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
