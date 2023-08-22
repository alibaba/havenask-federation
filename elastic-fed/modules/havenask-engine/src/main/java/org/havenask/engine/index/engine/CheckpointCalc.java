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

public class CheckpointCalc {

    // millisecond per time segment record
    private long millisPerSegment;

    // the reserved time for the gap between the fed and havenask time system
    private long timeGapMargin;

    // the size of the time segment record
    private int segmentSize;

    protected ArrayDeque<Checkpoint> checkpointDeque = new ArrayDeque<>(segmentSize);

    private long currentCheckpoint = -1;

    private static final long DEFAULT_MILLIS_PER_SEGMENT = 1000;

    private static final long DEFAULT_TIME_GAP_MARGIN = 0;

    private static final int DEFAULT_SEGMENT_SIZE = 1000;

    public CheckpointCalc() {
        this(DEFAULT_MILLIS_PER_SEGMENT, DEFAULT_TIME_GAP_MARGIN, DEFAULT_SEGMENT_SIZE);
    }

    public CheckpointCalc(long millisPerSegment, long timeGapMargin, int segmentSize) {
        if (millisPerSegment <= 0) {
            throw new IllegalArgumentException("millisPerSegment must be positive, illegal value: " + millisPerSegment);
        }
        if (timeGapMargin < 0) {
            throw new IllegalArgumentException("timeGapMargin must be non-negative, illegal value: " + timeGapMargin);
        }
        if (segmentSize <= 0) {
            throw new IllegalArgumentException("segmentSize must be positive, illegal value: " + segmentSize);
        }
        this.millisPerSegment = millisPerSegment;
        this.timeGapMargin = timeGapMargin;
        this.segmentSize = segmentSize;
    }

    public synchronized void addCheckpoint(long time, long seqNo) {
        Checkpoint lastCheckpoint = checkpointDeque.peekLast();

        long timeSegment = time / millisPerSegment;

        if (lastCheckpoint == null) {
            checkpointDeque.addLast(new Checkpoint(time, seqNo));
        } else {
            if (time <= lastCheckpoint.time || seqNo < lastCheckpoint.seqNo) {
                // Input error
                return;
            }

            if (seqNo == lastCheckpoint.seqNo) {
                // do nothing
                return;
            }

            if (timeSegment == lastCheckpoint.time / millisPerSegment) { // same time segment
                lastCheckpoint.setTime(time);
                lastCheckpoint.setSeqNo(seqNo);
            } else if (timeSegment > lastCheckpoint.time / millisPerSegment) { // new time segment
                if (checkpointDeque.size() == segmentSize) {
                    checkpointDeque.removeFirst();
                }
                checkpointDeque.addLast(new Checkpoint(time, seqNo));
            }

        }
    }

    public synchronized long getCheckpoint(long time) {
        time = time - timeGapMargin;

        for (Checkpoint checkpoint : checkpointDeque) {
            if (time > checkpoint.time) {
                currentCheckpoint = checkpoint.getSeqNo();
                checkpointDeque.removeFirst();
            }
        }

        return currentCheckpoint;
    }

    public void setTimeGapMargin(long timeGapMargin) {
        if (timeGapMargin < 0) timeGapMargin = 0;
        this.timeGapMargin = timeGapMargin;
    }

    public static class Checkpoint {
        private long time;
        private long seqNo;

        public Checkpoint(long time, long seqNo) {
            this.time = time;
            this.seqNo = seqNo;
        }

        public long getSeqNo() {
            return seqNo;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }

        public void setSeqNo(long seqNo) {
            this.seqNo = seqNo;
        }
    }
}
