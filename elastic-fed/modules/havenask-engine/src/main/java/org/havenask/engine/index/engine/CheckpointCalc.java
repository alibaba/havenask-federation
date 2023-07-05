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

    private long milisPerSegment;

    private long timeGapMargin;

    private int segmentSize;

    public ArrayDeque<Checkpoint> checkpointDeque = new ArrayDeque<>(segmentSize);

    private long currentCheckpoint = -1;

    CheckpointCalc() {
        this(60000, 0, 10);
    }

    CheckpointCalc(long milisPerSegment, long timeGapMargin, int segmentSize) {
        this.milisPerSegment = milisPerSegment;
        this.timeGapMargin = timeGapMargin;
        this.segmentSize = segmentSize;
    }

    public void addCheckpoint(long time, long seqNo) {
        Checkpoint lastCheckpoint = checkpointDeque.peekLast();

        long timeSegment = time / milisPerSegment;

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

            if (timeSegment == lastCheckpoint.time / milisPerSegment) { // same time segment
                lastCheckpoint.setTime(time);
                lastCheckpoint.setSeqNo(seqNo);
            } else if (timeSegment > lastCheckpoint.time / milisPerSegment) { // new time segment
                if (checkpointDeque.size() == segmentSize) {
                    checkpointDeque.removeFirst();
                }
                checkpointDeque.addLast(new Checkpoint(time, seqNo));
            }

        }
    }

    public long getCheckpoint(long time) {
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
