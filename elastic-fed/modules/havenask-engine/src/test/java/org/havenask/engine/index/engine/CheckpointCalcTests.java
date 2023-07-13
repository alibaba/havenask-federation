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

import org.havenask.test.HavenaskTestCase;

public class CheckpointCalcTests extends HavenaskTestCase {
    // addCheckpoint case 1: simple
    public void testAddCheckpointSimple() {
        CheckpointCalc checkpointCalc = new CheckpointCalc(100, 0, 5);
        checkpointCalc.addCheckpoint(50, 1);
        checkpointCalc.addCheckpoint(100, 2);
        checkpointCalc.addCheckpoint(150, 3);
        checkpointCalc.addCheckpoint(200, 3);
        checkpointCalc.addCheckpoint(250, 7);
        checkpointCalc.addCheckpoint(300, 7);
        checkpointCalc.addCheckpoint(350, 8);
        assertEquals(4, checkpointCalc.checkpointDeque.size());
        assertEquals(50, checkpointCalc.checkpointDeque.peekFirst().getTime());
        assertEquals(1, checkpointCalc.checkpointDeque.peekFirst().getSeqNo());
        checkpointCalc.checkpointDeque.removeFirst();
        assertEquals(150, checkpointCalc.checkpointDeque.peekFirst().getTime());
        assertEquals(3, checkpointCalc.checkpointDeque.peekFirst().getSeqNo());
        checkpointCalc.checkpointDeque.removeFirst();
        assertEquals(250, checkpointCalc.checkpointDeque.peekFirst().getTime());
        assertEquals(7, checkpointCalc.checkpointDeque.peekFirst().getSeqNo());
        checkpointCalc.checkpointDeque.removeFirst();
        assertEquals(350, checkpointCalc.checkpointDeque.peekFirst().getTime());
        assertEquals(8, checkpointCalc.checkpointDeque.peekFirst().getSeqNo());
    }

    // addCheckpoint case 2: add multi checkpoint with same seqNo
    public void testAddCheckpointSameSeqNo() {
        CheckpointCalc checkpointCalc = new CheckpointCalc(100, 0, 5);
        checkpointCalc.addCheckpoint(50, 1);
        checkpointCalc.addCheckpoint(100, 2);
        checkpointCalc.addCheckpoint(150, 3);
        checkpointCalc.addCheckpoint(200, 3);
        checkpointCalc.addCheckpoint(250, 7);
        checkpointCalc.addCheckpoint(300, 7);
        checkpointCalc.addCheckpoint(350, 8);
        checkpointCalc.addCheckpoint(400, 8);
        checkpointCalc.addCheckpoint(450, 8);
        checkpointCalc.addCheckpoint(500, 8);
        checkpointCalc.addCheckpoint(550, 8);
        checkpointCalc.addCheckpoint(600, 8);
        checkpointCalc.addCheckpoint(650, 8);
        checkpointCalc.addCheckpoint(700, 8);
        checkpointCalc.addCheckpoint(750, 8);
        checkpointCalc.addCheckpoint(800, 9);
        assertEquals(5, checkpointCalc.checkpointDeque.size());
        assertEquals(50, checkpointCalc.checkpointDeque.peekFirst().getTime());
        assertEquals(1, checkpointCalc.checkpointDeque.peekFirst().getSeqNo());
        checkpointCalc.checkpointDeque.removeFirst();
        assertEquals(150, checkpointCalc.checkpointDeque.peekFirst().getTime());
        assertEquals(3, checkpointCalc.checkpointDeque.peekFirst().getSeqNo());
        checkpointCalc.checkpointDeque.removeFirst();
        assertEquals(250, checkpointCalc.checkpointDeque.peekFirst().getTime());
        assertEquals(7, checkpointCalc.checkpointDeque.peekFirst().getSeqNo());
        checkpointCalc.checkpointDeque.removeFirst();
        assertEquals(350, checkpointCalc.checkpointDeque.peekFirst().getTime());
        assertEquals(8, checkpointCalc.checkpointDeque.peekFirst().getSeqNo());
        checkpointCalc.checkpointDeque.removeFirst();
        assertEquals(800, checkpointCalc.checkpointDeque.peekFirst().getTime());
        assertEquals(9, checkpointCalc.checkpointDeque.peekFirst().getSeqNo());
    }

    // addCheckpoint case 3: add multi checkpoint with same seqNo and queue overflow
    public void testAddCheckpointQueueOverflow() {
        CheckpointCalc checkpointCalc = new CheckpointCalc(100, 0, 5);
        checkpointCalc.addCheckpoint(50, 1);
        checkpointCalc.addCheckpoint(100, 2);
        checkpointCalc.addCheckpoint(150, 3);
        checkpointCalc.addCheckpoint(200, 3);
        checkpointCalc.addCheckpoint(250, 7);
        checkpointCalc.addCheckpoint(300, 7);
        checkpointCalc.addCheckpoint(350, 8);
        checkpointCalc.addCheckpoint(400, 8);
        checkpointCalc.addCheckpoint(450, 8);
        checkpointCalc.addCheckpoint(500, 8);
        checkpointCalc.addCheckpoint(550, 8);
        checkpointCalc.addCheckpoint(600, 8);
        checkpointCalc.addCheckpoint(650, 8);
        checkpointCalc.addCheckpoint(700, 8);
        checkpointCalc.addCheckpoint(750, 8);
        checkpointCalc.addCheckpoint(800, 9);
        checkpointCalc.addCheckpoint(850, 10);
        checkpointCalc.addCheckpoint(900, 11);
        assertEquals(5, checkpointCalc.checkpointDeque.size());
        assertEquals(150, checkpointCalc.checkpointDeque.peekFirst().getTime());
        assertEquals(3, checkpointCalc.checkpointDeque.peekFirst().getSeqNo());
        checkpointCalc.checkpointDeque.removeFirst();
        assertEquals(250, checkpointCalc.checkpointDeque.peekFirst().getTime());
        assertEquals(7, checkpointCalc.checkpointDeque.peekFirst().getSeqNo());
        checkpointCalc.checkpointDeque.removeFirst();
        assertEquals(350, checkpointCalc.checkpointDeque.peekFirst().getTime());
        assertEquals(8, checkpointCalc.checkpointDeque.peekFirst().getSeqNo());
        checkpointCalc.checkpointDeque.removeFirst();
        assertEquals(850, checkpointCalc.checkpointDeque.peekFirst().getTime());
        assertEquals(10, checkpointCalc.checkpointDeque.peekFirst().getSeqNo());
        checkpointCalc.checkpointDeque.removeFirst();
        assertEquals(900, checkpointCalc.checkpointDeque.peekFirst().getTime());
        assertEquals(11, checkpointCalc.checkpointDeque.peekFirst().getSeqNo());
    }

    // getCheckpoint case 1: simple case
    public void testGetCheckpointSimple() {
        CheckpointCalc checkpointCalc = new CheckpointCalc(100, 0, 5);
        checkpointCalc.addCheckpoint(50, 1);
        checkpointCalc.addCheckpoint(100, 2);
        checkpointCalc.addCheckpoint(150, 3);
        checkpointCalc.addCheckpoint(200, 3);
        checkpointCalc.addCheckpoint(250, 7);
        checkpointCalc.addCheckpoint(300, 7);
        checkpointCalc.addCheckpoint(350, 8);
        assertEquals(1, checkpointCalc.getCheckpoint(70));
        assertEquals(1, checkpointCalc.getCheckpoint(120));
        assertEquals(3, checkpointCalc.getCheckpoint(170));
        assertEquals(7, checkpointCalc.getCheckpoint(270));
        assertEquals(8, checkpointCalc.getCheckpoint(370));
    }

    // getCheckpoint case 2: get checkpoint while adding checkpoint
    public void testGetCheckpointGetWhileAdd() {
        CheckpointCalc checkpointCalc = new CheckpointCalc(100, 0, 5);
        checkpointCalc.addCheckpoint(50, 1);
        assertEquals(1, checkpointCalc.getCheckpoint(70));
        checkpointCalc.addCheckpoint(100, 2);
        assertEquals(2, checkpointCalc.getCheckpoint(120));
        checkpointCalc.addCheckpoint(150, 3);
        assertEquals(3, checkpointCalc.getCheckpoint(170));
        checkpointCalc.addCheckpoint(200, 3);
        assertEquals(3, checkpointCalc.getCheckpoint(220));
        checkpointCalc.addCheckpoint(250, 7);
        assertEquals(7, checkpointCalc.getCheckpoint(270));
        checkpointCalc.addCheckpoint(300, 7);
        assertEquals(7, checkpointCalc.getCheckpoint(320));
        checkpointCalc.addCheckpoint(350, 8);
        assertEquals(8, checkpointCalc.getCheckpoint(370));
    }

    // getCheckpoint case 3: get checkpoint while adding checkpoint with same seqNo and queue overflow
    public void testGetCheckpointComplex() {
        CheckpointCalc checkpointCalc = new CheckpointCalc(10000, 0, 5);
        checkpointCalc.addCheckpoint(5000, 1);
        assertEquals(1, checkpointCalc.getCheckpoint(7000));
        checkpointCalc.addCheckpoint(10000, 2);
        assertEquals(2, checkpointCalc.getCheckpoint(12000));
        checkpointCalc.addCheckpoint(15000, 3);
        assertEquals(3, checkpointCalc.getCheckpoint(17000));
        checkpointCalc.addCheckpoint(20000, 3);
        assertEquals(3, checkpointCalc.getCheckpoint(22000));
        checkpointCalc.addCheckpoint(25000, 7);
        assertEquals(7, checkpointCalc.getCheckpoint(27000));
        checkpointCalc.addCheckpoint(30000, 7);
        assertEquals(7, checkpointCalc.getCheckpoint(32000));
        checkpointCalc.addCheckpoint(35000, 8);
        assertEquals(8, checkpointCalc.getCheckpoint(37000));
        checkpointCalc.addCheckpoint(40000, 8);
        assertEquals(8, checkpointCalc.getCheckpoint(42000));
        checkpointCalc.addCheckpoint(45000, 8);
        assertEquals(8, checkpointCalc.getCheckpoint(47000));
        checkpointCalc.addCheckpoint(50000, 8);
        assertEquals(8, checkpointCalc.getCheckpoint(52000));
        checkpointCalc.addCheckpoint(55000, 8);
        assertEquals(8, checkpointCalc.getCheckpoint(57000));
        checkpointCalc.addCheckpoint(60000, 8);
        assertEquals(8, checkpointCalc.getCheckpoint(62000));
        checkpointCalc.addCheckpoint(65000, 8);
        assertEquals(8, checkpointCalc.getCheckpoint(67000));
        checkpointCalc.addCheckpoint(70000, 8);
        assertEquals(8, checkpointCalc.getCheckpoint(72000));
        checkpointCalc.addCheckpoint(75000, 8);
        assertEquals(8, checkpointCalc.getCheckpoint(77000));
        checkpointCalc.addCheckpoint(80000, 9);
        assertEquals(9, checkpointCalc.getCheckpoint(82000));
        checkpointCalc.addCheckpoint(85000, 10);
        assertEquals(10, checkpointCalc.getCheckpoint(87000));
        checkpointCalc.addCheckpoint(90000, 11);
        assertEquals(11, checkpointCalc.getCheckpoint(92000));
    }

    // test the case with illegal arguments
    public void testCheckpointCalcIllegalArguments() {
        try {
            CheckpointCalc checkpointCalc = new CheckpointCalc(0, 0, 5);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("millisPerSegment must be positive, illegal value: 0", e.getMessage());
        }
        try {
            CheckpointCalc checkpointCalc = new CheckpointCalc(100, -1, 5);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("timeGapMargin must be non-negative, illegal value: -1", e.getMessage());
        }
        try {
            CheckpointCalc checkpointCalc = new CheckpointCalc(100, 0, 0);
            fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("segmentSize must be positive, illegal value: 0", e.getMessage());
        }
    }
}
