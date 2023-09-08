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

package org.havenask.engine.index.query;

import java.util.Arrays;

public interface SearchFilter {
    @Deprecated // not safe
    static BitMapFilter getBitMapFilter(long[] bitMap) {
        return new BitMapFilter(bitMap);
    }

    static BitMapFilter getBitMapFilter(long[] bitMap, int size) {
        return new BitMapFilter(bitMap, size);
    }

    @Deprecated // not safe
    static Feeder getFeeder(int[] docIds) {
        return new Feeder(docIds);
    }

    static Feeder getFeeder(int[] docIds, int size) {
        return new Feeder(docIds, size);
    }

    static RangeFilter getRangeFiler(int startInclude, int endExclude) {
        return new RangeFilter(startInclude, endExclude);
    }

    int pop();

    void reset();

    int cardinality();

    boolean test(int doc);

    class RangeFilter implements SearchFilter {
        public int start;
        public int end;
        private int curIndex;

        private RangeFilter(int start, int end) {
            if (start > end || start < 0) {
                throw new RuntimeException("RangeFilter illegal");
            }
            this.start = start;
            this.end = end;
            this.curIndex = start - 1;
        }

        @Override
        public int cardinality() {
            return end - start;
        }

        @Override
        public boolean test(int doc) {
            return (doc >= start && doc < end);
        }

        @Override
        public int pop() {
            curIndex++;
            if (curIndex >= end) {
                return -1;
            }
            return curIndex;
        }

        @Override
        public void reset() {
            this.curIndex = start - 1;
        }
    }

    class BitMapFilter implements SearchFilter {
        public long[] bitMap;
        private int size;
        private int curIndex = -1;
        private long curBits = 0;

        private BitMapFilter(long[] bitMap) {
            if (bitMap == null || bitMap.length == 0) {
                throw new RuntimeException("BitMapFilter illegal");
            }
            this.bitMap = bitMap;
            this.size = bitMap.length;
        }

        private BitMapFilter(long[] bitMap, int size) {
            if (bitMap == null || size <= 0 || bitMap.length < size) {
                throw new RuntimeException("BitMapFilter illegal");
            }
            this.bitMap = bitMap;
            this.size = size;
        }

        @Override
        public int cardinality() {
            int popCount = 0;
            for (int i = 0; i < size; ++i) {
                popCount += Long.bitCount(bitMap[i]);
            }
            return popCount;
        }

        @Override
        public boolean test(int doc) {
            int wordIndex = doc / 64;
            long mark = 1L << (doc % 64);
            return wordIndex < size && ((bitMap[wordIndex] & mark) != 0L);
        }

        @Override
        public void reset() {
            curIndex = -1;
            curBits = 0;
        }

        @Override
        public int pop() {
            while (curBits == 0) {
                curIndex++;
                if (curIndex >= size) {
                    return -1;
                }
                curBits = bitMap[curIndex];
            }

            long t = curBits & -curBits;
            curBits ^= t;
            return curIndex * 64 + Long.bitCount(t - 1);
        }

        public int getNumWords() {
            return size;
        }
    }

    class Feeder implements SearchFilter {
        public int[] docIds;
        private int curIndex = -1;
        private int size;

        private Feeder(int[] docIds) {
            if (docIds == null || docIds.length == 0) {
                throw new RuntimeException("Feeder illegal");
            }
            this.docIds = docIds;
            this.size = docIds.length;
        }

        private Feeder(int[] docIds, int size) {
            if (docIds == null || size <= 0 || docIds.length < size) {
                throw new RuntimeException("Feeder illegal");
            }
            this.docIds = docIds;
            this.size = size;
        }

        @Override
        public int cardinality() {
            return size;
        }

        @Override
        public boolean test(int doc) {
            return Arrays.binarySearch(docIds, 0, size, doc) >= 0;
        }

        @Override
        public void reset() {
            curIndex = -1;
        }

        @Override
        public int pop() {
            curIndex++;
            if (curIndex >= size) {
                return -1;
            }
            return docIds[curIndex];
        }

        public int getNumIds() {
            return size;
        }
    }
}
