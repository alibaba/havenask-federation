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

package org.havenask.engine.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class RangeUtil {
    public static final int MAX_PARTITION_RANGE = 65535;
    private static final List<List<PartitionRange>> globalRangeVecTable = initRangeVecTable(512);

    private static List<List<PartitionRange>> initRangeVecTable(int maxPart) {
        List<List<PartitionRange>> rangeVecTable = new ArrayList<>();
        rangeVecTable.add(new ArrayList<>());
        for (int i = 1; i <= maxPart; i++) {
            rangeVecTable.add(splitRange(0, MAX_PARTITION_RANGE, i));
        }
        return rangeVecTable;
    }

    /**
     * get range by partCount and partId
     * @param partCount
     * @param partId
     * @return range, null if not found
     */
    public static PartitionRange getRange(int partCount, int partId) {
        if (partId >= partCount || partCount > MAX_PARTITION_RANGE + 1) {
            return null;
        }
        if (partCount < globalRangeVecTable.size()) {
            return globalRangeVecTable.get(partCount).get(partId);
        }

        List<PartitionRange> vec = splitRange(0, MAX_PARTITION_RANGE, partCount);
        if (partId >= vec.size()) {
            return null;
        }
        return vec.get(partId);
    }

    public static List<PartitionRange> splitRange(int rangeFrom, int rangeTo, int partitionCount) {
        assert rangeTo <= MAX_PARTITION_RANGE;
        List<PartitionRange> ranges = new ArrayList<>();
        if (partitionCount == 0 || rangeFrom > rangeTo) {
            return ranges;
        }
        int rangeCount = rangeTo - rangeFrom + 1;
        int c = rangeCount / partitionCount;
        int m = rangeCount % partitionCount;
        int from = rangeFrom;
        for (int i = 0; i < partitionCount && from <= rangeTo; ++i) {
            int to = from + c + (i >= m ? 0 : 1) - 1;
            ranges.add(new PartitionRange(from, to));
            from = to + 1;
        }
        return ranges;
    }

    public static int getRangeIdx(int rangeFrom, int rangeTo, int partitionCount, PartitionRange range) {
        List<PartitionRange> rangeVec = splitRange(rangeFrom, rangeTo, partitionCount);
        if (rangeVec.size() == 0) {
            return -1;
        }
        for (int i = 0; i < rangeVec.size(); i++) {
            if (rangeVec.get(i).first == range.first && rangeVec.get(i).second == range.second) {
                return i;
            }
        }
        return -1;
    }

    public static int getRangeIdxByHashId(int rangeFrom, int rangeTo, int partitionCount, int hashId) {
        assert rangeFrom <= rangeTo && rangeTo <= MAX_PARTITION_RANGE : "invalid range";
        assert partitionCount > 0 && partitionCount <= (rangeTo - rangeFrom + 1) : "invalid partitionCount";
        int rangeCount = rangeTo - rangeFrom + 1;
        int c = rangeCount / partitionCount;
        int m = rangeCount % partitionCount;
        int bound = m * (c + 1) + rangeFrom;
        if (hashId >= bound) {
            return (hashId - bound) / c + m;
        } else {
            return (hashId - rangeFrom) / (c + 1);
        }
    }

    public static String getRangePartition(int partCount, int partId) {
        PartitionRange range = getRange(partCount, partId);
        return String.format(Locale.ROOT, "partition_%d_%d", range.first, range.second);
    }

    public static String getRangeName(int partCount, int partId) {
        PartitionRange range = getRange(partCount, partId);
        return String.format(Locale.ROOT, "%d_%d", range.first, range.second);
    }

    public static String getRangeName(PartitionRange range) {
        return range.first + "_" + range.second;
    }

    public static class PartitionRange {
        public int first;
        public int second;

        public PartitionRange(int first, int second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartitionRange range = (PartitionRange) o;
            return first == range.first && second == range.second;
        }

        @Override
        public int hashCode() {
            return Objects.hash(first, second);
        }
    }
}
