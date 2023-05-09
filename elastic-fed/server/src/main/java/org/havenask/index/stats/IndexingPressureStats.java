/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.index.stats;

import org.havenask.LegacyESVersion;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.common.xcontent.ToXContentFragment;
import org.havenask.common.xcontent.XContentBuilder;

import java.io.IOException;

public class IndexingPressureStats implements Writeable, ToXContentFragment {


    private final long totalCombinedCoordinatingAndPrimaryBytes;
    private final long totalCoordinatingBytes;
    private final long totalPrimaryBytes;
    private final long totalReplicaBytes;

    private final long currentCombinedCoordinatingAndPrimaryBytes;
    private final long currentCoordinatingBytes;
    private final long currentPrimaryBytes;
    private final long currentReplicaBytes;
    private final long coordinatingRejections;
    private final long primaryRejections;
    private final long replicaRejections;
    private final long memoryLimit;

    public IndexingPressureStats(StreamInput in) throws IOException {
        totalCombinedCoordinatingAndPrimaryBytes = in.readVLong();
        totalCoordinatingBytes = in.readVLong();
        totalPrimaryBytes = in.readVLong();
        totalReplicaBytes = in.readVLong();

        currentCombinedCoordinatingAndPrimaryBytes = in.readVLong();
        currentCoordinatingBytes = in.readVLong();
        currentPrimaryBytes = in.readVLong();
        currentReplicaBytes = in.readVLong();

        coordinatingRejections = in.readVLong();
        primaryRejections = in.readVLong();
        replicaRejections = in.readVLong();

        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_10_0)) {
            memoryLimit = in.readVLong();
        } else {
            memoryLimit = -1L;
        }
    }

    public IndexingPressureStats(long totalCombinedCoordinatingAndPrimaryBytes, long totalCoordinatingBytes, long totalPrimaryBytes,
                                 long totalReplicaBytes, long currentCombinedCoordinatingAndPrimaryBytes, long currentCoordinatingBytes,
                                 long currentPrimaryBytes, long currentReplicaBytes, long coordinatingRejections, long primaryRejections,
                                 long replicaRejections, long memoryLimit) {
        this.totalCombinedCoordinatingAndPrimaryBytes = totalCombinedCoordinatingAndPrimaryBytes;
        this.totalCoordinatingBytes = totalCoordinatingBytes;
        this.totalPrimaryBytes = totalPrimaryBytes;
        this.totalReplicaBytes = totalReplicaBytes;
        this.currentCombinedCoordinatingAndPrimaryBytes = currentCombinedCoordinatingAndPrimaryBytes;
        this.currentCoordinatingBytes = currentCoordinatingBytes;
        this.currentPrimaryBytes = currentPrimaryBytes;
        this.currentReplicaBytes = currentReplicaBytes;
        this.coordinatingRejections = coordinatingRejections;
        this.primaryRejections = primaryRejections;
        this.replicaRejections = replicaRejections;
        this.memoryLimit = memoryLimit;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalCombinedCoordinatingAndPrimaryBytes);
        out.writeVLong(totalCoordinatingBytes);
        out.writeVLong(totalPrimaryBytes);
        out.writeVLong(totalReplicaBytes);

        out.writeVLong(currentCombinedCoordinatingAndPrimaryBytes);
        out.writeVLong(currentCoordinatingBytes);
        out.writeVLong(currentPrimaryBytes);
        out.writeVLong(currentReplicaBytes);

        out.writeVLong(coordinatingRejections);
        out.writeVLong(primaryRejections);
        out.writeVLong(replicaRejections);

        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_10_0)) {
            out.writeVLong(memoryLimit);
        }
    }

    public long getTotalCombinedCoordinatingAndPrimaryBytes() {
        return totalCombinedCoordinatingAndPrimaryBytes;
    }

    public long getTotalCoordinatingBytes() {
        return totalCoordinatingBytes;
    }

    public long getTotalPrimaryBytes() {
        return totalPrimaryBytes;
    }

    public long getTotalReplicaBytes() {
        return totalReplicaBytes;
    }

    public long getCurrentCombinedCoordinatingAndPrimaryBytes() {
        return currentCombinedCoordinatingAndPrimaryBytes;
    }

    public long getCurrentCoordinatingBytes() {
        return currentCoordinatingBytes;
    }

    public long getCurrentPrimaryBytes() {
        return currentPrimaryBytes;
    }

    public long getCurrentReplicaBytes() {
        return currentReplicaBytes;
    }

    public long getCoordinatingRejections() {
        return coordinatingRejections;
    }

    public long getPrimaryRejections() {
        return primaryRejections;
    }

    public long getReplicaRejections() {
        return replicaRejections;
    }

    private static final String COMBINED = "combined_coordinating_and_primary";
    private static final String COMBINED_IN_BYTES = "combined_coordinating_and_primary_in_bytes";
    private static final String COORDINATING = "coordinating";
    private static final String COORDINATING_IN_BYTES = "coordinating_in_bytes";
    private static final String PRIMARY = "primary";
    private static final String PRIMARY_IN_BYTES = "primary_in_bytes";
    private static final String REPLICA = "replica";
    private static final String REPLICA_IN_BYTES = "replica_in_bytes";
    private static final String ALL = "all";
    private static final String ALL_IN_BYTES = "all_in_bytes";
    private static final String COORDINATING_REJECTIONS = "coordinating_rejections";
    private static final String PRIMARY_REJECTIONS = "primary_rejections";
    private static final String REPLICA_REJECTIONS = "replica_rejections";
    private static final String LIMIT = "limit";
    private static final String LIMIT_IN_BYTES = "limit_in_bytes";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("indexing_pressure");
        builder.startObject("memory");
        builder.startObject("current");
        builder.humanReadableField(COMBINED_IN_BYTES, COMBINED, new ByteSizeValue(currentCombinedCoordinatingAndPrimaryBytes));
        builder.humanReadableField(COORDINATING_IN_BYTES, COORDINATING, new ByteSizeValue(currentCoordinatingBytes));
        builder.humanReadableField(PRIMARY_IN_BYTES, PRIMARY, new ByteSizeValue(currentPrimaryBytes));
        builder.humanReadableField(REPLICA_IN_BYTES, REPLICA, new ByteSizeValue(currentReplicaBytes));
        builder.humanReadableField(ALL_IN_BYTES, ALL, new ByteSizeValue(currentReplicaBytes + currentCombinedCoordinatingAndPrimaryBytes));
        builder.endObject();
        builder.startObject("total");
        builder.humanReadableField(COMBINED_IN_BYTES, COMBINED, new ByteSizeValue(totalCombinedCoordinatingAndPrimaryBytes));
        builder.humanReadableField(COORDINATING_IN_BYTES, COORDINATING, new ByteSizeValue(totalCoordinatingBytes));
        builder.humanReadableField(PRIMARY_IN_BYTES, PRIMARY, new ByteSizeValue(totalPrimaryBytes));
        builder.humanReadableField(REPLICA_IN_BYTES, REPLICA, new ByteSizeValue(totalReplicaBytes));
        builder.humanReadableField(ALL_IN_BYTES, ALL, new ByteSizeValue(totalReplicaBytes + totalCombinedCoordinatingAndPrimaryBytes));
        builder.field(COORDINATING_REJECTIONS, coordinatingRejections);
        builder.field(PRIMARY_REJECTIONS, primaryRejections);
        builder.field(REPLICA_REJECTIONS, replicaRejections);
        builder.endObject();
        builder.humanReadableField(LIMIT_IN_BYTES, LIMIT, new ByteSizeValue(memoryLimit));
        builder.endObject();
        return builder.endObject();
    }
}

