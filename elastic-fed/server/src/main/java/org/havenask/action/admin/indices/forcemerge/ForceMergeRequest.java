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

package org.havenask.action.admin.indices.forcemerge;

import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.action.support.broadcast.BroadcastRequest;
import org.havenask.common.Nullable;
import org.havenask.common.UUIDs;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to force merging the segments of one or more indices. In order to
 * run a merge on all the indices, pass an empty array or {@code null} for the
 * indices.
 * {@link #maxNumSegments(int)} allows to control the number of segments
 * to force merge down to. Defaults to simply checking if a merge needs
 * to execute, and if so, executes it
 *
 * @see org.havenask.client.Requests#forceMergeRequest(String...)
 * @see org.havenask.client.IndicesAdminClient#forceMerge(ForceMergeRequest)
 * @see ForceMergeResponse
 */
public class ForceMergeRequest extends BroadcastRequest<ForceMergeRequest> {

    public static final class Defaults {
        public static final int MAX_NUM_SEGMENTS = -1;
        public static final boolean ONLY_EXPUNGE_DELETES = false;
        public static final boolean FLUSH = true;
    }

    private int maxNumSegments = Defaults.MAX_NUM_SEGMENTS;
    private boolean onlyExpungeDeletes = Defaults.ONLY_EXPUNGE_DELETES;
    private boolean flush = Defaults.FLUSH;

    private static final Version FORCE_MERGE_UUID_VERSION = LegacyESVersion.V_7_7_0;

    /**
     * Force merge UUID to store in the live commit data of a shard under
     * {@link org.havenask.index.engine.Engine#FORCE_MERGE_UUID_KEY} after force merging it.
     */
    @Nullable
    private final String forceMergeUUID;

    /**
     * Constructs a merge request over one or more indices.
     *
     * @param indices The indices to merge, no indices passed means all indices will be merged.
     */
    public ForceMergeRequest(String... indices) {
        super(indices);
        forceMergeUUID = UUIDs.randomBase64UUID();
    }

    public ForceMergeRequest(StreamInput in) throws IOException {
        super(in);
        maxNumSegments = in.readInt();
        onlyExpungeDeletes = in.readBoolean();
        flush = in.readBoolean();
        if (in.getVersion().onOrAfter(FORCE_MERGE_UUID_VERSION)) {
            forceMergeUUID = in.readOptionalString();
        } else {
            forceMergeUUID = null;
        }
    }

    /**
     * Will merge the index down to &lt;= maxNumSegments. By default, will cause the merge
     * process to merge down to half the configured number of segments.
     */
    public int maxNumSegments() {
        return maxNumSegments;
    }

    /**
     * Will merge the index down to &lt;= maxNumSegments. By default, will cause the merge
     * process to merge down to half the configured number of segments.
     */
    public ForceMergeRequest maxNumSegments(int maxNumSegments) {
        this.maxNumSegments = maxNumSegments;
        return this;
    }

    /**
     * Should the merge only expunge deletes from the index, without full merging.
     * Defaults to full merging ({@code false}).
     */
    public boolean onlyExpungeDeletes() {
        return onlyExpungeDeletes;
    }

    /**
     * Should the merge only expunge deletes from the index, without full merge.
     * Defaults to full merging ({@code false}).
     */
    public ForceMergeRequest onlyExpungeDeletes(boolean onlyExpungeDeletes) {
        this.onlyExpungeDeletes = onlyExpungeDeletes;
        return this;
    }

    /**
     * Force merge UUID to use when force merging or {@code null} if not using one in a mixed version cluster containing nodes older than
     * {@link #FORCE_MERGE_UUID_VERSION}.
     */
    @Nullable
    public String forceMergeUUID() {
        return forceMergeUUID;
    }

    /**
     * Should flush be performed after the merge. Defaults to {@code true}.
     */
    public boolean flush() {
        return flush;
    }

    /**
     * Should flush be performed after the merge. Defaults to {@code true}.
     */
    public ForceMergeRequest flush(boolean flush) {
        this.flush = flush;
        return this;
    }

    @Override
    public String getDescription() {
        return "Force-merge indices " + Arrays.toString(indices()) +
            ", maxSegments[" + maxNumSegments +
            "], onlyExpungeDeletes[" + onlyExpungeDeletes +
            "], flush[" + flush + "]";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(maxNumSegments);
        out.writeBoolean(onlyExpungeDeletes);
        out.writeBoolean(flush);
        if (out.getVersion().onOrAfter(FORCE_MERGE_UUID_VERSION)) {
            out.writeOptionalString(forceMergeUUID);
        }
    }

    @Override
    public String toString() {
        return "ForceMergeRequest{" +
                "maxNumSegments=" + maxNumSegments +
                ", onlyExpungeDeletes=" + onlyExpungeDeletes +
                ", flush=" + flush +
                '}';
    }
}
