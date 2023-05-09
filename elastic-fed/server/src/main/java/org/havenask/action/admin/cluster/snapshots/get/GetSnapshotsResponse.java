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

package org.havenask.action.admin.cluster.snapshots.get;

import org.havenask.action.ActionResponse;
import org.havenask.common.ParseField;
import org.havenask.common.Strings;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.ConstructingObjectParser;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.snapshots.SnapshotInfo;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Get snapshots response
 */
public class GetSnapshotsResponse extends ActionResponse implements ToXContentObject {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetSnapshotsResponse, Void> GET_SNAPSHOT_PARSER =
        new ConstructingObjectParser<>(GetSnapshotsResponse.class.getName(), true,
            (args) -> new GetSnapshotsResponse((List<SnapshotInfo>) args[0]));

    static {
        GET_SNAPSHOT_PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(),
            (p, c) -> SnapshotInfo.SNAPSHOT_INFO_PARSER.apply(p, c).build(), new ParseField("snapshots"));
    }

    private final List<SnapshotInfo> snapshots;

    public GetSnapshotsResponse(List<SnapshotInfo> snapshots) {
        this.snapshots = Collections.unmodifiableList(snapshots);
    }

    GetSnapshotsResponse(StreamInput in) throws IOException {
        super(in);
        snapshots = Collections.unmodifiableList(in.readList(SnapshotInfo::new));
    }

    /**
     * Returns the list of snapshots
     *
     * @return the list of snapshots
     */
    public List<SnapshotInfo> getSnapshots() {
        return snapshots;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(snapshots);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray("snapshots");
        for (SnapshotInfo snapshotInfo : snapshots) {
            snapshotInfo.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static GetSnapshotsResponse fromXContent(XContentParser parser) throws IOException {
        return GET_SNAPSHOT_PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetSnapshotsResponse that = (GetSnapshotsResponse) o;
        return Objects.equals(snapshots, that.snapshots);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshots);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
