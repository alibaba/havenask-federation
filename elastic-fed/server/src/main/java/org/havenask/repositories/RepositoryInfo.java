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

package org.havenask.repositories;

import org.havenask.common.Nullable;
import org.havenask.common.Strings;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.xcontent.ToXContentFragment;
import org.havenask.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public final class RepositoryInfo implements Writeable, ToXContentFragment {
    public final String ephemeralId;
    public final String name;
    public final String type;
    public final Map<String, String> location;
    public final long startedAt;
    @Nullable
    public final Long stoppedAt;

    public RepositoryInfo(String ephemeralId,
                          String name,
                          String type,
                          Map<String, String> location,
                          long startedAt) {
        this(ephemeralId, name, type, location, startedAt, null);
    }

    public RepositoryInfo(String ephemeralId,
                          String name,
                          String type,
                          Map<String, String> location,
                          long startedAt,
                          @Nullable Long stoppedAt) {
        this.ephemeralId = ephemeralId;
        this.name = name;
        this.type = type;
        this.location = location;
        this.startedAt = startedAt;
        if (stoppedAt != null && startedAt > stoppedAt) {
            throw new IllegalArgumentException("createdAt must be before or equal to stoppedAt");
        }
        this.stoppedAt = stoppedAt;
    }

    public RepositoryInfo(StreamInput in) throws IOException {
        this.ephemeralId = in.readString();
        this.name = in.readString();
        this.type = in.readString();
        this.location = in.readMap(StreamInput::readString, StreamInput::readString);
        this.startedAt = in.readLong();
        this.stoppedAt = in.readOptionalLong();
    }

    public RepositoryInfo stopped(long stoppedAt) {
        assert isStopped() == false : "The repository is already stopped";

        return new RepositoryInfo(ephemeralId, name, type, location, startedAt, stoppedAt);
    }

    public boolean isStopped() {
        return stoppedAt != null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(ephemeralId);
        out.writeString(name);
        out.writeString(type);
        out.writeMap(location, StreamOutput::writeString, StreamOutput::writeString);
        out.writeLong(startedAt);
        out.writeOptionalLong(stoppedAt);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("repository_name", name);
        builder.field("repository_type", type);
        builder.field("repository_location", location);
        builder.field("repository_ephemeral_id", ephemeralId);
        builder.field("repository_started_at", startedAt);
        if (stoppedAt != null) {
            builder.field("repository_stopped_at", stoppedAt);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepositoryInfo that = (RepositoryInfo) o;
        return ephemeralId.equals(that.ephemeralId) &&
            name.equals(that.name) &&
            type.equals(that.type) &&
            location.equals(that.location) &&
            startedAt == that.startedAt &&
            Objects.equals(stoppedAt, that.stoppedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ephemeralId, name, type, location, startedAt, stoppedAt);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
