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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.search.internal;

import org.havenask.LegacyESVersion;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

public final class ShardSearchContextId implements Writeable {
    private final String sessionId;
    private final long id;

    public ShardSearchContextId(String sessionId, long id) {
        this.sessionId = Objects.requireNonNull(sessionId);
        this.id = id;
    }

    public ShardSearchContextId(StreamInput in) throws IOException {
        this.id = in.readLong();
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_7_0)) {
            this.sessionId = in.readString();
        } else {
            this.sessionId = "";
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(id);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_7_0)) {
            out.writeString(sessionId);
        }
    }

    public String getSessionId() {
        return sessionId;
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardSearchContextId other = (ShardSearchContextId) o;
        return id == other.id && sessionId.equals(other.sessionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, id);
    }

    @Override
    public String toString() {
        return "[" + sessionId + "][" + id + "]";
    }
}
