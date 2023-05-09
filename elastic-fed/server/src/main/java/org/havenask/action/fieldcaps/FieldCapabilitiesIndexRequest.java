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

package org.havenask.action.fieldcaps;

import org.havenask.LegacyESVersion;
import org.havenask.action.ActionRequest;
import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.IndicesRequest;
import org.havenask.action.OriginalIndices;
import org.havenask.action.support.IndicesOptions;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

public class FieldCapabilitiesIndexRequest extends ActionRequest implements IndicesRequest {

    public static final IndicesOptions INDICES_OPTIONS = IndicesOptions.strictSingleIndexNoExpandForbidClosed();

    private final String index;
    private final String[] fields;
    private final OriginalIndices originalIndices;
    private final QueryBuilder indexFilter;
    private final long nowInMillis;

    private ShardId shardId;

    // For serialization
    FieldCapabilitiesIndexRequest(StreamInput in) throws IOException {
        super(in);
        shardId = in.readOptionalWriteable(ShardId::new);
        index = in.readOptionalString();
        fields = in.readStringArray();
        if (in.getVersion().onOrAfter(LegacyESVersion.V_6_2_0)) {
            originalIndices = OriginalIndices.readOriginalIndices(in);
        } else {
            originalIndices = OriginalIndices.NONE;
        }
        indexFilter = in.getVersion().onOrAfter(LegacyESVersion.V_7_9_0) ? in.readOptionalNamedWriteable(QueryBuilder.class) : null;
        nowInMillis =  in.getVersion().onOrAfter(LegacyESVersion.V_7_9_0) ? in.readLong() : 0L;
    }

    FieldCapabilitiesIndexRequest(String[] fields,
                                  String index,
                                  OriginalIndices originalIndices,
                                  QueryBuilder indexFilter,
                                  long nowInMillis) {
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("specified fields can't be null or empty");
        }
        this.index = Objects.requireNonNull(index);
        this.fields = fields;
        this.originalIndices = originalIndices;
        this.indexFilter = indexFilter;
        this.nowInMillis = nowInMillis;
    }

    public String[] fields() {
        return fields;
    }

    @Override
    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    public String index() {
        return index;
    }

    public QueryBuilder indexFilter() {
        return indexFilter;
    }

    public ShardId shardId() {
        return shardId;
    }

    public long nowInMillis() {
        return nowInMillis;
    }

    FieldCapabilitiesIndexRequest shardId(ShardId shardId) {
        this.shardId = shardId;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(shardId);
        out.writeOptionalString(index);
        out.writeStringArray(fields);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_6_2_0)) {
            OriginalIndices.writeOriginalIndices(originalIndices, out);
        }
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_9_0)) {
            out.writeOptionalNamedWriteable(indexFilter);
            out.writeLong(nowInMillis);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
