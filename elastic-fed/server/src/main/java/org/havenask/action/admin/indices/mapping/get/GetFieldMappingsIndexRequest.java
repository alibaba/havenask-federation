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

package org.havenask.action.admin.indices.mapping.get;

import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.OriginalIndices;
import org.havenask.action.support.IndicesOptions;
import org.havenask.action.support.single.shard.SingleShardRequest;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetFieldMappingsIndexRequest extends SingleShardRequest<GetFieldMappingsIndexRequest> {

    private final boolean probablySingleFieldRequest;
    private final boolean includeDefaults;
    private final String[] fields;
    private final String[] types;

    private OriginalIndices originalIndices;

    GetFieldMappingsIndexRequest(StreamInput in) throws IOException {
        super(in);
        types = in.readStringArray();
        fields = in.readStringArray();
        includeDefaults = in.readBoolean();
        probablySingleFieldRequest = in.readBoolean();
        originalIndices = OriginalIndices.readOriginalIndices(in);
    }

    GetFieldMappingsIndexRequest(GetFieldMappingsRequest other, String index, boolean probablySingleFieldRequest) {
        this.probablySingleFieldRequest = probablySingleFieldRequest;
        this.includeDefaults = other.includeDefaults();
        this.types = other.types();
        this.fields = other.fields();
        assert index != null;
        this.index(index);
        this.originalIndices = new OriginalIndices(other);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String[] types() {
        return types;
    }

    public String[] fields() {
        return fields;
    }

    public boolean probablySingleFieldRequest() {
        return probablySingleFieldRequest;
    }

    public boolean includeDefaults() {
        return includeDefaults;
    }

    @Override
    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(types);
        out.writeStringArray(fields);
        out.writeBoolean(includeDefaults);
        out.writeBoolean(probablySingleFieldRequest);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }

}
