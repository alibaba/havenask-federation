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

package org.havenask.action.admin.indices.close;

import org.havenask.LegacyESVersion;
import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.IndicesRequest;
import org.havenask.action.support.ActiveShardCount;
import org.havenask.action.support.IndicesOptions;
import org.havenask.action.support.master.AcknowledgedRequest;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.util.CollectionUtils;

import java.io.IOException;

import static org.havenask.action.ValidateActions.addValidationError;

/**
 * A request to close an index.
 */
public class CloseIndexRequest extends AcknowledgedRequest<CloseIndexRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    private ActiveShardCount waitForActiveShards = ActiveShardCount.NONE;

    public CloseIndexRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_2_0)) {
            waitForActiveShards = ActiveShardCount.readFrom(in);
        } else {
            waitForActiveShards = ActiveShardCount.NONE;
        }
    }

    public CloseIndexRequest() {
    }

    /**
     * Constructs a new close index request for the specified index.
     */
    public CloseIndexRequest(String... indices) {
        this.indices = indices;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(indices)) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    /**
     * The indices to be closed
     * @return the indices to be closed
     */
    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Sets the indices to be closed
     * @param indices the indices to be closed
     * @return the request itself
     */
    @Override
    public CloseIndexRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @return the desired behaviour regarding indices to ignore and wildcard indices expressions
     */
    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal wild wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     * @return the request itself
     */
    public CloseIndexRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    public ActiveShardCount waitForActiveShards() {
        return waitForActiveShards;
    }

    public CloseIndexRequest waitForActiveShards(final ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_2_0)) {
            waitForActiveShards.writeTo(out);
        }
    }
}
