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

package org.havenask.action.admin.indices.shrink;

import org.havenask.LegacyESVersion;
import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.IndicesRequest;
import org.havenask.action.admin.indices.alias.Alias;
import org.havenask.action.admin.indices.create.CreateIndexRequest;
import org.havenask.action.support.ActiveShardCount;
import org.havenask.action.support.IndicesOptions;
import org.havenask.action.support.master.AcknowledgedRequest;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.ObjectParser;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.havenask.action.ValidateActions.addValidationError;

/**
 * Request class to shrink an index into a single shard
 */
public class ResizeRequest extends AcknowledgedRequest<ResizeRequest> implements IndicesRequest, ToXContentObject {

    public static final ObjectParser<ResizeRequest, Void> PARSER = new ObjectParser<>("resize_request");
    static {
        PARSER.declareField((parser, request, context) -> request.getTargetIndexRequest().settings(parser.map()),
            new ParseField("settings"), ObjectParser.ValueType.OBJECT);
        PARSER.declareField((parser, request, context) -> request.getTargetIndexRequest().aliases(parser.map()),
            new ParseField("aliases"), ObjectParser.ValueType.OBJECT);
    }

    private CreateIndexRequest targetIndexRequest;
    private String sourceIndex;
    private ResizeType type = ResizeType.SHRINK;
    private Boolean copySettings = true;

    public ResizeRequest(StreamInput in) throws IOException {
        super(in);
        targetIndexRequest = new CreateIndexRequest(in);
        sourceIndex = in.readString();
        if (in.getVersion().onOrAfter(ResizeAction.COMPATIBILITY_VERSION)) {
            type = in.readEnum(ResizeType.class);
        } else {
            type = ResizeType.SHRINK; // BWC this used to be shrink only
        }
        if (in.getVersion().before(LegacyESVersion.V_6_4_0)) {
            copySettings = null;
        } else {
            copySettings = in.readOptionalBoolean();
        }
    }

    ResizeRequest() {}

    public ResizeRequest(String targetIndex, String sourceIndex) {
        this.targetIndexRequest = new CreateIndexRequest(targetIndex);
        this.sourceIndex = sourceIndex;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = targetIndexRequest == null ? null : targetIndexRequest.validate();
        if (sourceIndex == null) {
            validationException = addValidationError("source index is missing", validationException);
        }
        if (targetIndexRequest == null) {
            validationException = addValidationError("target index request is missing", validationException);
        }
        if (targetIndexRequest.settings().getByPrefix("index.sort.").isEmpty() == false) {
            validationException = addValidationError("can't override index sort when resizing an index", validationException);
        }
        if (type == ResizeType.SPLIT && IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexRequest.settings()) == false) {
            validationException = addValidationError("index.number_of_shards is required for split operations", validationException);
        }
        assert copySettings == null || copySettings;
        return validationException;
    }

    public void setSourceIndex(String index) {
        this.sourceIndex = index;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        targetIndexRequest.writeTo(out);
        out.writeString(sourceIndex);
        if (out.getVersion().onOrAfter(ResizeAction.COMPATIBILITY_VERSION)) {
            if (type == ResizeType.CLONE && out.getVersion().before(LegacyESVersion.V_7_4_0)) {
                throw new IllegalArgumentException("can't send clone request to a node that's older than " + LegacyESVersion.V_7_4_0);
            }
            out.writeEnum(type);
        }
        // noinspection StatementWithEmptyBody
        if (out.getVersion().before(LegacyESVersion.V_6_4_0)) {

        } else {
            out.writeOptionalBoolean(copySettings);
        }
    }

    @Override
    public String[] indices() {
        return new String[] {sourceIndex};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.lenientExpandOpen();
    }

    public void setTargetIndex(CreateIndexRequest targetIndexRequest) {
        this.targetIndexRequest = Objects.requireNonNull(targetIndexRequest, "target index request must not be null");
    }

    /**
     * Returns the {@link CreateIndexRequest} for the shrink index
     */
    public CreateIndexRequest getTargetIndexRequest() {
        return targetIndexRequest;
    }

    /**
     * Returns the source index name
     */
    public String getSourceIndex() {
        return sourceIndex;
    }

    /**
     * Sets the number of shard copies that should be active for creation of the
     * new shrunken index to return. Defaults to {@link ActiveShardCount#DEFAULT}, which will
     * wait for one shard copy (the primary) to become active. Set this value to
     * {@link ActiveShardCount#ALL} to wait for all shards (primary and all replicas) to be active
     * before returning. Otherwise, use {@link ActiveShardCount#from(int)} to set this value to any
     * non-negative integer, up to the number of copies per shard (number of replicas + 1),
     * to wait for the desired amount of shard copies to become active before returning.
     * Index creation will only wait up until the timeout value for the number of shard copies
     * to be active before returning.  Check {@link ResizeResponse#isShardsAcknowledged()} to
     * determine if the requisite shard copies were all started before returning or timing out.
     *
     * @param waitForActiveShards number of active shard copies to wait on
     */
    public void setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.getTargetIndexRequest().waitForActiveShards(waitForActiveShards);
    }

    /**
     * A shortcut for {@link #setWaitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public void setWaitForActiveShards(final int waitForActiveShards) {
        setWaitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    /**
     * The type of the resize operation
     */
    public void setResizeType(ResizeType type) {
        this.type = Objects.requireNonNull(type);
    }

    /**
     * Returns the type of the resize operation
     */
    public ResizeType getResizeType() {
        return type;
    }

    public void setCopySettings(final Boolean copySettings) {
        if (copySettings != null && copySettings == false) {
            throw new IllegalArgumentException("[copySettings] can not be explicitly set to [false]");
        }
        this.copySettings = copySettings;
    }

    public Boolean getCopySettings() {
        return copySettings;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject(CreateIndexRequest.SETTINGS.getPreferredName());
            {
                targetIndexRequest.settings().toXContent(builder, params);
            }
            builder.endObject();
            builder.startObject(CreateIndexRequest.ALIASES.getPreferredName());
            {
                for (Alias alias : targetIndexRequest.aliases()) {
                    alias.toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public void fromXContent(XContentParser parser) throws IOException {
        PARSER.parse(parser, this, null);
    }
}
