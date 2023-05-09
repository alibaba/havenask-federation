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

package org.havenask.search.aggregations.pipeline;

import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.search.aggregations.PipelineAggregationBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * Base implementation of a {@link PipelineAggregationBuilder}.
 */
public abstract class AbstractPipelineAggregationBuilder<PAB extends AbstractPipelineAggregationBuilder<PAB>>
        extends PipelineAggregationBuilder {

    /**
     * Field shared by many parsers.
     */
    public static final ParseField BUCKETS_PATH_FIELD = new ParseField("buckets_path");

    protected final String type;
    protected Map<String, Object> metadata;

    protected AbstractPipelineAggregationBuilder(String name, String type, String[] bucketsPaths) {
        super(name, bucketsPaths);
        if (type == null) {
            throw new IllegalArgumentException("[type] must not be null: [" + name + "]");
        }
        this.type = type;
    }

    /**
     * Read from a stream.
     */
    protected AbstractPipelineAggregationBuilder(StreamInput in, String type) throws IOException {
        this(in.readString(), type, in.readStringArray());
        metadata = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringArray(bucketsPaths);
        out.writeMap(metadata);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    public String type() {
        return type;
    }

    protected abstract PipelineAggregator createInternal(Map<String, Object> metadata);

    /**
     * Creates the pipeline aggregator
     *
     * @return The created aggregator
     */
    @Override
    public final PipelineAggregator create() {
        PipelineAggregator aggregator = createInternal(this.metadata);
        return aggregator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public PAB setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return (PAB) this;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());

        if (this.metadata != null) {
            builder.field("meta", this.metadata);
        }
        builder.startObject(type);

        if (!overrideBucketsPath() && bucketsPaths != null) {
            builder.startArray(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName());
            for (String path : bucketsPaths) {
                builder.value(path);
            }
            builder.endArray();
        }

        internalXContent(builder, params);

        builder.endObject();

        return builder.endObject();
    }

    /**
     * @return <code>true</code> if the {@link AbstractPipelineAggregationBuilder}
     *         overrides the XContent rendering of the bucketPath option.
     */
    protected boolean overrideBucketsPath() {
        return false;
    }

    protected abstract XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(bucketsPaths), metadata, name, type);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AbstractPipelineAggregationBuilder<PAB> other = (AbstractPipelineAggregationBuilder<PAB>) obj;
        return Objects.equals(type, other.type)
            && Objects.equals(name, other.name)
            && Objects.equals(metadata, other.metadata)
            && Objects.deepEquals(bucketsPaths, other.bucketsPaths);
    }

    @Override
    public String getType() {
        return type;
    }
}
