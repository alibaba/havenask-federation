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

package org.havenask.search.aggregations.support;

import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.index.query.QueryShardContext;
import org.havenask.search.aggregations.AbstractAggregationBuilder;
import org.havenask.search.aggregations.AggregationInitializationException;
import org.havenask.search.aggregations.AggregatorFactories;
import org.havenask.search.aggregations.AggregatorFactories.Builder;
import org.havenask.search.aggregations.AggregatorFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class ArrayValuesSourceAggregationBuilder<AB extends ArrayValuesSourceAggregationBuilder<AB>>
    extends AbstractAggregationBuilder<AB> {

    public static final ParseField MULTIVALUE_MODE_FIELD = new ParseField("mode");

    public abstract static class LeafOnly<AB extends ArrayValuesSourceAggregationBuilder<AB>>
        extends ArrayValuesSourceAggregationBuilder<AB> {

        protected LeafOnly(String name) {
            super(name);
        }

        protected LeafOnly(LeafOnly<AB> clone, Builder factoriesBuilder, Map<String, Object> metadata) {
            super(clone, factoriesBuilder, metadata);
            if (factoriesBuilder.count() > 0) {
                throw new AggregationInitializationException("Aggregator [" + name + "] of type ["
                    + getType() + "] cannot accept sub-aggregations");
            }
        }

        /**
         * Read from a stream
         */
        protected LeafOnly(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public AB subAggregations(Builder subFactories) {
            throw new AggregationInitializationException("Aggregator [" + name + "] of type [" +
                getType() + "] cannot accept sub-aggregations");
        }

        @Override
        public final BucketCardinality bucketCardinality() {
            return BucketCardinality.NONE;
        }
    }

    private List<String> fields = Collections.emptyList();
    /* The parser doesn't support setting userValueTypeHint (aka valueType), but we do serialize and deserialize it, so keeping it around
    for now so as to not break BWC.  Future refactors should feel free to remove this field. --Tozzi 2020-01-16
     */
    private ValueType userValueTypeHint = null;
    private String format = null;
    private Object missing = null;
    private Map<String, Object> missingMap = Collections.emptyMap();

    protected ArrayValuesSourceAggregationBuilder(String name) {
        super(name);
    }

    protected ArrayValuesSourceAggregationBuilder(ArrayValuesSourceAggregationBuilder<AB> clone,
                                                  Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.fields = new ArrayList<>(clone.fields);
        this.userValueTypeHint = clone.userValueTypeHint;
        this.format = clone.format;
        this.missingMap = new HashMap<>(clone.missingMap);
        this.missing = clone.missing;
    }

    protected ArrayValuesSourceAggregationBuilder(StreamInput in)
        throws IOException {
        super(in);
        read(in);
    }

    /**
     * Read from a stream.
     */
    @SuppressWarnings("unchecked")
    private void read(StreamInput in) throws IOException {
        fields = (ArrayList<String>)in.readGenericValue();
        userValueTypeHint = in.readOptionalWriteable(ValueType::readFromStream);
        format = in.readOptionalString();
        missingMap = in.readMap();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        out.writeGenericValue(fields);
        out.writeOptionalWriteable(userValueTypeHint);
        out.writeOptionalString(format);
        out.writeMap(missingMap);
        innerWriteTo(out);
    }

    /**
     * Write subclass' state to the stream
     */
    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    /**
     * Sets the field to use for this aggregation.
     */
    @SuppressWarnings("unchecked")
    public AB fields(List<String> fields) {
        if (fields == null) {
            throw new IllegalArgumentException("[field] must not be null: [" + name + "]");
        }
        this.fields = fields;
        return (AB) this;
    }

    /**
     * Gets the field to use for this aggregation.
     */
    public List<String> fields() {
        return fields;
    }

    /**
     * Sets the format to use for the output of the aggregation.
     */
    @SuppressWarnings("unchecked")
    public AB format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return (AB) this;
    }

    /**
     * Gets the format to use for the output of the aggregation.
     */
    public String format() {
        return format;
    }

    /**
     * Sets the value to use when the aggregation finds a missing value in a
     * document
     */
    @SuppressWarnings("unchecked")
    public AB missingMap(Map<String, Object> missingMap) {
        if (missingMap == null) {
            throw new IllegalArgumentException("[missing] must not be null: [" + name + "]");
        }
        this.missingMap = missingMap;
        return (AB) this;
    }

    /**
     * Gets the value to use when the aggregation finds a missing value in a
     * document
     */
    public Map<String, Object> missingMap() {
        return missingMap;
    }

    @Override
    protected final ArrayValuesSourceAggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent,
                                                               Builder subFactoriesBuilder) throws IOException {
        Map<String, ValuesSourceConfig> configs = resolveConfig(queryShardContext);
        ArrayValuesSourceAggregatorFactory factory = innerBuild(queryShardContext, configs, parent, subFactoriesBuilder);
        return factory;
    }

    protected Map<String, ValuesSourceConfig> resolveConfig(QueryShardContext queryShardContext) {
        HashMap<String, ValuesSourceConfig> configs = new HashMap<>();
        for (String field : fields) {
            ValuesSourceConfig config = ValuesSourceConfig.resolveUnregistered(queryShardContext, userValueTypeHint, field, null,
                missingMap.get(field), null, format, CoreValuesSourceType.BYTES);
            configs.put(field, config);
        }
        return configs;
    }

    protected abstract ArrayValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                                     Map<String, ValuesSourceConfig> configs,
                                                                     AggregatorFactory parent,
                                                                     AggregatorFactories.Builder subFactoriesBuilder) throws IOException;

    @Override
    public final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        // todo add ParseField support to XContentBuilder
        if (fields != null) {
            builder.field(CommonFields.FIELDS.getPreferredName(), fields);
        }
        if (missing != null) {
            builder.field(CommonFields.MISSING.getPreferredName(), missing);
        }
        if (format != null) {
            builder.field(CommonFields.FORMAT.getPreferredName(), format);
        }
        if (userValueTypeHint != null) {
            builder.field(CommonFields.VALUE_TYPE.getPreferredName(), userValueTypeHint.getPreferredName());
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    protected abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields, format, missing, userValueTypeHint);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ArrayValuesSourceAggregationBuilder<?> other = (ArrayValuesSourceAggregationBuilder<?>) obj;
        return Objects.equals(fields, other.fields)
            && Objects.equals(format, other.format)
            && Objects.equals(missing, other.missing)
            && Objects.equals(userValueTypeHint, other.userValueTypeHint);
    }
}
