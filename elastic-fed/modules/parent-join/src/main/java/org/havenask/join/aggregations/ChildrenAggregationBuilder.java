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

package org.havenask.join.aggregations;

import org.apache.lucene.search.Query;
import org.havenask.common.ParsingException;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.query.QueryShardContext;
import org.havenask.join.mapper.ParentIdFieldMapper;
import org.havenask.join.mapper.ParentJoinFieldMapper;
import org.havenask.search.aggregations.AggregationBuilder;
import org.havenask.search.aggregations.AggregatorFactories.Builder;
import org.havenask.search.aggregations.AggregatorFactory;
import org.havenask.search.aggregations.support.CoreValuesSourceType;
import org.havenask.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.havenask.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.havenask.search.aggregations.support.ValuesSourceConfig;
import org.havenask.search.aggregations.support.ValuesSourceRegistry;
import org.havenask.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ChildrenAggregationBuilder extends ValuesSourceAggregationBuilder<ChildrenAggregationBuilder> {

    public static final String NAME = "children";

    private final String childType;
    private Query parentFilter;
    private Query childFilter;

    /**
     * @param name
     *            the name of this aggregation
     * @param childType
     *            the type of children documents
     */
    public ChildrenAggregationBuilder(String name, String childType) {
        super(name);
        if (childType == null) {
            throw new IllegalArgumentException("[childType] must not be null: [" + name + "]");
        }
        this.childType = childType;
    }

    protected ChildrenAggregationBuilder(ChildrenAggregationBuilder clone,
                                         Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.childType = clone.childType;
        this.childFilter = clone.childFilter;
        this.parentFilter = clone.parentFilter;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new ChildrenAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public ChildrenAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        childType = in.readString();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeString(childType);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                       ValuesSourceConfig config,
                                                       AggregatorFactory parent,
                                                       Builder subFactoriesBuilder) throws IOException {
        return new ChildrenAggregatorFactory(name, config, childFilter, parentFilter, queryShardContext, parent,
                subFactoriesBuilder, metadata);
    }

    @Override
    protected ValuesSourceConfig resolveConfig(QueryShardContext queryShardContext) {
        ValuesSourceConfig config;

        ParentJoinFieldMapper parentJoinFieldMapper = ParentJoinFieldMapper.getMapper(queryShardContext.getMapperService());
        if (parentJoinFieldMapper == null) {
            // Unmapped field case
            config = ValuesSourceConfig.resolveUnmapped(defaultValueSourceType(), queryShardContext);
            return config;
        }

        ParentIdFieldMapper parentIdFieldMapper = parentJoinFieldMapper.getParentIdFieldMapper(childType, false);
        if (parentIdFieldMapper == null) {
            // Unmapped field case
            config = ValuesSourceConfig.resolveUnmapped(defaultValueSourceType(), queryShardContext);
            return config;
        }

        parentFilter = parentIdFieldMapper.getParentFilter();
        childFilter = parentIdFieldMapper.getChildFilter(childType);
        MappedFieldType fieldType = parentIdFieldMapper.fieldType();
        config = ValuesSourceConfig.resolveFieldOnly(fieldType, queryShardContext);
        return config;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(ParentToChildrenAggregator.TYPE_FIELD.getPreferredName(), childType);
        return builder;
    }

    public static ChildrenAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        String childType = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("type".equals(currentFieldName)) {
                    childType = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (childType == null) {
            throw new ParsingException(parser.getTokenLocation(),
                    "Missing [child_type] field for children aggregation [" + aggregationName + "]");
        }

        return new ChildrenAggregationBuilder(aggregationName, childType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), childType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        ChildrenAggregationBuilder other = (ChildrenAggregationBuilder) obj;
        return Objects.equals(childType, other.childType);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return ValuesSourceRegistry.UNREGISTERED_KEY;
    }
}
