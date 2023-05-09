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


import org.havenask.LegacyESVersion;
import org.havenask.common.ParseField;
import org.havenask.common.io.stream.NamedWriteable;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.search.aggregations.InternalAggregation;
import org.havenask.search.aggregations.InternalAggregation.ReduceContext;
import org.havenask.search.aggregations.PipelineAggregationBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public abstract class PipelineAggregator implements NamedWriteable {
    /**
     * Parse the {@link PipelineAggregationBuilder} from a {@link XContentParser}.
     */
    @FunctionalInterface
    public interface Parser {
        ParseField BUCKETS_PATH = new ParseField("buckets_path");
        ParseField FORMAT = new ParseField("format");
        ParseField GAP_POLICY = new ParseField("gap_policy");

        /**
         * Returns the pipeline aggregator factory with which this parser is
         * associated.
         *
         * @param pipelineAggregatorName
         *            The name of the pipeline aggregation
         * @param parser the parser
         * @return The resolved pipeline aggregator factory
         * @throws java.io.IOException
         *             When parsing fails
         */
        PipelineAggregationBuilder parse(String pipelineAggregatorName, XContentParser parser)
                throws IOException;
    }

    /**
     * Tree of {@link PipelineAggregator}s to modify a tree of aggregations
     * after their final reduction.
     */
    public static class PipelineTree {
        /**
         * An empty tree of {@link PipelineAggregator}s.
         */
        public static final PipelineTree EMPTY = new PipelineTree(emptyMap(), emptyList());

        private final Map<String, PipelineTree> subTrees;
        private final List<PipelineAggregator> aggregators;

        public PipelineTree(Map<String, PipelineTree> subTrees, List<PipelineAggregator> aggregators) {
            this.subTrees = subTrees;
            this.aggregators = aggregators;
        }

        /**
         * The {@link PipelineAggregator}s for the aggregation at this
         * position in the tree.
         */
        public List<PipelineAggregator> aggregators() {
            return aggregators;
        }

        /**
         * Get the sub-tree at for the named sub-aggregation or {@link #EMPTY}
         * if there are no pipeline aggragations for that sub-aggregator.
         */
        public PipelineTree subTree(String name) {
            return subTrees.getOrDefault(name, EMPTY);
        }

        /**
         * Return {@code true} if this node in the tree has any subtrees.
         */
        public boolean hasSubTrees() {
            return false == subTrees.isEmpty();
        }

        @Override
        public String toString() {
            return "PipelineTree[" + aggregators + "," + subTrees + "]";
        }
    }

    private String name;
    private String[] bucketsPaths;
    private Map<String, Object> metadata;

    protected PipelineAggregator(String name, String[] bucketsPaths, Map<String, Object> metadata) {
        this.name = name;
        this.bucketsPaths = bucketsPaths;
        this.metadata = metadata;
    }

    /**
     * Read from a stream.
     * @deprecated pipeline aggregations added after 7.8.0 shouldn't call this
     */
    @Deprecated
    protected PipelineAggregator(StreamInput in) throws IOException {
        if (in.getVersion().before(LegacyESVersion.V_7_8_0)) {
            name = in.readString();
            bucketsPaths = in.readStringArray();
            metadata = in.readMap();
        } else {
           throw new IllegalStateException("Cannot deserialize pipeline [" + getClass() + "] from before 7.8.0");
        }
    }

    /**
     * {@inheritDoc}
     * @deprecated pipeline aggregations added after 7.8.0 shouldn't call this
     */
    @Override
    @Deprecated
    public final void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(LegacyESVersion.V_7_8_0)) {
            out.writeString(name);
            out.writeStringArray(bucketsPaths);
            out.writeMap(metadata);
            doWriteTo(out);
        } else {
            throw new IllegalArgumentException("[" + name + "] is not supported on versions before 7.8.0");
        }
    }

    /**
     * Write the body of the aggregation to the wire.
     * @deprecated pipeline aggregations added after 7.8.0 don't need to implement this
     */
    @Deprecated
    protected void doWriteTo(StreamOutput out) throws IOException {
    }

    /**
     * The name of the writeable object.
     * @deprecated pipeline aggregations added after 7.8.0 don't need to implement this
     */
    @Override
    @Deprecated
    public String getWriteableName() {
        throw new IllegalArgumentException("[" + name + "] is not supported on versions before 7.8.0");
    }


    public String name() {
        return name;
    }

    public String[] bucketsPaths() {
        return bucketsPaths;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public abstract InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext);
}
