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

package org.havenask.action.admin.indices.validate.query;

import org.havenask.action.support.DefaultShardOperationFailedException;
import org.havenask.action.support.broadcast.BroadcastResponse;
import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.ConstructingObjectParser;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.havenask.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.havenask.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The response of the validate action.
 *
 *
 */
public class ValidateQueryResponse extends BroadcastResponse {

    public static final String VALID_FIELD = "valid";
    public static final String EXPLANATIONS_FIELD = "explanations";

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ValidateQueryResponse, Void> PARSER = new ConstructingObjectParser<>(
        "validate_query",
        true,
        arg -> {
            BroadcastResponse response = (BroadcastResponse) arg[0];
            return
                new ValidateQueryResponse(
                    (boolean)arg[1],
                    (List<QueryExplanation>)arg[2],
                    response.getTotalShards(),
                    response.getSuccessfulShards(),
                    response.getFailedShards(),
                    Arrays.asList(response.getShardFailures())
                );
        }
    );
    static {
        declareBroadcastFields(PARSER);
        PARSER.declareBoolean(constructorArg(), new ParseField(VALID_FIELD));
        PARSER.declareObjectArray(
            optionalConstructorArg(), QueryExplanation.PARSER, new ParseField(EXPLANATIONS_FIELD)
        );
    }

    private final boolean valid;

    private final List<QueryExplanation> queryExplanations;

    ValidateQueryResponse(StreamInput in) throws IOException {
        super(in);
        valid = in.readBoolean();
        queryExplanations = in.readList(QueryExplanation::new);
    }

    ValidateQueryResponse(boolean valid, List<QueryExplanation> queryExplanations, int totalShards, int successfulShards, int failedShards,
                          List<DefaultShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.valid = valid;
        this.queryExplanations = queryExplanations == null ? Collections.emptyList() : queryExplanations;
    }

    /**
     * A boolean denoting whether the query is valid.
     */
    public boolean isValid() {
        return valid;
    }

    /**
     * The list of query explanations.
     */
    public List<? extends QueryExplanation> getQueryExplanation() {
        return queryExplanations;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(valid);
        out.writeCollection(queryExplanations);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(VALID_FIELD, isValid());
        if (getQueryExplanation() != null && !getQueryExplanation().isEmpty()) {
            builder.startArray(EXPLANATIONS_FIELD);
            for (QueryExplanation explanation : getQueryExplanation()) {
                builder.startObject();
                explanation.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
    }

    public static ValidateQueryResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
