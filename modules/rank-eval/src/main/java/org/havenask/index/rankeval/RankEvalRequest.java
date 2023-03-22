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

package org.havenask.index.rankeval;

import org.havenask.LegacyESVersion;
import org.havenask.action.ActionRequest;
import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.IndicesRequest;
import org.havenask.action.search.SearchRequest;
import org.havenask.action.search.SearchType;
import org.havenask.action.support.IndicesOptions;
import org.havenask.common.Strings;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Request to perform a search ranking evaluation.
 */
public class RankEvalRequest extends ActionRequest implements IndicesRequest.Replaceable {

    private RankEvalSpec rankingEvaluationSpec;

    private IndicesOptions indicesOptions  = SearchRequest.DEFAULT_INDICES_OPTIONS;
    private String[] indices = Strings.EMPTY_ARRAY;

    private SearchType searchType = SearchType.DEFAULT;

    public RankEvalRequest(RankEvalSpec rankingEvaluationSpec, String[] indices) {
        this.rankingEvaluationSpec = Objects.requireNonNull(rankingEvaluationSpec, "ranking evaluation specification must not be null");
        indices(indices);
    }

    RankEvalRequest(StreamInput in) throws IOException {
        super(in);
        rankingEvaluationSpec = new RankEvalSpec(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
            searchType = SearchType.fromId(in.readByte());
        }
    }

    RankEvalRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = null;
        if (rankingEvaluationSpec == null) {
            e = new ActionRequestValidationException();
            e.addValidationError("missing ranking evaluation specification");
        }
        return e;
    }

    /**
     * Returns the specification of the ranking evaluation.
     */
    public RankEvalSpec getRankEvalSpec() {
        return rankingEvaluationSpec;
    }

    /**
     * Set the specification of the ranking evaluation.
     */
    public void setRankEvalSpec(RankEvalSpec task) {
        this.rankingEvaluationSpec = task;
    }

    /**
     * Sets the indices the search will be executed on.
     */
    @Override
    public RankEvalRequest indices(String... indices) {
        Objects.requireNonNull(indices, "indices must not be null");
        for (String index : indices) {
            Objects.requireNonNull(index, "index must not be null");
        }
        this.indices = indices;
        return this;
    }

    /**
     * @return the indices for this request
     */
    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public void indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "indicesOptions must not be null");
    }

    /**
     * The search type to execute, defaults to {@link SearchType#DEFAULT}.
     */
    public void searchType(SearchType searchType) {
        this.searchType = Objects.requireNonNull(searchType, "searchType must not be null");
    }

    /**
     * The type of search to execute.
     */
    public SearchType searchType() {
        return searchType;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        rankingEvaluationSpec.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
            out.writeByte(searchType.id());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RankEvalRequest that = (RankEvalRequest) o;
        return Objects.equals(indicesOptions, that.indicesOptions) &&
                Arrays.equals(indices, that.indices) &&
                Objects.equals(rankingEvaluationSpec, that.rankingEvaluationSpec) &&
                Objects.equals(searchType, that.searchType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indicesOptions, Arrays.hashCode(indices), rankingEvaluationSpec, searchType);
    }
}
