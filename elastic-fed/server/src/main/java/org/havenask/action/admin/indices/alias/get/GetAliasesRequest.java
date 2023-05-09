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

package org.havenask.action.admin.indices.alias.get;

import org.havenask.LegacyESVersion;
import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.AliasesRequest;
import org.havenask.action.support.IndicesOptions;
import org.havenask.action.support.master.MasterNodeReadRequest;
import org.havenask.common.Strings;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetAliasesRequest extends MasterNodeReadRequest<GetAliasesRequest> implements AliasesRequest {

    private String[] indices = Strings.EMPTY_ARRAY;
    private String[] aliases = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandHidden();
    private String[] originalAliases = Strings.EMPTY_ARRAY;

    public GetAliasesRequest(String... aliases) {
        this.aliases = aliases;
        this.originalAliases = aliases;
    }

    public GetAliasesRequest() {
    }

    public GetAliasesRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        aliases = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        if (in.getVersion().onOrAfter(LegacyESVersion.V_6_4_0)) {
            originalAliases = in.readStringArray();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        out.writeStringArray(aliases);
        indicesOptions.writeIndicesOptions(out);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_6_4_0)) {
            out.writeStringArray(originalAliases);
        }
    }

    @Override
    public GetAliasesRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public GetAliasesRequest aliases(String... aliases) {
        this.aliases = aliases;
        this.originalAliases = aliases;
        return this;
    }

    public GetAliasesRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public String[] aliases() {
        return aliases;
    }

    @Override
    public void replaceAliases(String... aliases) {
        this.aliases = aliases;
    }

    /**
     * Returns the aliases as was originally specified by the user
     */
    public String[] getOriginalAliases() {
        return originalAliases;
    }

    @Override
    public boolean expandAliasesWildcards() {
        return true;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
