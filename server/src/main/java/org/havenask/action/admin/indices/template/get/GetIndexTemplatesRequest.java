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

package org.havenask.action.admin.indices.template.get;

import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.support.master.MasterNodeReadRequest;
import org.havenask.common.Strings;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.havenask.action.ValidateActions.addValidationError;

/**
 * Request that allows to retrieve index templates
 */
public class GetIndexTemplatesRequest extends MasterNodeReadRequest<GetIndexTemplatesRequest> {

    private String[] names;

    public GetIndexTemplatesRequest() {
    }

    public GetIndexTemplatesRequest(String... names) {
        this.names = names;
    }

    public GetIndexTemplatesRequest(StreamInput in) throws IOException {
        super(in);
        names = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(names);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (names == null) {
            validationException = addValidationError("names is null or empty", validationException);
        } else {
            for (String name : names) {
                if (name == null || !Strings.hasText(name)) {
                    validationException = addValidationError("name is missing", validationException);
                }
            }
        }
        return validationException;
    }

    /**
     * Sets the names of the index templates.
     */
    public GetIndexTemplatesRequest names(String... names) {
        this.names = names;
        return this;
    }

    /**
     * The names of the index templates.
     */
    public String[] names() {
        return this.names;
    }
}
