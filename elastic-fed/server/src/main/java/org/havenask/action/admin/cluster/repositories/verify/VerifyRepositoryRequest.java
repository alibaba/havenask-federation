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

package org.havenask.action.admin.cluster.repositories.verify;

import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.support.master.AcknowledgedRequest;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.havenask.action.ValidateActions.addValidationError;

/**
 * Verify repository request.
 */
public class VerifyRepositoryRequest extends AcknowledgedRequest<VerifyRepositoryRequest> {

    private String name;

    public VerifyRepositoryRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
    }

    public VerifyRepositoryRequest() {
    }

    /**
     * Constructs a new unregister repository request with the provided name.
     *
     * @param name name of the repository
     */
    public VerifyRepositoryRequest(String name) {
        this.name = name;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the name of the repository to unregister.
     *
     * @param name name of the repository
     */
    public VerifyRepositoryRequest name(String name) {
        this.name = name;
        return this;
    }

    /**
     * The name of the repository.
     *
     * @return the name of the repository
     */
    public String name() {
        return this.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
    }
}
