/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.stop.action;

import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.support.nodes.BaseNodesRequest;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.havenask.action.ValidateActions.addValidationError;

public class HavenaskStopRequest extends BaseNodesRequest<HavenaskStopRequest> {

    private String role;

    public HavenaskStopRequest(StreamInput in) throws IOException {
        super(in);
        this.role = in.readString();
    }

    /**
     * Execute stop action for nodes based on the specified nodes ids.
     * If none ids are passed, all nodes will execute stop action.
     */
    public HavenaskStopRequest(String role, String... nodesIds) {
        super(nodesIds);
        this.role = role;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        // role must be "searcher", "qrs" or "all"
        if (role == null) {
            validationException = addValidationError("role must be specified", validationException);
        } else if (!role.equals("searcher") && !role.equals("qrs") && !role.equals("all")) {
            validationException = addValidationError("role must be \"searcher\", \"qrs\" or \"all\", but get " + role, validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(role);
    }

    public String getRole() {
        return this.role;
    }
}
