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

package org.havenask.action.admin.indices.template.put;

import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.ActionType;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.action.support.master.MasterNodeRequest;
import org.havenask.cluster.metadata.ComponentTemplate;
import org.havenask.common.Nullable;
import org.havenask.common.Strings;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.havenask.action.ValidateActions.addValidationError;

/**
 * An action for putting a single component template into the cluster state
 */
public class PutComponentTemplateAction extends ActionType<AcknowledgedResponse> {

    public static final PutComponentTemplateAction INSTANCE = new PutComponentTemplateAction();
    public static final String NAME = "cluster:admin/component_template/put";

    private PutComponentTemplateAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    /**
     * A request for putting a single component template into the cluster state
     */
    public static class Request extends MasterNodeRequest<Request> {
        private final String name;
        @Nullable
        private String cause;
        private boolean create;
        private ComponentTemplate componentTemplate;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            this.cause = in.readOptionalString();
            this.create = in.readBoolean();
            this.componentTemplate = new ComponentTemplate(in);
        }

        /**
         * Constructs a new put component template request with the provided name.
         */
        public Request(String name) {
            this.name = name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeOptionalString(cause);
            out.writeBoolean(create);
            this.componentTemplate.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (name == null || Strings.hasText(name) == false) {
                validationException = addValidationError("name is missing", validationException);
            }
            if (componentTemplate == null) {
                validationException = addValidationError("a component template is required", validationException);
            }
            return validationException;
        }

        /**
         * The name of the index template.
         */
        public String name() {
            return this.name;
        }

        /**
         * Set to {@code true} to force only creation, not an update of an index template. If it already
         * exists, it will fail with an {@link IllegalArgumentException}.
         */
        public Request create(boolean create) {
            this.create = create;
            return this;
        }

        public boolean create() {
            return create;
        }

        /**
         * The cause for this index template creation.
         */
        public Request cause(@Nullable String cause) {
            this.cause = cause;
            return this;
        }

        @Nullable
        public String cause() {
            return this.cause;
        }

        /**
         * The component template that will be inserted into the cluster state
         */
        public Request componentTemplate(ComponentTemplate template) {
            this.componentTemplate = template;
            return this;
        }

        public ComponentTemplate componentTemplate() {
            return this.componentTemplate;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("PutComponentRequest[");
            sb.append("name=").append(name);
            sb.append(", cause=").append(cause);
            sb.append(", create=").append(create);
            sb.append(", component_template=").append(componentTemplate);
            sb.append("]");
            return sb.toString();
        }
    }

}
