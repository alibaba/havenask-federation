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

package org.havenask.client.indices;

import org.havenask.client.TimedRequest;
import org.havenask.cluster.metadata.ComposableIndexTemplate;
import org.havenask.common.Strings;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A request to create an index template.
 */
public class PutComposableIndexTemplateRequest extends TimedRequest implements ToXContentObject {

    private String name;

    private String cause = "";

    private boolean create;

    private ComposableIndexTemplate indexTemplate;

    /**
     * Sets the name of the index template.
     */
    public PutComposableIndexTemplateRequest name(String name) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name cannot be null or empty");
        }
        this.name = name;
        return this;
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
    public PutComposableIndexTemplateRequest create(boolean create) {
        this.create = create;
        return this;
    }

    public boolean create() {
        return create;
    }

    /**
     * The index template to create.
     */
    public PutComposableIndexTemplateRequest indexTemplate(ComposableIndexTemplate indexTemplate) {
        this.indexTemplate = indexTemplate;
        return this;
    }

    /**
     * The cause for this index template creation.
     */
    public PutComposableIndexTemplateRequest cause(String cause) {
        this.cause = cause;
        return this;
    }

    public String cause() {
        return this.cause;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (indexTemplate != null) {
            indexTemplate.toXContent(builder, params);
        }
        return builder;
    }
}
