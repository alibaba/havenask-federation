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

import org.havenask.action.support.master.MasterNodeOperationRequestBuilder;
import org.havenask.client.HavenaskClient;

/**
 * Builder for verify repository request
 */
public class VerifyRepositoryRequestBuilder
        extends MasterNodeOperationRequestBuilder<VerifyRepositoryRequest, VerifyRepositoryResponse, VerifyRepositoryRequestBuilder> {

    /**
     * Constructs unregister repository request builder
     */
    public VerifyRepositoryRequestBuilder(HavenaskClient client, VerifyRepositoryAction action) {
        super(client, action, new VerifyRepositoryRequest());
    }

    /**
     * Constructs unregister repository request builder with specified repository name
     */
    public VerifyRepositoryRequestBuilder(HavenaskClient client, VerifyRepositoryAction action, String name) {
        super(client, action, new VerifyRepositoryRequest(name));
    }

    /**
     * Sets the repository name
     *
     * @param name the repository name
     */
    public VerifyRepositoryRequestBuilder setName(String name) {
        request.name(name);
        return this;
    }
}
