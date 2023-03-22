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

package org.havenask.action.ingest;

import org.havenask.action.ActionRequestBuilder;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.client.HavenaskClient;

public class DeletePipelineRequestBuilder extends ActionRequestBuilder<DeletePipelineRequest, AcknowledgedResponse> {

    public DeletePipelineRequestBuilder(HavenaskClient client, DeletePipelineAction action) {
        super(client, action, new DeletePipelineRequest());
    }

    public DeletePipelineRequestBuilder(HavenaskClient client, DeletePipelineAction action, String id) {
        super(client, action, new DeletePipelineRequest(id));
    }

    /**
     * Sets the id of the pipeline to delete.
     */
    public DeletePipelineRequestBuilder setId(String id) {
        request.setId(id);
        return this;
    }

}
