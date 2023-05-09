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

package org.havenask.action.admin.indices.forcemerge;

import org.havenask.client.node.NodeClient;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentType;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.admin.indices.RestForceMergeAction;
import org.havenask.test.rest.FakeRestChannel;
import org.havenask.test.rest.FakeRestRequest;
import org.havenask.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class RestForceMergeActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestForceMergeAction());
    }

    public void testBodyRejection() throws Exception {
        final RestForceMergeAction handler = new RestForceMergeAction();
        String json = JsonXContent.contentBuilder().startObject().field("max_num_segments", 1).endObject().toString();
        final FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
                .withContent(new BytesArray(json), XContentType.JSON)
                .withPath("/_forcemerge")
                .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> handler.handleRequest(request, new FakeRestChannel(request, randomBoolean(), 1), mock(NodeClient.class)));
        assertThat(e.getMessage(), equalTo("request [GET /_forcemerge] does not support having a body"));
    }

    public void testDeprecationMessage() {
        final Map<String, String> params = new HashMap<>();
        params.put("only_expunge_deletes", Boolean.TRUE.toString());
        params.put("max_num_segments", Integer.toString(randomIntBetween(0, 10)));
        params.put("flush", Boolean.toString(randomBoolean()));

        final RestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withPath("/_forcemerge")
            .withMethod(RestRequest.Method.POST)
            .withParams(params)
            .build();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        dispatchRequest(request);
        assertWarnings("setting only_expunge_deletes and max_num_segments at the same time is deprecated " +
            "and will be rejected in a future version");
    }
}
