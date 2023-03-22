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

package org.havenask.search.scroll;

import org.apache.lucene.util.SetOnce;
import org.havenask.action.ActionListener;
import org.havenask.action.search.ClearScrollRequest;
import org.havenask.action.search.ClearScrollResponse;
import org.havenask.client.node.NodeClient;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.xcontent.XContentType;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.search.RestClearScrollAction;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.client.NoOpNodeClient;
import org.havenask.test.rest.FakeRestChannel;
import org.havenask.test.rest.FakeRestRequest;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RestClearScrollActionTests extends HavenaskTestCase {

    public void testParseClearScrollRequestWithInvalidJsonThrowsException() throws Exception {
        RestClearScrollAction action = new RestClearScrollAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray("{invalid_json}"), XContentType.JSON).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("Failed to parse request body"));
    }

    public void testBodyParamsOverrideQueryStringParams() throws Exception {
        SetOnce<Boolean> scrollCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener) {
                scrollCalled.set(true);
                assertThat(request.getScrollIds(), hasSize(1));
                assertThat(request.getScrollIds().get(0), equalTo("BODY"));
            }
        }) {
            RestClearScrollAction action = new RestClearScrollAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
                .withParams(Collections.singletonMap("scroll_id", "QUERY_STRING"))
                .withContent(new BytesArray("{\"scroll_id\": [\"BODY\"]}"), XContentType.JSON).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(scrollCalled.get(), equalTo(true));
        }
    }
}
