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

package org.havenask.rest.action.admin.indices;

import org.havenask.action.ActionListener;
import org.havenask.action.ActionRequest;
import org.havenask.action.ActionType;
import org.havenask.action.admin.indices.validate.query.ValidateQueryAction;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.TransportAction;
import org.havenask.client.node.NodeClient;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.concurrent.ThreadContext;
import org.havenask.common.xcontent.XContentType;
import org.havenask.indices.breaker.NoneCircuitBreakerService;
import org.havenask.rest.RestChannel;
import org.havenask.rest.RestController;
import org.havenask.rest.RestRequest;
import org.havenask.search.AbstractSearchTestCase;
import org.havenask.tasks.Task;
import org.havenask.tasks.TaskManager;
import org.havenask.test.rest.FakeRestChannel;
import org.havenask.test.rest.FakeRestRequest;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.havenask.usage.UsageService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RestValidateQueryActionTests extends AbstractSearchTestCase {

    private static ThreadPool threadPool = new TestThreadPool(RestValidateQueryActionTests.class.getName());
    private static NodeClient client = new NodeClient(Settings.EMPTY, threadPool);

    private static UsageService usageService = new UsageService();
    private static RestController controller = new RestController(emptySet(), null, client,
        new NoneCircuitBreakerService(), usageService);
    private static RestValidateQueryAction action = new RestValidateQueryAction();

    /**
     * Configures {@link NodeClient} to stub {@link ValidateQueryAction} transport action.
     * <p>
     * This lower level of validation is out of the scope of this test.
     */
    @BeforeClass
    public static void stubValidateQueryAction() {
        final TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());

        final TransportAction transportAction = new TransportAction(ValidateQueryAction.NAME,
            new ActionFilters(Collections.emptySet()), taskManager) {
            @Override
            protected void doExecute(Task task, ActionRequest request, ActionListener listener) {
            }
        };

        final Map<ActionType, TransportAction> actions = new HashMap<>();
        actions.put(ValidateQueryAction.INSTANCE, transportAction);

        client.initialize(actions, () -> "local", null, new NamedWriteableRegistry(Collections.emptyList()));
        controller.registerHandler(action);
    }

    @Before
    public void ensureCleanContext() {
        // Make sure we have a clean context for each test
        threadPool.getThreadContext().stashContext();
    }

    @AfterClass
    public static void terminateThreadPool() throws InterruptedException {
        terminate(threadPool);

        threadPool = null;
        client = null;

        usageService = null;
        controller = null;
        action = null;
    }

    public void testRestValidateQueryAction() throws Exception {
        // GIVEN a valid query
        final String content = "{\"query\":{\"bool\":{\"must\":{\"term\":{\"user\":\"foobar\"}}}}}";

        final RestRequest request = createRestRequest(content);
        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);

        // WHEN
        action.handleRequest(request, channel, client);

        // THEN query is valid (i.e. not marked as invalid)
        assertThat(channel.responses().get(), equalTo(0));
        assertThat(channel.errors().get(), equalTo(0));
        assertNull(channel.capturedResponse());
    }

    public void testRestValidateQueryAction_emptyQuery() throws Exception {
        // GIVEN an empty (i.e. invalid) query wrapped into a valid JSON
        final String content = "{\"query\":{}}";

        final RestRequest request = createRestRequest(content);
        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);

        // WHEN
        action.handleRequest(request, channel, client);

        // THEN query is marked as invalid
        assertThat(channel.responses().get(), equalTo(1));
        assertThat(channel.errors().get(), equalTo(0));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("{\"valid\":false}"));
    }

    public void testRestValidateQueryAction_malformedQuery() throws Exception {
        // GIVEN an invalid query due to a malformed JSON
        final String content = "{malformed_json}";

        final RestRequest request = createRestRequest(content);
        final FakeRestChannel channel = new FakeRestChannel(request, true, 0);

        // WHEN
        action.handleRequest(request, channel, client);

        // THEN query is marked as invalid
        assertThat(channel.responses().get(), equalTo(1));
        assertThat(channel.errors().get(), equalTo(0));
        assertThat(channel.capturedResponse().content().utf8ToString(), containsString("{\"valid\":false}"));
    }

    private RestRequest createRestRequest(String content) {
        return new FakeRestRequest.Builder(xContentRegistry())
            .withPath("index1/type1/_validate/query")
            .withParams(emptyMap())
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
    }

    public void testTypeInPath() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.GET)
            .withPath("/some_index/some_type/_validate/query")
            .build();

        performRequest(request);
        assertWarnings(RestValidateQueryAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeParameter() {
        Map<String, String> params = new HashMap<>();
        params.put("type", "some_type");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.GET)
            .withPath("_validate/query")
            .withParams(params)
            .build();

        performRequest(request);
        assertWarnings(RestValidateQueryAction.TYPES_DEPRECATION_MESSAGE);
    }

    private void performRequest(RestRequest request) {
        RestChannel channel = new FakeRestChannel(request, false, 1);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        controller.dispatchRequest(request, channel, threadContext);
    }
}
