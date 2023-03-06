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

package org.havenask.http.netty4;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.ReferenceCounted;
import org.havenask.HavenaskException;
import org.havenask.common.network.NetworkService;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.transport.TransportAddress;
import org.havenask.common.util.MockBigArrays;
import org.havenask.common.util.MockPageCacheRecycler;
import org.havenask.common.util.concurrent.ThreadContext;
import org.havenask.http.HttpServerTransport;
import org.havenask.http.HttpTransportSettings;
import org.havenask.indices.breaker.NoneCircuitBreakerService;
import org.havenask.rest.BytesRestResponse;
import org.havenask.rest.RestChannel;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestStatus;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.SharedGroupFactory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class Netty4BadRequestTests extends HavenaskTestCase {

    private NetworkService networkService;
    private MockBigArrays bigArrays;
    private ThreadPool threadPool;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Collections.emptyList());
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdown() throws Exception {
        terminate(threadPool);
    }

    public void testBadParameterEncoding() throws Exception {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
            @Override
            public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                fail();
            }

            @Override
            public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                try {
                    final Exception e = cause instanceof Exception ? (Exception) cause : new HavenaskException(cause);
                    channel.sendResponse(new BytesRestResponse(channel, RestStatus.BAD_REQUEST, e));
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };

        Settings settings = Settings.builder().put(HttpTransportSettings.SETTING_HTTP_PORT.getKey(), getPortRange()).build();
        try (HttpServerTransport httpServerTransport = new Netty4HttpServerTransport(settings, networkService, bigArrays, threadPool,
            xContentRegistry(), dispatcher, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            new SharedGroupFactory(Settings.EMPTY))) {
            httpServerTransport.start();
            final TransportAddress transportAddress = randomFrom(httpServerTransport.boundAddress().boundAddresses());

            try (Netty4HttpClient nettyHttpClient = new Netty4HttpClient()) {
                final Collection<FullHttpResponse> responses =
                        nettyHttpClient.get(transportAddress.address(), "/_cluster/settings?pretty=%");
                try {
                    assertThat(responses, hasSize(1));
                    assertThat(responses.iterator().next().status().code(), equalTo(400));
                    final Collection<String> responseBodies = Netty4HttpClient.returnHttpResponseBodies(responses);
                    assertThat(responseBodies, hasSize(1));
                    assertThat(responseBodies.iterator().next(), containsString("\"type\":\"bad_parameter_exception\""));
                    assertThat(
                        responseBodies.iterator().next(),
                        containsString(
                            "\"reason\":\"java.lang.IllegalArgumentException: unterminated escape sequence at end of string: %\""));
                } finally {
                    responses.forEach(ReferenceCounted::release);
                }
            }
        }
    }

}
