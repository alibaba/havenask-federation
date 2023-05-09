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

package org.havenask.http;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.havenask.common.UUIDs;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.logging.Loggers;
import org.havenask.common.network.NetworkService;
import org.havenask.common.network.NetworkUtils;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.transport.TransportAddress;
import org.havenask.common.util.MockBigArrays;
import org.havenask.common.util.MockPageCacheRecycler;
import org.havenask.common.util.concurrent.ThreadContext;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.indices.breaker.NoneCircuitBreakerService;
import org.havenask.rest.RestChannel;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestResponse;
import org.havenask.rest.RestStatus;
import org.havenask.tasks.Task;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.MockLogAppender;
import org.havenask.test.junit.annotations.TestLogging;
import org.havenask.test.rest.FakeRestRequest;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.net.InetAddress.getByName;
import static java.util.Arrays.asList;
import static org.havenask.http.AbstractHttpServerTransport.resolvePublishPort;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AbstractHttpServerTransportTests extends HavenaskTestCase {

    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Collections.emptyList());
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = null;
        networkService = null;
        bigArrays = null;
    }

    public void testHttpPublishPort() throws Exception {
        int boundPort = randomIntBetween(9000, 9100);
        int otherBoundPort = randomIntBetween(9200, 9300);

        int publishPort = resolvePublishPort(Settings.builder().put(HttpTransportSettings.SETTING_HTTP_PUBLISH_PORT.getKey(), 9080).build(),
            randomAddresses(), getByName("127.0.0.2"));
        assertThat("Publish port should be explicitly set to 9080", publishPort, equalTo(9080));

        publishPort = resolvePublishPort(Settings.EMPTY, asList(address("127.0.0.1", boundPort), address("127.0.0.2", otherBoundPort)),
            getByName("127.0.0.1"));
        assertThat("Publish port should be derived from matched address", publishPort, equalTo(boundPort));

        publishPort = resolvePublishPort(Settings.EMPTY, asList(address("127.0.0.1", boundPort), address("127.0.0.2", boundPort)),
            getByName("127.0.0.3"));
        assertThat("Publish port should be derived from unique port of bound addresses", publishPort, equalTo(boundPort));

        final BindHttpException e =
            expectThrows(BindHttpException.class,
                () -> resolvePublishPort(
                    Settings.EMPTY,
                    asList(address("127.0.0.1", boundPort), address("127.0.0.2", otherBoundPort)),
                    getByName("127.0.0.3")));
        assertThat(e.getMessage(), containsString("Failed to auto-resolve http publish port"));

        publishPort = resolvePublishPort(Settings.EMPTY, asList(address("0.0.0.0", boundPort), address("127.0.0.2", otherBoundPort)),
            getByName("127.0.0.1"));
        assertThat("Publish port should be derived from matching wildcard address", publishPort, equalTo(boundPort));

        if (NetworkUtils.SUPPORTS_V6) {
            publishPort = resolvePublishPort(Settings.EMPTY, asList(address("0.0.0.0", boundPort), address("127.0.0.2", otherBoundPort)),
                getByName("::1"));
            assertThat("Publish port should be derived from matching wildcard address", publishPort, equalTo(boundPort));
        }
    }

    public void testDispatchDoesNotModifyThreadContext() {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                threadContext.putHeader("foo", "bar");
                threadContext.putTransient("bar", "baz");
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel,
                                           final ThreadContext threadContext,
                                           final Throwable cause) {
                threadContext.putHeader("foo_bad", "bar");
                threadContext.putTransient("bar_bad", "baz");
            }

        };

        try (AbstractHttpServerTransport transport =
                 new AbstractHttpServerTransport(Settings.EMPTY, networkService, bigArrays, threadPool, xContentRegistry(), dispatcher,
                     new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)) {

                     @Override
                     protected HttpServerChannel bind(InetSocketAddress hostAddress) {
                         return null;
                     }

                     @Override
                     protected void doStart() {

                     }

                     @Override
                     protected void stopInternal() {

                     }

                     @Override
                     public HttpStats stats() {
                         return null;
                     }
                 }) {

            transport.dispatchRequest(null, null, null);
            assertNull(threadPool.getThreadContext().getHeader("foo"));
            assertNull(threadPool.getThreadContext().getTransient("bar"));

            transport.dispatchRequest(null, null, new Exception());
            assertNull(threadPool.getThreadContext().getHeader("foo_bad"));
            assertNull(threadPool.getThreadContext().getTransient("bar_bad"));
        }
    }

    @TestLogging(
        value = "org.havenask.http.HttpTracer:trace",
        reason = "to ensure we log REST requests on TRACE level")
    public void testTracerLog() throws Exception {
        final String includeSettings;
        final String excludeSettings;
        if (randomBoolean()) {
            includeSettings = randomBoolean() ? "*" : "";
        } else {
            includeSettings = "/internal/test";
        }
        excludeSettings = "/internal/testNotSeen";

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (AbstractHttpServerTransport transport =
                 new AbstractHttpServerTransport(Settings.EMPTY, networkService, bigArrays, threadPool, xContentRegistry(),
                     new HttpServerTransport.Dispatcher() {
                         @Override
                         public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                             channel.sendResponse(emptyResponse(RestStatus.OK));
                         }

                         @Override
                         public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                             channel.sendResponse(emptyResponse(RestStatus.BAD_REQUEST));
                         }
                     }, clusterSettings) {
                     @Override
                     protected HttpServerChannel bind(InetSocketAddress hostAddress) {
                         return null;
                     }

                     @Override
                     protected void doStart() {

                     }

                     @Override
                     protected void stopInternal() {

                     }

                     @Override
                     public HttpStats stats() {
                         return null;
                     }
                 }) {
            clusterSettings.applySettings(Settings.builder()
                .put(HttpTransportSettings.SETTING_HTTP_TRACE_LOG_INCLUDE.getKey(), includeSettings)
                .put(HttpTransportSettings.SETTING_HTTP_TRACE_LOG_EXCLUDE.getKey(), excludeSettings)
                .build());
            MockLogAppender appender = new MockLogAppender();
            final String traceLoggerName = "org.havenask.http.HttpTracer";
            try {
                appender.start();
                Loggers.addAppender(LogManager.getLogger(traceLoggerName), appender);

                final String opaqueId = UUIDs.randomBase64UUID(random());
                appender.addExpectation(
                    new MockLogAppender.PatternSeenEventExpectation(
                        "received request", traceLoggerName, Level.TRACE,
                        "\\[\\d+\\]\\[" + opaqueId + "\\]\\[OPTIONS\\]\\[/internal/test\\] received request from \\[.*"));

                final boolean badRequest = randomBoolean();

                appender.addExpectation(
                    new MockLogAppender.PatternSeenEventExpectation(
                        "sent response", traceLoggerName, Level.TRACE,
                        "\\[\\d+\\]\\[" + opaqueId + "\\]\\[" +
                            (badRequest ? "BAD_REQUEST" : "OK")
                            + "\\]\\[null\\]\\[0\\] sent response to \\[.*"));

                appender.addExpectation(
                    new MockLogAppender.UnseenEventExpectation(
                        "received other request", traceLoggerName, Level.TRACE,
                        "\\[\\d+\\]\\[" + opaqueId + "\\]\\[OPTIONS\\]\\[/internal/testNotSeen\\] received request from \\[.*"));

                final Exception inboundException;
                if (badRequest) {
                    inboundException = new RuntimeException();
                } else {
                    inboundException = null;
                }

                final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
                    .withMethod(RestRequest.Method.OPTIONS)
                    .withPath("/internal/test")
                    .withHeaders(Collections.singletonMap(Task.X_OPAQUE_ID, Collections.singletonList(opaqueId)))
                    .withInboundException(inboundException)
                    .build();

                transport.incomingRequest(fakeRestRequest.getHttpRequest(), fakeRestRequest.getHttpChannel());

                final Exception inboundExceptionExcludedPath;
                if (randomBoolean()) {
                    inboundExceptionExcludedPath = new RuntimeException();
                } else {
                    inboundExceptionExcludedPath = null;
                }

                final FakeRestRequest fakeRestRequestExcludedPath = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
                    .withMethod(RestRequest.Method.OPTIONS)
                    .withPath("/internal/testNotSeen")
                    .withHeaders(Collections.singletonMap(Task.X_OPAQUE_ID, Collections.singletonList(opaqueId)))
                    .withInboundException(inboundExceptionExcludedPath)
                    .build();

                transport.incomingRequest(fakeRestRequestExcludedPath.getHttpRequest(), fakeRestRequestExcludedPath.getHttpChannel());
                appender.assertAllExpectationsMatched();
            } finally {
                Loggers.removeAppender(LogManager.getLogger(traceLoggerName), appender);
                appender.stop();
            }
        }
    }

    private static RestResponse emptyResponse(RestStatus status) {
        return new RestResponse() {
            @Override
            public String contentType() {
                return null;
            }

            @Override
            public BytesReference content() {
                return BytesArray.EMPTY;
            }

            @Override
            public RestStatus status() {
                return status;
            }
        };
    }

    private TransportAddress address(String host, int port) throws UnknownHostException {
        return new TransportAddress(getByName(host), port);
    }

    private TransportAddress randomAddress() throws UnknownHostException {
        return address("127.0.0." + randomIntBetween(1, 100), randomIntBetween(9200, 9300));
    }

    private List<TransportAddress> randomAddresses() throws UnknownHostException {
        List<TransportAddress> addresses = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            addresses.add(randomAddress());
        }
        return addresses;
    }
}
