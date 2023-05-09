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

import org.havenask.action.ActionListener;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.bytes.ReleasableBytesReference;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.ReleasableBytesStreamOutput;
import org.havenask.common.lease.Releasable;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.BigArrays;
import org.havenask.common.util.ByteArray;
import org.havenask.common.util.MockBigArrays;
import org.havenask.common.util.MockPageCacheRecycler;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.indices.breaker.NoneCircuitBreakerService;
import org.havenask.rest.BytesRestResponse;
import org.havenask.rest.RestChannel;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestResponse;
import org.havenask.rest.RestStatus;
import org.havenask.tasks.Task;
import org.havenask.test.HavenaskTestCase;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DefaultRestChannelTests extends HavenaskTestCase {

    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private HttpChannel httpChannel;

    @Before
    public void setup() {
        httpChannel = mock(HttpChannel.class);
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    @After
    public void shutdown() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

    public void testResponse() {
        final TestHttpResponse response = executeRequest(Settings.EMPTY, "request-host");
        assertThat(response.content(), equalTo(new TestRestResponse().content()));
    }

    public void testCorsEnabledWithoutAllowOrigins() {
        // Set up an HTTP transport with only the CORS enabled setting
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .build();
        TestHttpResponse response = executeRequest(settings, "request-host");
        assertThat(response.headers().get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN), nullValue());
    }

    public void testCorsEnabledWithAllowOrigins() {
        final String originValue = "remote-host";
        final String pattern;
        if (randomBoolean()) {
            pattern = originValue;
        } else {
            pattern = "/remote-hos.+/";
        }
        // create an HTTP transport with CORS enabled and allow origin configured
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN.getKey(), pattern)
            .build();
        TestHttpResponse response = executeRequest(settings, originValue, "https://127.0.0.1");
        assertEquals(originValue, response.headers().get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN).get(0));
        assertThat(response.headers().get(CorsHandler.VARY), containsInAnyOrder(CorsHandler.ORIGIN));
    }

    public void testCorsEnabledWithAllowOriginsAndAllowCredentials() {
        final String originValue = "remote-host";
        // create an HTTP transport with CORS enabled and allow origin configured
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN.getKey(), CorsHandler.ANY_ORIGIN)
            .put(HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
            .build();
        TestHttpResponse response = executeRequest(settings, originValue, "https://127.0.0.1");
        assertEquals(originValue, response.headers().get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN).get(0));
        assertEquals(CorsHandler.ORIGIN, response.headers().get(CorsHandler.VARY).get(0));
        assertEquals("true", response.headers().get(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS).get(0));
    }

    public void testThatAnyOriginWorks() {
        final String originValue = CorsHandler.ANY_ORIGIN;
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN.getKey(), originValue)
            .build();
        TestHttpResponse response = executeRequest(settings, originValue, "https://127.0.0.1");
        assertEquals(originValue, response.headers().get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN).get(0));
        assertNull(response.headers().get(CorsHandler.VARY));
    }

    public void testHeadersSet() {
        Settings settings = Settings.builder().build();
        final TestHttpRequest httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        httpRequest.getHeaders().put(Task.X_OPAQUE_ID, Collections.singletonList("abc"));
        final RestRequest request = RestRequest.request(xContentRegistry(), httpRequest, httpChannel);
        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        // send a response
        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, httpRequest, request, bigArrays, handlingSettings,
            threadPool.getThreadContext(), CorsHandler.fromSettings(settings), null);
        TestRestResponse resp = new TestRestResponse();
        final String customHeader = "custom-header";
        final String customHeaderValue = "xyz";
        resp.addHeader(customHeader, customHeaderValue);
        channel.sendResponse(resp);

        // inspect what was written
        ArgumentCaptor<TestHttpResponse> responseCaptor = ArgumentCaptor.forClass(TestHttpResponse.class);
        verify(httpChannel).sendResponse(responseCaptor.capture(), any());
        TestHttpResponse httpResponse = responseCaptor.getValue();
        Map<String, List<String>> headers = httpResponse.headers();
        assertNull(headers.get("non-existent-header"));
        assertEquals(customHeaderValue, headers.get(customHeader).get(0));
        assertEquals("abc", headers.get(Task.X_OPAQUE_ID).get(0));
        assertEquals(Integer.toString(resp.content().length()), headers.get(DefaultRestChannel.CONTENT_LENGTH).get(0));
        assertEquals(resp.contentType(), headers.get(DefaultRestChannel.CONTENT_TYPE).get(0));
    }

    public void testCookiesSet() {
        Settings settings = Settings.builder().put(HttpTransportSettings.SETTING_HTTP_RESET_COOKIES.getKey(), true).build();
        final TestHttpRequest httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        httpRequest.getHeaders().put(Task.X_OPAQUE_ID, Collections.singletonList("abc"));
        final RestRequest request = RestRequest.request(xContentRegistry(), httpRequest, httpChannel);
        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        // send a response
        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, httpRequest, request, bigArrays, handlingSettings,
            threadPool.getThreadContext(), CorsHandler.fromSettings(settings), null);
        channel.sendResponse(new TestRestResponse());

        // inspect what was written
        ArgumentCaptor<TestHttpResponse> responseCaptor = ArgumentCaptor.forClass(TestHttpResponse.class);
        verify(httpChannel).sendResponse(responseCaptor.capture(), any());
        TestHttpResponse nioResponse = responseCaptor.getValue();
        Map<String, List<String>> headers = nioResponse.headers();
        assertThat(headers.get(DefaultRestChannel.SET_COOKIE), hasItem("cookie"));
        assertThat(headers.get(DefaultRestChannel.SET_COOKIE), hasItem("cookie2"));
    }

    @SuppressWarnings("unchecked")
    public void testReleaseInListener() throws IOException {
        final Settings settings = Settings.builder().build();
        final TestHttpRequest httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        final RestRequest request = RestRequest.request(xContentRegistry(), httpRequest, httpChannel);
        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, httpRequest, request, bigArrays, handlingSettings,
            threadPool.getThreadContext(), CorsHandler.fromSettings(settings), null);
        final BytesRestResponse response = new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR,
            JsonXContent.contentBuilder().startObject().endObject());
        assertThat(response.content(), not(instanceOf(Releasable.class)));

        // ensure we have reserved bytes
        if (randomBoolean()) {
            BytesStreamOutput out = channel.bytesOutput();
            assertThat(out, instanceOf(ReleasableBytesStreamOutput.class));
        } else {
            try (XContentBuilder builder = channel.newBuilder()) {
                // do something builder
                builder.startObject().endObject();
            }
        }

        channel.sendResponse(response);
        Class<ActionListener<Void>> listenerClass = (Class<ActionListener<Void>>) (Class) ActionListener.class;
        ArgumentCaptor<ActionListener<Void>> listenerCaptor = ArgumentCaptor.forClass(listenerClass);
        verify(httpChannel).sendResponse(any(), listenerCaptor.capture());
        ActionListener<Void> listener = listenerCaptor.getValue();
        if (randomBoolean()) {
            listener.onResponse(null);
        } else {
            listener.onFailure(new ClosedChannelException());
        }
        // HavenaskTestCase#after will invoke ensureAllArraysAreReleased which will fail if the response content was not released
    }

    @SuppressWarnings("unchecked")
    public void testConnectionClose() throws Exception {
        final Settings settings = Settings.builder().build();
        final HttpRequest httpRequest;
        final boolean brokenRequest = randomBoolean();
        final boolean close = brokenRequest || randomBoolean();
        if (brokenRequest) {
            httpRequest = new TestHttpRequest(() -> {
                throw new IllegalArgumentException("Can't parse HTTP version");
            }, RestRequest.Method.GET, "/");
        } else if (randomBoolean()) {
            httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
            if (close) {
                httpRequest.getHeaders().put(DefaultRestChannel.CONNECTION, Collections.singletonList(DefaultRestChannel.CLOSE));
            }
        } else {
            httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_0, RestRequest.Method.GET, "/");
            if (!close) {
                httpRequest.getHeaders().put(DefaultRestChannel.CONNECTION, Collections.singletonList(DefaultRestChannel.KEEP_ALIVE));
            }
        }
        final RestRequest request = RestRequest.request(xContentRegistry(), httpRequest, httpChannel);

        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, httpRequest, request, bigArrays, handlingSettings,
            threadPool.getThreadContext(), CorsHandler.fromSettings(settings), null);
        channel.sendResponse(new TestRestResponse());
        Class<ActionListener<Void>> listenerClass = (Class<ActionListener<Void>>) (Class) ActionListener.class;
        ArgumentCaptor<ActionListener<Void>> listenerCaptor = ArgumentCaptor.forClass(listenerClass);
        verify(httpChannel).sendResponse(any(), listenerCaptor.capture());
        ActionListener<Void> listener = listenerCaptor.getValue();
        if (randomBoolean()) {
            listener.onResponse(null);
        } else {
            listener.onFailure(new ClosedChannelException());
        }
        if (close) {
            verify(httpChannel, times(1)).close();
        } else {
            verify(httpChannel, times(0)).close();
        }
    }

    public void testUnsupportedHttpMethod() {
        final boolean close = randomBoolean();
        final HttpRequest.HttpVersion httpVersion = close ? HttpRequest.HttpVersion.HTTP_1_0 : HttpRequest.HttpVersion.HTTP_1_1;
        final String httpConnectionHeaderValue = close ? DefaultRestChannel.CLOSE : DefaultRestChannel.KEEP_ALIVE;
        final RestRequest request = RestRequest.request(xContentRegistry(), new TestHttpRequest(httpVersion, null, "/") {
            @Override
            public RestRequest.Method method() {
                throw new IllegalArgumentException("test");
            }
        }, httpChannel);
        request.getHttpRequest().getHeaders().put(DefaultRestChannel.CONNECTION, Collections.singletonList(httpConnectionHeaderValue));

        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, request.getHttpRequest(), request, bigArrays,
            HttpHandlingSettings.fromSettings(Settings.EMPTY), threadPool.getThreadContext(), CorsHandler.fromSettings(Settings.EMPTY),
            null);

        // HavenaskTestCase#after will invoke ensureAllArraysAreReleased which will fail if the response content was not released
        final BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        final ByteArray byteArray = bigArrays.newByteArray(0, false);
        final BytesReference content = new ReleasableBytesReference(BytesReference.fromByteArray(byteArray, 0) , byteArray);
        channel.sendResponse(new TestRestResponse(RestStatus.METHOD_NOT_ALLOWED, content));

        Class<ActionListener<Void>> listenerClass = (Class<ActionListener<Void>>) (Class) ActionListener.class;
        ArgumentCaptor<ActionListener<Void>> listenerCaptor = ArgumentCaptor.forClass(listenerClass);
        verify(httpChannel).sendResponse(any(), listenerCaptor.capture());
        ActionListener<Void> listener = listenerCaptor.getValue();
        if (randomBoolean()) {
            listener.onResponse(null);
        } else {
            listener.onFailure(new ClosedChannelException());
        }
        if (close) {
            verify(httpChannel, times(1)).close();
        } else {
            verify(httpChannel, times(0)).close();
        }
    }

    public void testCloseOnException() {
        final boolean close = randomBoolean();
        final HttpRequest.HttpVersion httpVersion = close ? HttpRequest.HttpVersion.HTTP_1_0 : HttpRequest.HttpVersion.HTTP_1_1;
        final String httpConnectionHeaderValue = close ? DefaultRestChannel.CLOSE : DefaultRestChannel.KEEP_ALIVE;
        final RestRequest request = RestRequest.request(xContentRegistry(), new TestHttpRequest(httpVersion, null, "/") {
            @Override
            public HttpResponse createResponse(RestStatus status, BytesReference content) {
                throw new IllegalArgumentException("test");
            }
        }, httpChannel);
        request.getHttpRequest().getHeaders().put(DefaultRestChannel.CONNECTION, Collections.singletonList(httpConnectionHeaderValue));

        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, request.getHttpRequest(), request, bigArrays,
            HttpHandlingSettings.fromSettings(Settings.EMPTY), threadPool.getThreadContext(), CorsHandler.fromSettings(Settings.EMPTY),
            null);

        // HavenaskTestCase#after will invoke ensureAllArraysAreReleased which will fail if the response content was not released
        final BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        final ByteArray byteArray = bigArrays.newByteArray(0, false);
        final BytesReference content = new ReleasableBytesReference(BytesReference.fromByteArray(byteArray, 0) , byteArray);

        expectThrows(IllegalArgumentException.class, () -> channel.sendResponse(new TestRestResponse(RestStatus.OK, content)));

        if (close) {
            verify(httpChannel, times(1)).close();
        } else {
            verify(httpChannel, times(0)).close();
        }
    }

    private TestHttpResponse executeRequest(final Settings settings, final String host) {
        return executeRequest(settings, null, host);
    }

    private TestHttpResponse executeRequest(final Settings settings, final String originValue, final String host) {
        HttpRequest httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        if (originValue != null) {
            httpRequest.getHeaders().put(CorsHandler.ORIGIN, Collections.singletonList(originValue));
        }
        httpRequest.getHeaders().put(CorsHandler.HOST, Collections.singletonList(host));
        final RestRequest request = RestRequest.request(xContentRegistry(), httpRequest, httpChannel);

        HttpHandlingSettings httpHandlingSettings = HttpHandlingSettings.fromSettings(settings);
        RestChannel channel = new DefaultRestChannel(httpChannel, httpRequest, request, bigArrays, httpHandlingSettings,
            threadPool.getThreadContext(), new CorsHandler(CorsHandler.buildConfig(settings)), null);
        channel.sendResponse(new TestRestResponse());

        // get the response
        ArgumentCaptor<TestHttpResponse> responseCaptor = ArgumentCaptor.forClass(TestHttpResponse.class);
        verify(httpChannel, atLeastOnce()).sendResponse(responseCaptor.capture(), any());
        return responseCaptor.getValue();
    }

    private static class TestRestResponse extends RestResponse {

        private final RestStatus status;
        private final BytesReference content;

        TestRestResponse(final RestStatus status, final BytesReference content) {
            this.status = Objects.requireNonNull(status);
            this.content = Objects.requireNonNull(content);
        }

        TestRestResponse() {
            this(RestStatus.OK, new BytesArray("content".getBytes(StandardCharsets.UTF_8)));
        }

        public String contentType() {
            return "text";
        }

        public BytesReference content() {
            return content;
        }

        public RestStatus status() {
            return status;
        }
    }
}
