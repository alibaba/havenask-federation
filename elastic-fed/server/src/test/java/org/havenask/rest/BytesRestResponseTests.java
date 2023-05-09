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

package org.havenask.rest;

import org.havenask.ExceptionsHelper;
import org.havenask.HavenaskStatusException;
import org.havenask.HavenaskException;
import org.havenask.ResourceAlreadyExistsException;
import org.havenask.ResourceNotFoundException;
import org.havenask.action.OriginalIndices;
import org.havenask.action.search.SearchPhaseExecutionException;
import org.havenask.action.search.ShardSearchFailure;
import org.havenask.common.ParsingException;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.transport.TransportAddress;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.shard.ShardId;
import org.havenask.search.SearchShardTarget;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.rest.FakeRestRequest;
import org.havenask.transport.RemoteTransportException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.havenask.HavenaskExceptionTests.assertDeepEquals;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class BytesRestResponseTests extends HavenaskTestCase {

    class UnknownException extends Exception {
        UnknownException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    public void testWithHeaders() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = randomBoolean() ? new DetailedExceptionRestChannel(request) : new SimpleExceptionRestChannel(request);

        BytesRestResponse response = new BytesRestResponse(channel, new WithHeadersException());
        assertEquals(2, response.getHeaders().size());
        assertThat(response.getHeaders().get("n1"), notNullValue());
        assertThat(response.getHeaders().get("n1"), contains("v11", "v12"));
        assertThat(response.getHeaders().get("n2"), notNullValue());
        assertThat(response.getHeaders().get("n2"), contains("v21", "v22"));
    }

    public void testSimpleExceptionMessage() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new SimpleExceptionRestChannel(request);

        Exception t = new HavenaskException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        BytesRestResponse response = new BytesRestResponse(channel, t);
        String text = response.content().utf8ToString();
        assertThat(text, containsString("HavenaskException[an error occurred reading data]"));
        assertThat(text, not(containsString("FileNotFoundException")));
        assertThat(text, not(containsString("/foo/bar")));
        assertThat(text, not(containsString("error_trace")));
    }

    public void testDetailedExceptionMessage() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new DetailedExceptionRestChannel(request);

        Exception t = new HavenaskException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        BytesRestResponse response = new BytesRestResponse(channel, t);
        String text = response.content().utf8ToString();
        assertThat(text, containsString("{\"type\":\"exception\",\"reason\":\"an error occurred reading data\"}"));
        assertThat(text, containsString("{\"type\":\"file_not_found_exception\",\"reason\":\"/foo/bar\"}"));
    }

    public void testNonHavenaskExceptionIsNotShownAsSimpleMessage() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new SimpleExceptionRestChannel(request);

        Exception t = new UnknownException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        BytesRestResponse response = new BytesRestResponse(channel, t);
        String text = response.content().utf8ToString();
        assertThat(text, not(containsString("UnknownException[an error occurred reading data]")));
        assertThat(text, not(containsString("FileNotFoundException[/foo/bar]")));
        assertThat(text, not(containsString("error_trace")));
        assertThat(text, containsString("\"error\":\"No HavenaskException found\""));
    }

    public void testErrorTrace() throws Exception {
        RestRequest request = new FakeRestRequest();
        request.params().put("error_trace", "true");
        RestChannel channel = new DetailedExceptionRestChannel(request);

        Exception t = new UnknownException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        BytesRestResponse response = new BytesRestResponse(channel, t);
        String text = response.content().utf8ToString();
        assertThat(text, containsString("\"type\":\"unknown_exception\",\"reason\":\"an error occurred reading data\""));
        assertThat(text, containsString("{\"type\":\"file_not_found_exception\""));
        assertThat(text, containsString("\"stack_trace\":\"[an error occurred reading data]"));
    }

    public void testGuessRootCause() throws IOException {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new DetailedExceptionRestChannel(request);
        {
            Exception e = new HavenaskException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
            BytesRestResponse response = new BytesRestResponse(channel, e);
            String text = response.content().utf8ToString();
            assertThat(text, containsString("{\"root_cause\":[{\"type\":\"exception\",\"reason\":\"an error occurred reading data\"}]"));
        }
        {
            Exception e = new FileNotFoundException("/foo/bar");
            BytesRestResponse response = new BytesRestResponse(channel, e);
            String text = response.content().utf8ToString();
            assertThat(text, containsString("{\"root_cause\":[{\"type\":\"file_not_found_exception\",\"reason\":\"/foo/bar\"}]"));
        }
    }

    public void testNullThrowable() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new SimpleExceptionRestChannel(request);

        BytesRestResponse response = new BytesRestResponse(channel, null);
        String text = response.content().utf8ToString();
        assertThat(text, containsString("\"error\":\"unknown\""));
        assertThat(text, not(containsString("error_trace")));
    }

    public void testConvert() throws IOException {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new DetailedExceptionRestChannel(request);
        ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                new SearchShardTarget("node_1", new ShardId("foo", "_na_", 1), null, OriginalIndices.NONE));
        ShardSearchFailure failure1 = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                new SearchShardTarget("node_1", new ShardId("foo", "_na_", 2), null, OriginalIndices.NONE));
        SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed",
            new ShardSearchFailure[] {failure, failure1});
        BytesRestResponse response = new BytesRestResponse(channel, new RemoteTransportException("foo", ex));
        String text = response.content().utf8ToString();
        String expected = "{\"error\":{\"root_cause\":[{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}]," +
            "\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\",\"phase\":\"search\",\"grouped\":true," +
            "\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\",\"reason\":{\"type\":\"parsing_exception\"," +
            "\"reason\":\"foobar\",\"line\":1,\"col\":2}}]},\"status\":400}";
        assertEquals(expected.trim(), text.trim());
        String stackTrace = ExceptionsHelper.stackTrace(ex);
        assertTrue(stackTrace.contains("Caused by: ParsingException[foobar]"));
    }

    public void testResponseWhenPathContainsEncodingError() throws IOException {
        final String path = "%a";
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(path).build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RestUtils.decodeComponent(request.rawPath()));
        final RestChannel channel = new DetailedExceptionRestChannel(request);
        // if we try to decode the path, this will throw an IllegalArgumentException again
        final BytesRestResponse response = new BytesRestResponse(channel, e);
        assertNotNull(response.content());
        final String content = response.content().utf8ToString();
        assertThat(content, containsString("\"type\":\"illegal_argument_exception\""));
        assertThat(content, containsString("\"reason\":\"partial escape sequence at end of string: %a\""));
        assertThat(content, containsString("\"status\":" + 400));
    }

    public void testResponseWhenInternalServerError() throws IOException {
        final RestRequest request = new FakeRestRequest();
        final RestChannel channel = new DetailedExceptionRestChannel(request);
        final BytesRestResponse response = new BytesRestResponse(channel, new HavenaskException("simulated"));
        assertNotNull(response.content());
        final String content = response.content().utf8ToString();
        assertThat(content, containsString("\"type\":\"exception\""));
        assertThat(content, containsString("\"reason\":\"simulated\""));
        assertThat(content, containsString("\"status\":" + 500));
    }

    public void testErrorToAndFromXContent() throws IOException {
        final boolean detailed = randomBoolean();

        Exception original;
        HavenaskException cause = null;
        String reason;
        String type = "exception";
        RestStatus status = RestStatus.INTERNAL_SERVER_ERROR;
        boolean addHeadersOrMetadata = false;

        switch (randomIntBetween(0, 5)) {
            case 0:
                original = new HavenaskException("HavenaskException without cause");
                if (detailed) {
                    addHeadersOrMetadata = randomBoolean();
                    reason = "HavenaskException without cause";
                } else {
                    reason = "HavenaskException[HavenaskException without cause]";
                }
                break;
            case 1:
                original = new HavenaskException("HavenaskException with a cause", new FileNotFoundException("missing"));
                if (detailed) {
                    addHeadersOrMetadata = randomBoolean();
                    type = "exception";
                    reason = "HavenaskException with a cause";
                    cause = new HavenaskException("Havenask exception [type=file_not_found_exception, reason=missing]");
                } else {
                    reason = "HavenaskException[HavenaskException with a cause]";
                }
                break;
            case 2:
                original = new ResourceNotFoundException("HavenaskException with custom status");
                status = RestStatus.NOT_FOUND;
                if (detailed) {
                    addHeadersOrMetadata = randomBoolean();
                    type = "resource_not_found_exception";
                    reason = "HavenaskException with custom status";
                } else {
                    reason = "ResourceNotFoundException[HavenaskException with custom status]";
                }
                break;
            case 3:
                TransportAddress address = buildNewFakeTransportAddress();
                original = new RemoteTransportException("remote", address, "action",
                        new ResourceAlreadyExistsException("HavenaskWrapperException with a cause that has a custom status"));
                status = RestStatus.BAD_REQUEST;
                if (detailed) {
                    type = "resource_already_exists_exception";
                    reason = "HavenaskWrapperException with a cause that has a custom status";
                } else {
                    reason = "RemoteTransportException[[remote][" + address.toString() + "][action]]";
                }
                break;
            case 4:
                original = new RemoteTransportException("HavenaskWrapperException with a cause that has a special treatment",
                        new IllegalArgumentException("wrong"));
                status = RestStatus.BAD_REQUEST;
                if (detailed) {
                    type = "illegal_argument_exception";
                    reason = "wrong";
                } else {
                    reason = "RemoteTransportException[[HavenaskWrapperException with a cause that has a special treatment]]";
                }
                break;
            case 5:
                status = randomFrom(RestStatus.values());
                original = new HavenaskStatusException("HavenaskStatusException with random status", status);
                if (detailed) {
                    addHeadersOrMetadata = randomBoolean();
                    type = "status_exception";
                    reason = "HavenaskStatusException with random status";
                } else {
                    reason = "HavenaskStatusException[HavenaskStatusException with random status]";
                }
                break;
            default:
                throw new UnsupportedOperationException("Failed to generate random exception");
        }

        String message = "Havenask exception [type=" + type + ", reason=" + reason + "]";
        HavenaskStatusException expected = new HavenaskStatusException(message, status, cause);

        if (addHeadersOrMetadata) {
            HavenaskException originalException = ((HavenaskException) original);
            if (randomBoolean()) {
                originalException.addHeader("foo", "bar", "baz");
                expected.addHeader("foo", "bar", "baz");
            }
            if (randomBoolean()) {
                originalException.addMetadata("havenask.metadata_0", "0");
                expected.addMetadata("havenask.metadata_0", "0");
            }
            if (randomBoolean()) {
                String resourceType = randomAlphaOfLength(5);
                String resourceId = randomAlphaOfLength(5);
                originalException.setResources(resourceType, resourceId);
                expected.setResources(resourceType, resourceId);
            }
            if (randomBoolean()) {
                originalException.setIndex("_index");
                expected.setIndex("_index");
            }
        }

        final XContentType xContentType = randomFrom(XContentType.values());

        Map<String, String> params = Collections.singletonMap("format", xContentType.mediaType());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        RestChannel channel = detailed ? new DetailedExceptionRestChannel(request) : new SimpleExceptionRestChannel(request);

        BytesRestResponse response = new BytesRestResponse(channel, original);

        HavenaskException parsedError;
        try (XContentParser parser = createParser(xContentType.xContent(), response.content())) {
            parsedError = BytesRestResponse.errorFromXContent(parser);
            assertNull(parser.nextToken());
        }

        assertEquals(expected.status(), parsedError.status());
        assertDeepEquals(expected, parsedError);
    }

    public void testNoErrorFromXContent() throws IOException {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            try (XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.values()).xContent())) {
                builder.startObject();
                builder.field("status", randomFrom(RestStatus.values()).getStatus());
                builder.endObject();

                try (XContentParser parser = createParser(builder.contentType().xContent(), BytesReference.bytes(builder))) {
                    BytesRestResponse.errorFromXContent(parser);
                }
            }
        });
        assertEquals("Failed to parse havenask status exception: no exception was found", e.getMessage());
    }

    public static class WithHeadersException extends HavenaskException {

        WithHeadersException() {
            super("");
            this.addHeader("n1", "v11", "v12");
            this.addHeader("n2", "v21", "v22");
            this.addMetadata("havenask.test", "value1", "value2");
        }
    }

    private static class SimpleExceptionRestChannel extends AbstractRestChannel {

        SimpleExceptionRestChannel(RestRequest request) {
            super(request, false);
        }

        @Override
        public void sendResponse(RestResponse response) {
        }
    }

    private static class DetailedExceptionRestChannel extends AbstractRestChannel {

        DetailedExceptionRestChannel(RestRequest request) {
            super(request, true);
        }

        @Override
        public void sendResponse(RestResponse response) {
        }
    }
}
