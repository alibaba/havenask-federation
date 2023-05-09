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

package org.havenask.test.rest.yaml.section;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.client.HasAttributeNodeSelector;
import org.havenask.client.Node;
import org.havenask.client.NodeSelector;
import org.havenask.common.ParsingException;
import org.havenask.common.Strings;
import org.havenask.common.collect.Tuple;
import org.havenask.common.logging.HeaderWarning;
import org.havenask.common.xcontent.DeprecationHandler;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentLocation;
import org.havenask.common.xcontent.XContentParseException;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.test.rest.yaml.ClientYamlTestExecutionContext;
import org.havenask.test.rest.yaml.ClientYamlTestResponse;
import org.havenask.test.rest.yaml.ClientYamlTestResponseException;
import org.havenask.test.hamcrest.RegexMatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;
import static org.havenask.common.collect.Tuple.tuple;
import static org.havenask.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Represents a do section:
 *
 *   - do:
 *      catch:      missing
 *      headers:
 *          Authorization: Basic user:pass
 *          Content-Type: application/json
 *      warnings:
 *          - Stuff is deprecated, yo
 *          - Don't use deprecated stuff
 *          - Please, stop. It hurts.
 *      allowed_warnings:
 *          - Maybe this warning shows up
 *          - But it isn't actually required for the test to pass.
 *      update:
 *          index:  test_1
 *          type:   test
 *          id:     1
 *          body:   { doc: { foo: bar } }
 *
 */
public class DoSection implements ExecutableSection {
    public static DoSection parse(XContentParser parser) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;

        DoSection doSection = new DoSection(parser.getTokenLocation());
        ApiCallSection apiCallSection = null;
        NodeSelector nodeSelector = NodeSelector.ANY;
        Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<String> expectedWarnings = new ArrayList<>();
        List<String> allowedWarnings = new ArrayList<>();

        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("expected [" + XContentParser.Token.START_OBJECT + "], " +
                    "found [" + parser.currentToken() + "], the do section is not properly indented");
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("catch".equals(currentFieldName)) {
                    doSection.setCatch(parser.text());
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "unsupported field [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("warnings".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) == XContentParser.Token.VALUE_STRING) {
                        expectedWarnings.add(parser.text());
                    }
                    if (token != XContentParser.Token.END_ARRAY) {
                        throw new ParsingException(parser.getTokenLocation(), "[warnings] must be a string array but saw [" + token + "]");
                    }
                } else if ("allowed_warnings".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) == XContentParser.Token.VALUE_STRING) {
                        allowedWarnings.add(parser.text());
                    }
                    if (token != XContentParser.Token.END_ARRAY) {
                        throw new ParsingException(parser.getTokenLocation(),
                                "[allowed_warnings] must be a string array but saw [" + token + "]");
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "unknown array [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("headers".equals(currentFieldName)) {
                    String headerName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            headerName = parser.currentName();
                        } else if (token.isValue()) {
                            headers.put(headerName, parser.text());
                        }
                    }
                } else if ("node_selector".equals(currentFieldName)) {
                    String selectorName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            selectorName = parser.currentName();
                        } else {
                            NodeSelector newSelector = buildNodeSelector(selectorName, parser);
                            nodeSelector = nodeSelector == NodeSelector.ANY ?
                                newSelector : new ComposeNodeSelector(nodeSelector, newSelector);
                        }
                    }
                } else if (currentFieldName != null) { // must be part of API call then
                    apiCallSection = new ApiCallSection(currentFieldName);
                    String paramName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            paramName = parser.currentName();
                        } else if (token.isValue()) {
                            if ("body".equals(paramName)) {
                                String body = parser.text();
                                XContentParser bodyParser = JsonXContent.jsonXContent
                                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, body);
                                //multiple bodies are supported e.g. in case of bulk provided as a whole string
                                while(bodyParser.nextToken() != null) {
                                    apiCallSection.addBody(bodyParser.mapOrdered());
                                }
                            } else {
                                apiCallSection.addParam(paramName, parser.text());
                            }
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            if ("body".equals(paramName)) {
                                apiCallSection.addBody(parser.mapOrdered());
                            }
                        }
                    }
                }
            }
        }
        try {
            if (apiCallSection == null) {
                throw new IllegalArgumentException("client call section is mandatory within a do section");
            }
            for (String w : expectedWarnings) {
                if (allowedWarnings.contains(w)) {
                    throw new IllegalArgumentException("the warning [" + w + "] was both allowed and expected");
                }
            }
            apiCallSection.addHeaders(headers);
            apiCallSection.setNodeSelector(nodeSelector);
            doSection.setApiCallSection(apiCallSection);
            doSection.setExpectedWarningHeaders(unmodifiableList(expectedWarnings));
            doSection.setAllowedWarningHeaders(unmodifiableList(allowedWarnings));
        } finally {
            parser.nextToken();
        }
        return doSection;
    }

    private static final Logger logger = LogManager.getLogger(DoSection.class);

    private final XContentLocation location;
    private String catchParam;
    private ApiCallSection apiCallSection;
    private List<String> expectedWarningHeaders = emptyList();
    private List<String> allowedWarningHeaders = emptyList();

    public DoSection(XContentLocation location) {
        this.location = location;
    }

    public String getCatch() {
        return catchParam;
    }

    public void setCatch(String catchParam) {
        this.catchParam = catchParam;
    }

    public ApiCallSection getApiCallSection() {
        return apiCallSection;
    }

    void setApiCallSection(ApiCallSection apiCallSection) {
        this.apiCallSection = apiCallSection;
    }

    /**
     * Warning headers that we expect from this response. If the headers don't match exactly this request is considered to have failed.
     * Defaults to emptyList.
     */
    List<String> getExpectedWarningHeaders() {
        return expectedWarningHeaders;
    }

    /**
     * Set the warning headers that we expect from this response. If the headers don't match exactly this request is considered to have
     * failed. Defaults to emptyList.
     */
    void setExpectedWarningHeaders(List<String> expectedWarningHeaders) {
        this.expectedWarningHeaders = expectedWarningHeaders;
    }

    /**
     * Warning headers that we allow from this response. These warning
     * headers don't cause the test to fail. Defaults to emptyList.
     */
    List<String> getAllowedWarningHeaders() {
        return allowedWarningHeaders;
    }

    /**
     * Set the warning headers that we expect from this response. These
     * warning headers don't cause the test to fail. Defaults to emptyList.
     */
    void setAllowedWarningHeaders(List<String> allowedWarningHeaders) {
        this.allowedWarningHeaders = allowedWarningHeaders;
    }

    @Override
    public XContentLocation getLocation() {
        return location;
    }

    @Override
    public void execute(ClientYamlTestExecutionContext executionContext) throws IOException {

        if ("param".equals(catchParam)) {
            //client should throw validation error before sending request
            //lets just return without doing anything as we don't have any client to test here
            logger.info("found [catch: param], no request sent");
            return;
        }

        try {
            ClientYamlTestResponse response = executionContext.callApi(apiCallSection.getApi(), apiCallSection.getParams(),
                    apiCallSection.getBodies(), apiCallSection.getHeaders(), apiCallSection.getNodeSelector());
            if (Strings.hasLength(catchParam)) {
                String catchStatusCode;
                if (catches.containsKey(catchParam)) {
                    catchStatusCode = catches.get(catchParam).v1();
                } else if (catchParam.startsWith("/") && catchParam.endsWith("/")) {
                    catchStatusCode = "4xx|5xx";
                } else {
                    throw new UnsupportedOperationException("catch value [" + catchParam + "] not supported");
                }
                fail(formatStatusCodeMessage(response, catchStatusCode));
            }
            checkWarningHeaders(response.getWarningHeaders(), executionContext.masterVersion());
        } catch(ClientYamlTestResponseException e) {
            ClientYamlTestResponse restTestResponse = e.getRestTestResponse();
            if (!Strings.hasLength(catchParam)) {
                fail(formatStatusCodeMessage(restTestResponse, "2xx"));
            } else if (catches.containsKey(catchParam)) {
                assertStatusCode(restTestResponse);
            } else if (catchParam.length() > 2 && catchParam.startsWith("/") && catchParam.endsWith("/")) {
                //the text of the error message matches regular expression
                assertThat(formatStatusCodeMessage(restTestResponse, "4xx|5xx"),
                        e.getResponseException().getResponse().getStatusLine().getStatusCode(), greaterThanOrEqualTo(400));
                Object error = executionContext.response("error");
                assertThat("error was expected in the response", error, notNullValue());
                //remove delimiters from regex
                String regex = catchParam.substring(1, catchParam.length() - 1);
                assertThat("the error message was expected to match the provided regex but didn't",
                        error.toString(), RegexMatcher.matches(regex));
            } else {
                throw new UnsupportedOperationException("catch value [" + catchParam + "] not supported");
            }
        }
    }

    /**
     * Check that the response contains only the warning headers that we expect.
     */
    void checkWarningHeaders(final List<String> warningHeaders, final Version masterVersion) {
        final List<String> unexpected = new ArrayList<>();
        final List<String> unmatched = new ArrayList<>();
        final List<String> missing = new ArrayList<>();
        Set<String> allowed = allowedWarningHeaders.stream()
                .map(HeaderWarning::escapeAndEncode)
                .collect(toSet());
        // LinkedHashSet so that missing expected warnings come back in a predictable order which is nice for testing
        final Set<String> expected = expectedWarningHeaders.stream()
                .map(HeaderWarning::escapeAndEncode)
                .collect(toCollection(LinkedHashSet::new));
        for (final String header : warningHeaders) {
            final Matcher matcher = HeaderWarning.WARNING_HEADER_PATTERN.matcher(header);
            final boolean matches = matcher.matches();
            if (matches) {
                final String message = HeaderWarning.extractWarningValueFromWarningHeader(header, true);
                if (masterVersion.before(LegacyESVersion.V_7_0_0)
                        && message.equals("the default number of shards will change from [5] to [1] in 7.0.0; "
                        + "if you wish to continue using the default of [5] shards, "
                        + "you must manage this on the create index request or with an index template")) {
                    /*
                     * This warning header will come back in the vast majority of our tests that create an index when running against an
                     * older master. Rather than rewrite our tests to assert this warning header, we assume that it is expected.
                     */
                    continue;
                }
                if (message.startsWith("[types removal]")) {
                    // We skip warnings related to types deprecation because they are *everywhere*.
                    continue;
                }
                if (allowed.contains(message)) {
                    continue;
                }
                if (expected.remove(message)) {
                    continue;
                }
                unexpected.add(header);
            } else {
                unmatched.add(header);
            }
        }
        if (expected.isEmpty() == false) {
            for (final String header : expected) {
                missing.add(header);
            }
        }

        if (unexpected.isEmpty() == false || unmatched.isEmpty() == false || missing.isEmpty() == false) {
            final StringBuilder failureMessage = new StringBuilder();
            appendBadHeaders(failureMessage, unexpected, "got unexpected warning header" + (unexpected.size() > 1 ? "s" : ""));
            appendBadHeaders(failureMessage, unmatched, "got unmatched warning header" + (unmatched.size() > 1 ? "s" : ""));
            appendBadHeaders(failureMessage, missing, "did not get expected warning header" + (missing.size() > 1 ? "s" : ""));
            fail(failureMessage.toString());
        }

    }

    private void appendBadHeaders(final StringBuilder sb, final List<String> headers, final String message) {
        if (headers.isEmpty() == false) {
            sb.append(message).append(" [\n");
            for (final String header : headers) {
                sb.append("\t").append(header).append("\n");
            }
            sb.append("]\n");
        }
    }

    private void assertStatusCode(ClientYamlTestResponse restTestResponse) {
        Tuple<String, org.hamcrest.Matcher<Integer>> stringMatcherTuple = catches.get(catchParam);
        assertThat(formatStatusCodeMessage(restTestResponse, stringMatcherTuple.v1()),
                restTestResponse.getStatusCode(), stringMatcherTuple.v2());
    }

    private String formatStatusCodeMessage(ClientYamlTestResponse restTestResponse, String expected) {
        String api = apiCallSection.getApi();
        if ("raw".equals(api)) {
            api += "[method=" + apiCallSection.getParams().get("method") + " path=" + apiCallSection.getParams().get("path") + "]";
        }
        return "expected [" + expected + "] status code but api [" + api + "] returned [" + restTestResponse.getStatusCode() +
                " " + restTestResponse.getReasonPhrase() + "] [" + restTestResponse.getBodyAsString() + "]";
    }

    private static Map<String, Tuple<String, org.hamcrest.Matcher<Integer>>> catches = new HashMap<>();

    static {
        catches.put("bad_request", tuple("400", equalTo(400)));
        catches.put("unauthorized", tuple("401", equalTo(401)));
        catches.put("forbidden", tuple("403", equalTo(403)));
        catches.put("missing", tuple("404", equalTo(404)));
        catches.put("request_timeout", tuple("408", equalTo(408)));
        catches.put("conflict", tuple("409", equalTo(409)));
        catches.put("unavailable", tuple("503", equalTo(503)));
        catches.put("request", tuple("4xx|5xx", allOf(greaterThanOrEqualTo(400),
                not(equalTo(400)),
                not(equalTo(401)),
                not(equalTo(403)),
                not(equalTo(404)),
                not(equalTo(408)),
                not(equalTo(409)))));
    }

    private static NodeSelector buildNodeSelector(String name, XContentParser parser) throws IOException {
        switch (name) {
        case "attribute":
            return parseAttributeValuesSelector(parser);
        case "version":
            return parseVersionSelector(parser);
        default:
            throw new XContentParseException(parser.getTokenLocation(), "unknown node_selector [" + name + "]");
        }
    }

    private static NodeSelector parseAttributeValuesSelector(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new XContentParseException(parser.getTokenLocation(), "expected START_OBJECT");
        }
        String key = null;
        XContentParser.Token token;
        NodeSelector result = NodeSelector.ANY;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                key = parser.currentName();
            } else if (token.isValue()) {
                /*
                 * HasAttributeNodeSelector selects nodes that do not have
                 * attribute metadata set so it can be used against nodes that
                 * have not yet been sniffed. In these tests we expect the node
                 * metadata to be explicitly sniffed if we need it and we'd
                 * like to hard fail if it is not so we wrap the selector so we
                 * can assert that the data is sniffed.
                 */
                NodeSelector delegate = new HasAttributeNodeSelector(key, parser.text());
                NodeSelector newSelector = new NodeSelector() {
                    @Override
                    public void select(Iterable<Node> nodes) {
                        for (Node node : nodes) {
                            if (node.getAttributes() == null) {
                                throw new IllegalStateException("expected [attributes] metadata to be set but got "
                                        + node);
                            }
                        }
                        delegate.select(nodes);
                    }

                    @Override
                    public String toString() {
                        return delegate.toString();
                    }
                };
                result = result == NodeSelector.ANY ?
                    newSelector : new ComposeNodeSelector(result, newSelector);
            } else {
                throw new XContentParseException(parser.getTokenLocation(), "expected [" + key + "] to be a value");
            }
        }
        return result;
    }

    private static NodeSelector parseVersionSelector(XContentParser parser) throws IOException {
        if (false == parser.currentToken().isValue()) {
            throw new XContentParseException(parser.getTokenLocation(), "expected [version] to be a value");
        }
        List<VersionRange> skipVersionRanges = SkipSection.parseVersionRanges(parser.text());
        return new NodeSelector() {
            @Override
            public void select(Iterable<Node> nodes) {
                for (Iterator<Node> itr = nodes.iterator(); itr.hasNext();) {
                    Node node = itr.next();
                    if (node.getVersion() == null) {
                        throw new IllegalStateException("expected [version] metadata to be set but got "
                                + node);
                    }
                    Version version = Version.fromString(node.getVersion());
                    boolean skip = skipVersionRanges.stream().anyMatch(v -> v.contains(version));
                    if (false == skip) {
                        itr.remove();
                    }
                }
            }

            @Override
            public String toString() {
                return "version ranges "+skipVersionRanges;
            }
        };
    }

    /**
     * Selector that composes two selectors, running the "right" most selector
     * first and then running the "left" selector on the results of the "right"
     * selector.
     */
    private static class ComposeNodeSelector implements NodeSelector {
        private final NodeSelector lhs;
        private final NodeSelector rhs;

        private ComposeNodeSelector(NodeSelector lhs, NodeSelector rhs) {
            this.lhs = Objects.requireNonNull(lhs, "lhs is required");
            this.rhs = Objects.requireNonNull(rhs, "rhs is required");
        }

        @Override
        public void select(Iterable<Node> nodes) {
            rhs.select(nodes);
            lhs.select(nodes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ComposeNodeSelector that = (ComposeNodeSelector) o;
            return Objects.equals(lhs, that.lhs) &&
                    Objects.equals(rhs, that.rhs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lhs, rhs);
        }

        @Override
        public String toString() {
            // . as in haskell's "compose" operator
            return lhs + "." + rhs;
        }
    }
}
