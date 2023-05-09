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

package org.havenask.http.netty4;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCounted;
import org.havenask.HavenaskNetty4IntegTestCase;
import org.havenask.common.collect.Tuple;
import org.havenask.common.settings.Settings;
import org.havenask.common.transport.TransportAddress;
import org.havenask.common.unit.ByteSizeUnit;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.http.HttpServerTransport;
import org.havenask.indices.breaker.HierarchyCircuitBreakerService;
import org.havenask.test.HavenaskIntegTestCase.ClusterScope;
import org.havenask.test.HavenaskIntegTestCase.Scope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * This test checks that in-flight requests are limited on HTTP level and that requests that are excluded from limiting can pass.
 *
 * As the same setting is also used to limit in-flight requests on transport level, we avoid transport messages by forcing
 * a single node "cluster". We also force test infrastructure to use the node client instead of the transport client for the same reason.
 */
@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numClientNodes = 0, numDataNodes = 1, transportClientRatio = 0)
public class Netty4HttpRequestSizeLimitIT extends HavenaskNetty4IntegTestCase {

    private static final ByteSizeValue LIMIT = new ByteSizeValue(2, ByteSizeUnit.KB);

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), LIMIT)
            .build();
    }

    public void testLimitsInFlightRequests() throws Exception {
        ensureGreen();

        // we use the limit size as a (very) rough indication on how many requests we should sent to hit the limit
        int numRequests = LIMIT.bytesAsInt() / 100;

        StringBuilder bulkRequest = new StringBuilder();
        for (int i = 0; i < numRequests; i++) {
            bulkRequest.append("{\"index\": {}}");
            bulkRequest.append(System.lineSeparator());
            bulkRequest.append("{ \"field\" : \"value\" }");
            bulkRequest.append(System.lineSeparator());
        }

        List<Tuple<String, CharSequence>> requests = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
            requests.add(Tuple.tuple("/index/type/_bulk", bulkRequest));
        }

        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress transportAddress = randomFrom(httpServerTransport.boundAddress().boundAddresses());

        try (Netty4HttpClient nettyHttpClient = new Netty4HttpClient()) {
            Collection<FullHttpResponse> singleResponse = nettyHttpClient.post(transportAddress.address(), requests.subList(0, 1));
            try {
                assertThat(singleResponse, hasSize(1));
                assertAtLeastOnceExpectedStatus(singleResponse, HttpResponseStatus.OK);

                Collection<FullHttpResponse> multipleResponses = nettyHttpClient.post(transportAddress.address(), requests);
                try {
                    assertThat(multipleResponses, hasSize(requests.size()));
                    assertAtLeastOnceExpectedStatus(multipleResponses, HttpResponseStatus.TOO_MANY_REQUESTS);
                } finally {
                    multipleResponses.forEach(ReferenceCounted::release);
                }
            } finally {
                singleResponse.forEach(ReferenceCounted::release);
            }
        }
    }

    public void testDoesNotLimitExcludedRequests() throws Exception {
        ensureGreen();

        List<Tuple<String, CharSequence>> requestUris = new ArrayList<>();
        for (int i = 0; i < 1500; i++) {
            requestUris.add(Tuple.tuple("/_cluster/settings",
                "{ \"transient\": {\"search.default_search_timeout\": \"40s\" } }"));
        }

        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress transportAddress = randomFrom(httpServerTransport.boundAddress().boundAddresses());

        try (Netty4HttpClient nettyHttpClient = new Netty4HttpClient()) {
            Collection<FullHttpResponse> responses = nettyHttpClient.put(transportAddress.address(), requestUris);
            try {
                assertThat(responses, hasSize(requestUris.size()));
                assertAllInExpectedStatus(responses, HttpResponseStatus.OK);
            } finally {
                responses.forEach(ReferenceCounted::release);
            }
        }
    }

    private void assertAtLeastOnceExpectedStatus(Collection<FullHttpResponse> responses, HttpResponseStatus expectedStatus) {
        long countExpectedStatus = responses.stream().filter(r -> r.status().equals(expectedStatus)).count();
        assertThat("Expected at least one request with status [" + expectedStatus + "]", countExpectedStatus, greaterThan(0L));
    }

    private void assertAllInExpectedStatus(Collection<FullHttpResponse> responses, HttpResponseStatus expectedStatus) {
        long countUnexpectedStatus = responses.stream().filter(r -> r.status().equals(expectedStatus) == false).count();
        assertThat("Expected all requests with status [" + expectedStatus + "] but [" + countUnexpectedStatus +
            "] requests had a different one", countUnexpectedStatus, equalTo(0L));
    }

}
