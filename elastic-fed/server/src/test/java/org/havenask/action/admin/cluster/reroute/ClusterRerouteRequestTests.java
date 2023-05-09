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

package org.havenask.action.admin.cluster.reroute;

import org.havenask.action.support.master.AcknowledgedRequest;
import org.havenask.action.support.master.MasterNodeRequest;
import org.havenask.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.havenask.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.havenask.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.havenask.cluster.routing.allocation.command.AllocationCommand;
import org.havenask.cluster.routing.allocation.command.CancelAllocationCommand;
import org.havenask.cluster.routing.allocation.command.MoveAllocationCommand;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.NamedWriteableAwareStreamInput;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.network.NetworkModule;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.rest.RestRequest;
import org.havenask.rest.action.admin.cluster.RestClusterRerouteAction;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.rest.FakeRestRequest;
import org.havenask.action.admin.cluster.reroute.ClusterRerouteRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableList;
import static org.havenask.common.unit.TimeValue.timeValueMillis;

/**
 * Test for serialization and parsing of {@link ClusterRerouteRequest} and its commands. See the superclass for, well, everything.
 */
public class ClusterRerouteRequestTests extends HavenaskTestCase {
    private static final int ROUNDS = 30;
    private final List<Supplier<AllocationCommand>> RANDOM_COMMAND_GENERATORS = unmodifiableList(
            Arrays.<Supplier<AllocationCommand>> asList(
            () -> new AllocateReplicaAllocationCommand(randomAlphaOfLengthBetween(2, 10), between(0, 1000),
                    randomAlphaOfLengthBetween(2, 10)),
            () -> new AllocateEmptyPrimaryAllocationCommand(randomAlphaOfLengthBetween(2, 10), between(0, 1000),
                    randomAlphaOfLengthBetween(2, 10), randomBoolean()),
            () -> new AllocateStalePrimaryAllocationCommand(randomAlphaOfLengthBetween(2, 10), between(0, 1000),
                    randomAlphaOfLengthBetween(2, 10), randomBoolean()),
            () -> new CancelAllocationCommand(randomAlphaOfLengthBetween(2, 10), between(0, 1000),
                    randomAlphaOfLengthBetween(2, 10), randomBoolean()),
            () -> new MoveAllocationCommand(randomAlphaOfLengthBetween(2, 10), between(0, 1000),
                    randomAlphaOfLengthBetween(2, 10), randomAlphaOfLengthBetween(2, 10))));
    private final NamedWriteableRegistry namedWriteableRegistry;

    public ClusterRerouteRequestTests() {
        namedWriteableRegistry = new NamedWriteableRegistry(NetworkModule.getNamedWriteables());
    }

    private ClusterRerouteRequest randomRequest() {
        ClusterRerouteRequest request = new ClusterRerouteRequest();
        int commands = between(0, 10);
        for (int i = 0; i < commands; i++) {
            request.add(randomFrom(RANDOM_COMMAND_GENERATORS).get());
        }
        request.dryRun(randomBoolean());
        request.explain(randomBoolean());
        request.setRetryFailed(randomBoolean());
        return request;
    }

    public void testEqualsAndHashCode() {
        for (int round = 0; round < ROUNDS; round++) {
            ClusterRerouteRequest request = randomRequest();
            assertEquals(request, request);
            assertEquals(request.hashCode(), request.hashCode());

            ClusterRerouteRequest copy = new ClusterRerouteRequest()
                    .add(request.getCommands().commands().toArray(new AllocationCommand[0]));
            copy.dryRun(request.dryRun()).explain(request.explain()).timeout(request.timeout()).setRetryFailed(request.isRetryFailed());
            copy.masterNodeTimeout(request.masterNodeTimeout());
            assertEquals(request, copy);
            assertEquals(copy, request); // Commutative
            assertEquals(request.hashCode(), copy.hashCode());

            // Changing dryRun makes requests not equal
            copy.dryRun(!copy.dryRun());
            assertNotEquals(request, copy);
            assertNotEquals(request.hashCode(), copy.hashCode());
            copy.dryRun(!copy.dryRun());
            assertEquals(request, copy);
            assertEquals(request.hashCode(), copy.hashCode());

            // Changing explain makes requests not equal
            copy.explain(!copy.explain());
            assertNotEquals(request, copy);
            assertNotEquals(request.hashCode(), copy.hashCode());
            copy.explain(!copy.explain());
            assertEquals(request, copy);
            assertEquals(request.hashCode(), copy.hashCode());

            // Changing timeout makes requests not equal
            copy.timeout(timeValueMillis(request.timeout().millis() + 1));
            assertNotEquals(request, copy);
            assertNotEquals(request.hashCode(), copy.hashCode());
            copy.timeout(request.timeout());
            assertEquals(request, copy);
            assertEquals(request.hashCode(), copy.hashCode());

            // Changing masterNodeTime makes requests not equal
            copy.masterNodeTimeout(timeValueMillis(request.masterNodeTimeout().millis() + 1));
            assertNotEquals(request, copy);
            assertNotEquals(request.hashCode(), copy.hashCode());
            copy.masterNodeTimeout(request.masterNodeTimeout());
            assertEquals(request, copy);
            assertEquals(request.hashCode(), copy.hashCode());

            // Changing commands makes requests not equal
            copy.add(randomFrom(RANDOM_COMMAND_GENERATORS).get());
            assertNotEquals(request, copy);
            // Can't check hashCode because we can't be sure that changing commands changes the hashCode. It usually does but might not.
        }
    }

    public void testSerialization() throws IOException {
        for (int round = 0; round < ROUNDS; round++) {
            ClusterRerouteRequest request = randomRequest();
            ClusterRerouteRequest copy = roundTripThroughBytes(request);
            assertEquals(request, copy);
            assertEquals(request.hashCode(), copy.hashCode());
            assertNotSame(request, copy);
        }
    }

    public void testParsing() throws IOException {
        for (int round = 0; round < ROUNDS; round++) {
            ClusterRerouteRequest request = randomRequest();
            ClusterRerouteRequest copy = roundTripThroughRestRequest(request);
            assertEquals(request, copy);
            assertEquals(request.hashCode(), copy.hashCode());
            assertNotSame(request, copy);
        }
    }

    private ClusterRerouteRequest roundTripThroughBytes(ClusterRerouteRequest original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                return new ClusterRerouteRequest(in);
            }
        }
    }

    private ClusterRerouteRequest roundTripThroughRestRequest(ClusterRerouteRequest original) throws IOException {
        RestRequest restRequest = toRestRequest(original);
        return RestClusterRerouteAction.createRequest(restRequest);
    }

    private RestRequest toRestRequest(ClusterRerouteRequest original) throws IOException {
        Map<String, String> params = new HashMap<>();
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        boolean hasBody = false;
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        builder.startObject();
        if (randomBoolean()) {
            params.put("dry_run", Boolean.toString(original.dryRun()));
        } else {
            hasBody = true;
            builder.field("dry_run", original.dryRun());
        }
        params.put("explain", Boolean.toString(original.explain()));
        if (false == original.timeout().equals(AcknowledgedRequest.DEFAULT_ACK_TIMEOUT) || randomBoolean()) {
            params.put("timeout", original.timeout().toString());
        }
        if (original.isRetryFailed() || randomBoolean()) {
            params.put("retry_failed", Boolean.toString(original.isRetryFailed()));
        }
        if (false == original.masterNodeTimeout().equals(MasterNodeRequest.DEFAULT_MASTER_NODE_TIMEOUT) || randomBoolean()) {
            params.put("master_timeout", original.masterNodeTimeout().toString());
        }
        if (original.getCommands() != null) {
            hasBody = true;
            original.getCommands().toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endObject();

        FakeRestRequest.Builder requestBuilder = new FakeRestRequest.Builder(xContentRegistry());
        requestBuilder.withParams(params);
        if (hasBody) {
            requestBuilder.withContent(BytesReference.bytes(builder), builder.contentType());
        }
        return requestBuilder.build();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(NetworkModule.getNamedXContents());
    }
}
