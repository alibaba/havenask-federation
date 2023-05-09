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

package org.havenask.action.support.replication;

import org.havenask.HavenaskException;
import org.havenask.action.support.replication.ReplicationResponse.ShardInfo;
import org.havenask.common.Strings;
import org.havenask.common.breaker.CircuitBreaker;
import org.havenask.common.breaker.CircuitBreakingException;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.collect.Tuple;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.shard.ShardId;
import org.havenask.rest.RestStatus;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.RandomObjects;

import java.io.IOException;
import java.util.Locale;

import static org.havenask.HavenaskExceptionTests.assertDeepEquals;
import static org.havenask.common.xcontent.XContentHelper.toXContent;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertToXContentEquivalent;

public class ReplicationResponseTests extends HavenaskTestCase {

    public void testShardInfoToString() {
        final int total = 5;
        final int successful = randomIntBetween(1, total);
        final ShardInfo shardInfo = new ShardInfo(total, successful);
        assertEquals(String.format(Locale.ROOT, "ShardInfo{total=5, successful=%d, failures=[]}", successful), shardInfo.toString());
    }

    public void testShardInfoToXContent() throws IOException {
        {
            ShardInfo shardInfo = new ShardInfo(5, 3);
            String output = Strings.toString(shardInfo);
            assertEquals("{\"total\":5,\"successful\":3,\"failed\":0}", output);
        }
        {
            ShardInfo shardInfo = new ShardInfo(6, 4,
                    new ShardInfo.Failure(new ShardId("index", "_uuid", 3),
                            "_node_id", new IllegalArgumentException("Wrong"), RestStatus.BAD_REQUEST, false),
                    new ShardInfo.Failure(new ShardId("index", "_uuid", 1),
                            "_node_id", new CircuitBreakingException("Wrong", 12, 21, CircuitBreaker.Durability.PERMANENT),
                        RestStatus.NOT_ACCEPTABLE, true));
            String output = Strings.toString(shardInfo);
            assertEquals("{\"total\":6,\"successful\":4,\"failed\":2,\"failures\":[{\"_index\":\"index\",\"_shard\":3," +
                    "\"_node\":\"_node_id\",\"reason\":{\"type\":\"illegal_argument_exception\",\"reason\":\"Wrong\"}," +
                    "\"status\":\"BAD_REQUEST\",\"primary\":false},{\"_index\":\"index\",\"_shard\":1,\"_node\":\"_node_id\"," +
                    "\"reason\":{\"type\":\"circuit_breaking_exception\",\"reason\":\"Wrong\",\"bytes_wanted\":12,\"bytes_limit\":21" +
                    ",\"durability\":\"PERMANENT\"},\"status\":\"NOT_ACCEPTABLE\",\"primary\":true}]}", output);
        }
    }

    public void testShardInfoToAndFromXContent() throws IOException {
        final Tuple<ShardInfo, ShardInfo> tuple = RandomObjects.randomShardInfo(random());
        ShardInfo shardInfo = tuple.v1();
        ShardInfo expectedShardInfo = tuple.v2();

        final XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(shardInfo, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        // Shuffle the XContent fields
        if (randomBoolean()) {
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                originalBytes = BytesReference.bytes(shuffleXContent(parser, randomBoolean()));
            }
        }

        ShardInfo parsedShardInfo;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            // Move to the first start object
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedShardInfo = ShardInfo.fromXContent(parser);
            assertNull(parser.nextToken());
        }
        assertShardInfo(expectedShardInfo, parsedShardInfo);

        BytesReference expectedFinalBytes = toXContent(expectedShardInfo, xContentType, humanReadable);
        BytesReference finalBytes = toXContent(parsedShardInfo, xContentType, humanReadable);
        assertToXContentEquivalent(expectedFinalBytes, finalBytes, xContentType);
    }

    public static void assertShardInfo(ShardInfo expected, ShardInfo actual) {
        if (expected == null) {
            assertNull(actual);
        } else {
            assertEquals(expected.getTotal(), actual.getTotal());
            assertEquals(expected.getSuccessful(), actual.getSuccessful());
            assertEquals(expected.getFailed(), actual.getFailed());

            ReplicationResponse.ShardInfo.Failure[] expectedFailures = expected.getFailures();
            ReplicationResponse.ShardInfo.Failure[] actualFailures = actual.getFailures();
            assertEquals(expectedFailures.length, actualFailures.length);

            for (int i = 0; i < expectedFailures.length; i++) {
                ReplicationResponse.ShardInfo.Failure expectedFailure = expectedFailures[i];
                ReplicationResponse.ShardInfo.Failure actualFailure = actualFailures[i];

                assertEquals(expectedFailure.fullShardId(), actualFailure.fullShardId());
                assertEquals(expectedFailure.status(), actualFailure.status());
                assertEquals(expectedFailure.nodeId(), actualFailure.nodeId());
                assertEquals(expectedFailure.primary(), actualFailure.primary());

                HavenaskException expectedCause = (HavenaskException) expectedFailure.getCause();
                HavenaskException actualCause = (HavenaskException) actualFailure.getCause();
                assertDeepEquals(expectedCause, actualCause);
            }
        }
    }
}
