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

package org.havenask.action;

import org.havenask.common.Nullable;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.rest.RestStatus;
import org.havenask.test.HavenaskTestCase;
import org.havenask.action.ShardOperationFailedException;

import java.io.IOException;

public class ShardOperationFailedExceptionTests extends HavenaskTestCase {

    public void testCauseCannotBeNull() {
        NullPointerException nullPointerException = expectThrows(NullPointerException.class, () -> new Failure(
            randomAlphaOfLengthBetween(3, 10), randomInt(), randomAlphaOfLengthBetween(5, 10), randomFrom(RestStatus.values()), null));
        assertEquals("cause cannot be null", nullPointerException.getMessage());
    }

    public void testStatusCannotBeNull() {
        NullPointerException nullPointerException = expectThrows(NullPointerException.class, () -> new Failure(
            randomAlphaOfLengthBetween(3, 10), randomInt(), randomAlphaOfLengthBetween(5, 10), null, new IllegalArgumentException()));
        assertEquals("status cannot be null", nullPointerException.getMessage());
    }

    public void testReasonCannotBeNull() {
        NullPointerException nullPointerException = expectThrows(NullPointerException.class, () -> new Failure(
            randomAlphaOfLengthBetween(3, 10), randomInt(), null, randomFrom(RestStatus.values()), new IllegalArgumentException()));
        assertEquals("reason cannot be null", nullPointerException.getMessage());
    }

    public void testIndexIsNullable() {
        new Failure(null, randomInt(), randomAlphaOfLengthBetween(5, 10), randomFrom(RestStatus.values()), new IllegalArgumentException());
    }

    private static class Failure extends ShardOperationFailedException {

        Failure(@Nullable String index, int shardId, String reason, RestStatus status, Throwable cause) {
           super(index, shardId, reason, status, cause);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }
    }
}
