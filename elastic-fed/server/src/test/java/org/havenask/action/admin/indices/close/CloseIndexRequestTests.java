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

package org.havenask.action.admin.indices.close;

import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.action.support.ActiveShardCount;
import org.havenask.action.support.IndicesOptions;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.tasks.TaskId;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.VersionUtils;

public class CloseIndexRequestTests extends HavenaskTestCase {

    public void testSerialization() throws Exception {
        final CloseIndexRequest request = randomRequest();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);

            final CloseIndexRequest deserializedRequest;
            try (StreamInput in = out.bytes().streamInput()) {
                deserializedRequest = new CloseIndexRequest(in);
            }
            assertEquals(request.timeout(), deserializedRequest.timeout());
            assertEquals(request.masterNodeTimeout(), deserializedRequest.masterNodeTimeout());
            assertEquals(request.indicesOptions(), deserializedRequest.indicesOptions());
            assertEquals(request.getParentTask(), deserializedRequest.getParentTask());
            assertEquals(request.waitForActiveShards(), deserializedRequest.waitForActiveShards());
            assertArrayEquals(request.indices(), deserializedRequest.indices());
        }
    }

    public void testBwcSerialization() throws Exception {
        {
            final CloseIndexRequest request = randomRequest();
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));
                request.writeTo(out);

                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(out.getVersion());
                    assertEquals(request.getParentTask(), TaskId.readFromStream(in));
                    assertEquals(request.masterNodeTimeout(), in.readTimeValue());
                    assertEquals(request.timeout(), in.readTimeValue());
                    assertArrayEquals(request.indices(), in.readStringArray());
                    // indices options are not equivalent when sent to an older version and re-read due
                    // to the addition of hidden indices as expand to hidden indices is always true when
                    // read from a prior version
                    final IndicesOptions indicesOptions = IndicesOptions.readIndicesOptions(in);
                    if (out.getVersion().onOrAfter(LegacyESVersion.V_7_7_0) || request.indicesOptions().expandWildcardsHidden()) {
                        assertEquals(request.indicesOptions(), indicesOptions);
                    }
                    if (in.getVersion().onOrAfter(LegacyESVersion.V_7_2_0)) {
                        assertEquals(request.waitForActiveShards(), ActiveShardCount.readFrom(in));
                    } else {
                        assertEquals(0, in.available());
                    }
                }
            }
        }
        {
            final CloseIndexRequest sample = randomRequest();
            final Version version = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                sample.getParentTask().writeTo(out);
                out.writeTimeValue(sample.masterNodeTimeout());
                out.writeTimeValue(sample.timeout());
                out.writeStringArray(sample.indices());
                sample.indicesOptions().writeIndicesOptions(out);
                if (out.getVersion().onOrAfter(LegacyESVersion.V_7_2_0)) {
                    sample.waitForActiveShards().writeTo(out);
                }

                final CloseIndexRequest deserializedRequest;
                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(version);
                    deserializedRequest = new CloseIndexRequest(in);
                }
                assertEquals(sample.getParentTask(), deserializedRequest.getParentTask());
                assertEquals(sample.masterNodeTimeout(), deserializedRequest.masterNodeTimeout());
                assertEquals(sample.timeout(), deserializedRequest.timeout());
                assertArrayEquals(sample.indices(), deserializedRequest.indices());
                // indices options are not equivalent when sent to an older version and re-read due
                // to the addition of hidden indices as expand to hidden indices is always true when
                // read from a prior version
                if (out.getVersion().onOrAfter(LegacyESVersion.V_7_7_0) || sample.indicesOptions().expandWildcardsHidden()) {
                    assertEquals(sample.indicesOptions(), deserializedRequest.indicesOptions());
                }
                if (out.getVersion().onOrAfter(LegacyESVersion.V_7_2_0)) {
                    assertEquals(sample.waitForActiveShards(), deserializedRequest.waitForActiveShards());
                } else {
                    assertEquals(ActiveShardCount.NONE, deserializedRequest.waitForActiveShards());
                }
            }
        }
    }

    private CloseIndexRequest randomRequest() {
        CloseIndexRequest request = new CloseIndexRequest();
        request.indices(generateRandomStringArray(10, 5, false, false));
        if (randomBoolean()) {
            request.indicesOptions(
                IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        if (randomBoolean()) {
            request.timeout(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            request.masterNodeTimeout(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            request.setParentTask(randomAlphaOfLength(5), randomNonNegativeLong());
        }
        if (randomBoolean()) {
            request.waitForActiveShards(randomFrom(ActiveShardCount.DEFAULT, ActiveShardCount.NONE, ActiveShardCount.ONE,
                ActiveShardCount.ALL));
        }
        return request;
    }
}
