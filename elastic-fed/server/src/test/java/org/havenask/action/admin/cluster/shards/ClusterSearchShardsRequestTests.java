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

package org.havenask.action.admin.cluster.shards;

import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.action.support.IndicesOptions;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.VersionUtils;

public class ClusterSearchShardsRequestTests extends HavenaskTestCase {

    public void testSerialization() throws Exception {
        ClusterSearchShardsRequest request = new ClusterSearchShardsRequest();
        if (randomBoolean()) {
            int numIndices = randomIntBetween(1, 5);
            String[] indices = new String[numIndices];
            for (int i = 0; i < numIndices; i++) {
                indices[i] = randomAlphaOfLengthBetween(3, 10);
            }
            request.indices(indices);
        }
        if (randomBoolean()) {
            request.indicesOptions(
                    IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        if (randomBoolean()) {
            request.preference(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            int numRoutings = randomIntBetween(1, 3);
            String[] routings = new String[numRoutings];
            for (int i = 0; i < numRoutings; i++) {
                routings[i] = randomAlphaOfLengthBetween(3, 10);
            }
            request.routing(routings);
        }

        Version version = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                ClusterSearchShardsRequest deserialized = new ClusterSearchShardsRequest(in);
                assertArrayEquals(request.indices(), deserialized.indices());
                // indices options are not equivalent when sent to an older version and re-read due
                // to the addition of hidden indices as expand to hidden indices is always true when
                // read from a prior version
                if (version.onOrAfter(LegacyESVersion.V_7_7_0) || request.indicesOptions().expandWildcardsHidden()) {
                    assertEquals(request.indicesOptions(), deserialized.indicesOptions());
                }
                assertEquals(request.routing(), deserialized.routing());
                assertEquals(request.preference(), deserialized.preference());
            }
        }
    }

    public void testIndicesMustNotBeNull() {
        ClusterSearchShardsRequest request = new ClusterSearchShardsRequest();
        assertNotNull(request.indices());
        expectThrows(NullPointerException.class, () -> request.indices((String[])null));
        expectThrows(NullPointerException.class, () -> request.indices((String)null));
        expectThrows(NullPointerException.class, () -> request.indices(new String[]{"index1", null, "index3"}));
    }
}
