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

package org.havenask.action.ingest;

import org.havenask.common.bytes.BytesArray;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.xcontent.XContentType;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.equalTo;

public class SimulatePipelineRequestTests extends HavenaskTestCase {

    public void testSerialization() throws IOException {
        SimulatePipelineRequest request = new SimulatePipelineRequest(new BytesArray(""), XContentType.JSON);
        // Sometimes we set an id
        if (randomBoolean()) {
            request.setId(randomAlphaOfLengthBetween(1, 10));
        }

        // Sometimes we explicitly set a boolean (with whatever value)
        if (randomBoolean()) {
            request.setVerbose(randomBoolean());
        }

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        SimulatePipelineRequest otherRequest = new SimulatePipelineRequest(streamInput);

        assertThat(otherRequest.getId(), equalTo(request.getId()));
        assertThat(otherRequest.isVerbose(), equalTo(request.isVerbose()));
    }

    public void testSerializationWithXContent() throws IOException {
        SimulatePipelineRequest request =
            new SimulatePipelineRequest(new BytesArray("{}".getBytes(StandardCharsets.UTF_8)), XContentType.JSON);
        assertEquals(XContentType.JSON, request.getXContentType());

        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        StreamInput in = StreamInput.wrap(output.bytes().toBytesRef().bytes);

        SimulatePipelineRequest serialized = new SimulatePipelineRequest(in);
        assertEquals(XContentType.JSON, serialized.getXContentType());
        assertEquals("{}", serialized.getSource().utf8ToString());
    }
}
