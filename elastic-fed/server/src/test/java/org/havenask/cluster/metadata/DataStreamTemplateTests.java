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

package org.havenask.cluster.metadata;

import org.havenask.Version;
import org.havenask.cluster.metadata.ComposableIndexTemplate.DataStreamTemplate;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.test.AbstractSerializingTestCase;
import org.havenask.test.VersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DataStreamTemplateTests extends AbstractSerializingTestCase<DataStreamTemplate> {

    @Override
    protected DataStreamTemplate doParseInstance(XContentParser parser) throws IOException {
        return DataStreamTemplate.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DataStreamTemplate> instanceReader() {
        return DataStreamTemplate::new;
    }

    @Override
    protected DataStreamTemplate createTestInstance() {
        return new DataStreamTemplate(new DataStream.TimestampField("timestamp_" + randomAlphaOfLength(5)));
    }

    public void testBackwardCompatibleSerialization() throws Exception {
        Version version = VersionUtils.getPreviousVersion(Version.V_1_0_0);
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(version);

        DataStreamTemplate outTemplate = new DataStreamTemplate();
        outTemplate.writeTo(out);
        assertThat(out.size(), equalTo(0));

        StreamInput in = out.bytes().streamInput();
        in.setVersion(version);
        DataStreamTemplate inTemplate = new DataStreamTemplate(in);

        assertThat(inTemplate, equalTo(outTemplate));
    }

}
