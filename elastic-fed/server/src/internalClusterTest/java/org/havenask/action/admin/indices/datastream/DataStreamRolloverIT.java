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

package org.havenask.action.admin.indices.datastream;

import org.havenask.action.admin.indices.rollover.RolloverResponse;
import org.havenask.cluster.metadata.DataStream;
import org.havenask.index.Index;

import java.util.Collections;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamRolloverIT extends DataStreamTestCase {

    public void testDataStreamRollover() throws Exception {
        createDataStreamIndexTemplate("demo-template", Collections.singletonList("logs-*"));
        createDataStream("logs-demo");

        DataStream dataStream;
        GetDataStreamAction.Response.DataStreamInfo dataStreamInfo;
        GetDataStreamAction.Response response;

        // Data stream before a rollover.
        response = getDataStreams("logs-demo");
        dataStreamInfo = response.getDataStreams().get(0);
        assertThat(dataStreamInfo.getIndexTemplate(), equalTo("demo-template"));
        dataStream = dataStreamInfo.getDataStream();
        assertThat(dataStream.getGeneration(), equalTo(1L));
        assertThat(dataStream.getIndices().size(), equalTo(1));
        assertThat(dataStream.getTimeStampField(), equalTo(new DataStream.TimestampField("@timestamp")));
        assertThat(
            dataStream.getIndices().stream().map(Index::getName).collect(Collectors.toList()),
            containsInAnyOrder(".ds-logs-demo-000001")
        );

        // Perform a rollover.
        RolloverResponse rolloverResponse = rolloverDataStream("logs-demo");
        assertThat(rolloverResponse.getOldIndex(), equalTo(".ds-logs-demo-000001"));
        assertThat(rolloverResponse.getNewIndex(), equalTo(".ds-logs-demo-000002"));

        // Data stream after a rollover.
        response = getDataStreams("logs-demo");
        dataStreamInfo = response.getDataStreams().get(0);
        assertThat(dataStreamInfo.getIndexTemplate(), equalTo("demo-template"));
        dataStream = dataStreamInfo.getDataStream();
        assertThat(dataStream.getGeneration(), equalTo(2L));
        assertThat(dataStream.getIndices().size(), equalTo(2));
        assertThat(dataStream.getTimeStampField(), equalTo(new DataStream.TimestampField("@timestamp")));
        assertThat(
            dataStream.getIndices().stream().map(Index::getName).collect(Collectors.toList()),
            containsInAnyOrder(".ds-logs-demo-000001", ".ds-logs-demo-000002")
        );
    }

}
