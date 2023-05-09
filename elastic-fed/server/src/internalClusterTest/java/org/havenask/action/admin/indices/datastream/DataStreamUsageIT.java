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

import org.havenask.action.DocWriteRequest;
import org.havenask.action.admin.indices.datastream.DataStreamsStatsAction.DataStreamStats;
import org.havenask.action.index.IndexRequest;
import org.havenask.action.index.IndexResponse;
import org.havenask.cluster.metadata.DataStream;
import org.havenask.common.collect.List;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.rest.RestStatus;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class DataStreamUsageIT extends DataStreamTestCase {

    public void testDataStreamCrudAPIs() throws Exception {
        // Data stream creation without a matching index template should fail.
        ExecutionException exception = expectThrows(
            ExecutionException.class,
            () -> createDataStream("test-data-stream")
        );
        assertThat(exception.getMessage(), containsString("no matching index template found for data stream"));

        // Create an index template for data streams.
        createDataStreamIndexTemplate("data-stream-template", List.of("logs-*", "metrics-*", "events"));

        // Create multiple data streams matching the above index pattern.
        createDataStream("logs-dev");
        createDataStream("logs-prod");
        createDataStream("metrics-prod");
        createDataStream("events");
        ensureGreen();

        // Get all data streams.
        assertThat(getDataStreamsNames(), containsInAnyOrder("logs-dev", "logs-prod", "metrics-prod", "events"));
        assertThat(getDataStreamsNames("*"), containsInAnyOrder("logs-dev", "logs-prod", "metrics-prod", "events"));

        // Get data streams with and without wildcards.
        assertThat(getDataStreamsNames("logs-*", "events"), containsInAnyOrder("logs-dev", "logs-prod", "events"));

        // Get data stream by name.
        GetDataStreamAction.Response response = getDataStreams("logs-prod");
        assertThat(response.getDataStreams().size(), equalTo(1));
        DataStream dataStream = response.getDataStreams().get(0).getDataStream();
        assertThat(dataStream.getName(), equalTo("logs-prod"));
        assertThat(dataStream.getIndices().size(), equalTo(1));
        assertThat(dataStream.getGeneration(), equalTo(1L));
        assertThat(dataStream.getTimeStampField(), equalTo(new DataStream.TimestampField("@timestamp")));

        // Get data stream stats.
        DataStreamsStatsAction.Response stats = getDataStreamsStats("*");
        assertThat(stats.getTotalShards(), equalTo(16));  // 4 data streams, 1 backing index per stream, 2 shards, 1 replica
        assertThat(stats.getSuccessfulShards(), equalTo(16));
        assertThat(stats.getBackingIndices(), equalTo(4));
        assertThat(stats.getTotalStoreSize().getBytes(), greaterThan(0L));
        assertThat(stats.getDataStreams().length, equalTo(4));
        assertThat(
            Arrays.stream(stats.getDataStreams()).map(DataStreamStats::getDataStream).collect(Collectors.toList()),
            containsInAnyOrder("logs-dev", "logs-prod", "metrics-prod", "events")
        );

        // Delete multiple data streams at once; with and without wildcards.
        deleteDataStreams("logs-*", "events");
        deleteDataStreams("metrics-prod");
        assertThat(getDataStreamsNames("*").size(), equalTo(0));
    }

    public void testDataStreamIndexDocumentsDefaultTimestampField() throws Exception {
        assertDataStreamIndexDocuments("@timestamp");
    }

    public void testDataStreamIndexDocumentsCustomTimestampField() throws Exception {
        assertDataStreamIndexDocuments("timestamp_" + randomAlphaOfLength(5));
    }

    public void assertDataStreamIndexDocuments(String timestampFieldName) throws Exception {
        createDataStreamIndexTemplate("demo-template", List.of("logs-*"), timestampFieldName);
        createDataStream("logs-demo");

        Exception exception;

        // Only op_type=create requests should be allowed.
        exception = expectThrows(Exception.class, () -> index(
            new IndexRequest("logs-demo")
                .id("doc-1")
                .source("{}", XContentType.JSON)
        ));
        assertThat(
            exception.getMessage(),
            containsString("only write ops with an op_type of create are allowed in data streams")
        );

        // Documents must contain a valid timestamp field.
        exception = expectThrows(Exception.class, () -> index(
            new IndexRequest("logs-demo")
                .id("doc-1")
                .source("{}", XContentType.JSON)
                .opType(DocWriteRequest.OpType.CREATE)
        ));
        assertThat(
            exception.getMessage(),
            containsString("documents must contain a single-valued timestamp field '" + timestampFieldName + "' of date type")
        );

        // The timestamp field cannot have multiple values.
        exception = expectThrows(Exception.class, () -> index(
            new IndexRequest("logs-demo")
                .id("doc-1")
                .opType(DocWriteRequest.OpType.CREATE)
                .source(
                    XContentFactory
                        .jsonBuilder()
                        .startObject()
                        .array(timestampFieldName, "2020-12-06T11:04:05.000Z", "2020-12-07T11:04:05.000Z")
                        .field("message", "User registration successful")
                        .endObject()
                )
        ));
        assertThat(
            exception.getMessage(),
            containsString("documents must contain a single-valued timestamp field '" + timestampFieldName + "' of date type")
        );

        // Successful case.
        IndexResponse response = index(
            new IndexRequest("logs-demo")
                .id("doc-1")
                .opType(DocWriteRequest.OpType.CREATE)
                .source(
                    XContentFactory
                        .jsonBuilder()
                        .startObject()
                        .field(timestampFieldName, "2020-12-06T11:04:05.000Z")
                        .field("message", "User registration successful")
                        .endObject()
                )
        );
        assertThat(response.status(), equalTo(RestStatus.CREATED));
        assertThat(response.getId(), equalTo("doc-1"));
        assertThat(response.getIndex(), equalTo(".ds-logs-demo-000001"));

        // Perform a rollover and ingest more documents.
        rolloverDataStream("logs-demo");
        response = index(
            new IndexRequest("logs-demo")
                .id("doc-2")
                .opType(DocWriteRequest.OpType.CREATE)
                .source(
                    XContentFactory
                        .jsonBuilder()
                        .startObject()
                        .field(timestampFieldName, "2020-12-06T11:04:05.000Z")
                        .field("message", "User registration successful")
                        .endObject()
                )
        );
        assertThat(response.status(), equalTo(RestStatus.CREATED));
        assertThat(response.getId(), equalTo("doc-2"));
        assertThat(response.getIndex(), equalTo(".ds-logs-demo-000002"));
    }

    private IndexResponse index(IndexRequest request) throws Exception {
        return client().index(request).get();
    }

}
