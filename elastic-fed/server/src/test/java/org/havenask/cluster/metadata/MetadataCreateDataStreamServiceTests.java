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

package org.havenask.cluster.metadata;

import org.havenask.ResourceAlreadyExistsException;
import org.havenask.Version;
import org.havenask.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.ComposableIndexTemplate;
import org.havenask.cluster.metadata.DataStream;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.metadata.MetadataCreateDataStreamService;
import org.havenask.cluster.metadata.MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.test.HavenaskTestCase;
import org.havenask.cluster.metadata.MetadataCreateIndexService;

import java.util.Collections;

import static org.havenask.cluster.DataStreamTestHelper.createFirstBackingIndex;
import static org.havenask.cluster.DataStreamTestHelper.createTimestampField;
import static org.havenask.cluster.DataStreamTestHelper.generateMapping;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataCreateDataStreamServiceTests extends HavenaskTestCase {

    public void testCreateDataStream() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        ComposableIndexTemplate template = new ComposableIndexTemplate(Collections.singletonList(dataStreamName + "*"),
            null, null, null, null, null, new ComposableIndexTemplate.DataStreamTemplate());
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req =
            new CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);
        ClusterState newState = MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req);
        assertThat(newState.metadata().dataStreams().size(), equalTo(1));
        assertThat(newState.metadata().dataStreams().get(dataStreamName).getName(), equalTo(dataStreamName));
        assertThat(newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)), notNullValue());
        assertThat(newState.metadata().index(DataStream.getDefaultBackingIndexName(dataStreamName, 1)).getSettings().get("index.hidden"),
            equalTo("true"));
    }

    public void testCreateDuplicateDataStream() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        IndexMetadata idx = createFirstBackingIndex(dataStreamName).build();
        DataStream existingDataStream =
            new DataStream(dataStreamName, createTimestampField("@timestamp"), Collections.singletonList(idx.getIndex()));
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().dataStreams(Collections.singletonMap(dataStreamName, existingDataStream)).build()).build();
        CreateDataStreamClusterStateUpdateRequest req =
            new CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);

        ResourceAlreadyExistsException e = expectThrows(ResourceAlreadyExistsException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req));
        assertThat(e.getMessage(), containsString("data_stream [" + dataStreamName + "] already exists"));
    }

    public void testCreateDataStreamWithInvalidName() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "_My-da#ta- ,stream-";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        CreateDataStreamClusterStateUpdateRequest req =
            new CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req));
        assertThat(e.getMessage(), containsString("must not contain the following characters"));
    }

    public void testCreateDataStreamWithUppercaseCharacters() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "MAY_NOT_USE_UPPERCASE";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        CreateDataStreamClusterStateUpdateRequest req =
            new CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req));
        assertThat(e.getMessage(), containsString("data_stream [" + dataStreamName + "] must be lowercase"));
    }

    public void testCreateDataStreamStartingWithPeriod() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = ".may_not_start_with_period";
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        CreateDataStreamClusterStateUpdateRequest req =
            new CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req));
        assertThat(e.getMessage(), containsString("data_stream [" + dataStreamName + "] must not start with '.'"));
    }

    public void testCreateDataStreamNoTemplate() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .build();
        CreateDataStreamClusterStateUpdateRequest req =
            new CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req));
        assertThat(e.getMessage(), equalTo("no matching index template found for data stream [my-data-stream]"));
    }

    public void testCreateDataStreamNoValidTemplate() throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        final String dataStreamName = "my-data-stream";
        ComposableIndexTemplate template =
            new ComposableIndexTemplate(Collections.singletonList(dataStreamName + "*"), null, null, null, null, null, null);
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put("template", template).build())
            .build();
        CreateDataStreamClusterStateUpdateRequest req =
            new CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);
        Exception e = expectThrows(IllegalArgumentException.class,
            () -> MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req));
        assertThat(e.getMessage(),
            equalTo("matching index template [template] for data stream [my-data-stream] has no data stream template"));
    }

    public static ClusterState createDataStream(final String dataStreamName) throws Exception {
        final MetadataCreateIndexService metadataCreateIndexService = getMetadataCreateIndexService();
        ComposableIndexTemplate template = new ComposableIndexTemplate(Collections.singletonList(dataStreamName + "*"),
            null, null, null, null, null, new ComposableIndexTemplate.DataStreamTemplate());
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put("template", template).build())
            .build();
        MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest req =
            new MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest(dataStreamName, TimeValue.ZERO, TimeValue.ZERO);
        return MetadataCreateDataStreamService.createDataStream(metadataCreateIndexService, cs, req);
    }

    private static MetadataCreateIndexService getMetadataCreateIndexService() throws Exception {
        MetadataCreateIndexService s = mock(MetadataCreateIndexService.class);
        when(s.applyCreateIndexRequest(any(ClusterState.class), any(CreateIndexClusterStateUpdateRequest.class), anyBoolean()))
            .thenAnswer(mockInvocation -> {
                ClusterState currentState = (ClusterState) mockInvocation.getArguments()[0];
                CreateIndexClusterStateUpdateRequest request = (CreateIndexClusterStateUpdateRequest) mockInvocation.getArguments()[1];

                Metadata.Builder b = Metadata.builder(currentState.metadata())
                    .put(IndexMetadata.builder(request.index())
                        .settings(Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(request.settings())
                            .build())
                        .putMapping("_doc", generateMapping("@timestamp"))
                        .numberOfShards(1)
                        .numberOfReplicas(1)
                        .build(), false);
                return ClusterState.builder(currentState).metadata(b.build()).build();
            });

        return s;
    }

}
