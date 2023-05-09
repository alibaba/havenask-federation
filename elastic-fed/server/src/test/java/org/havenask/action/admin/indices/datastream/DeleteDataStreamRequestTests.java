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

package org.havenask.action.admin.indices.datastream;

import org.havenask.Version;
import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.admin.indices.datastream.DeleteDataStreamAction;
import org.havenask.action.admin.indices.datastream.DeleteDataStreamAction.Request;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.SnapshotsInProgress;
import org.havenask.cluster.metadata.DataStream;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.metadata.MetadataDeleteIndexService;
import org.havenask.common.Strings;
import org.havenask.common.collect.ImmutableOpenMap;
import org.havenask.common.collect.Tuple;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.settings.Settings;
import org.havenask.index.Index;
import org.havenask.snapshots.Snapshot;
import org.havenask.snapshots.SnapshotId;
import org.havenask.snapshots.SnapshotInProgressException;
import org.havenask.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.havenask.cluster.DataStreamTestHelper.createTimestampField;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleteDataStreamRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(randomArray(1, 3, String[]::new, () -> randomAlphaOfLength(6)));
    }

    public void testValidateRequest() {
        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(new String[]{"my-data-stream"});
        ActionRequestValidationException e = req.validate();
        assertNull(e);
    }

    public void testValidateRequestWithoutName() {
        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(new String[0]);
        ActionRequestValidationException e = req.validate();
        assertNotNull(e);
        assertThat(e.validationErrors().size(), equalTo(1));
        assertThat(e.validationErrors().get(0), containsString("no data stream(s) specified"));
    }

    public void testDeleteDataStream() {
        final String dataStreamName = "my-data-stream";
        final List<String> otherIndices = randomSubsetOf(org.havenask.common.collect.List.of("foo", "bar", "baz"));
        ClusterState cs = getClusterStateWithDataStreams(
            org.havenask.common.collect.List.of(new Tuple<>(dataStreamName, 2)), otherIndices);
        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(new String[]{dataStreamName});
        ClusterState newState = DeleteDataStreamAction.TransportAction.removeDataStream(getMetadataDeleteIndexService(), cs, req);
        assertThat(newState.metadata().dataStreams().size(), equalTo(0));
        assertThat(newState.metadata().indices().size(), equalTo(otherIndices.size()));
        for (String indexName : otherIndices) {
            assertThat(newState.metadata().indices().get(indexName).getIndex().getName(), equalTo(indexName));
        }
    }

    public void testDeleteMultipleDataStreams() {
        String[] dataStreamNames = {"foo", "bar", "baz", "eggplant"};
        ClusterState cs = getClusterStateWithDataStreams(org.havenask.common.collect.List.of(
            new Tuple<>(dataStreamNames[0], randomIntBetween(1, 3)),
            new Tuple<>(dataStreamNames[1], randomIntBetween(1, 3)),
            new Tuple<>(dataStreamNames[2], randomIntBetween(1, 3)),
            new Tuple<>(dataStreamNames[3], randomIntBetween(1, 3))
        ), org.havenask.common.collect.List.of());

        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(new String[]{"ba*", "eggplant"});
        ClusterState newState = DeleteDataStreamAction.TransportAction.removeDataStream(getMetadataDeleteIndexService(), cs, req);
        assertThat(newState.metadata().dataStreams().size(), equalTo(1));
        DataStream remainingDataStream = newState.metadata().dataStreams().get(dataStreamNames[0]);
        assertNotNull(remainingDataStream);
        assertThat(newState.metadata().indices().size(), equalTo(remainingDataStream.getIndices().size()));
        for (Index i : remainingDataStream.getIndices()) {
            assertThat(newState.metadata().indices().get(i.getName()).getIndex(), equalTo(i));
        }
    }

    public void testDeleteSnapshottingDataStream() {
        final String dataStreamName = "my-data-stream1";
        final String dataStreamName2 = "my-data-stream2";
        final List<String> otherIndices = randomSubsetOf(Arrays.asList("foo", "bar", "baz"));

        ClusterState cs = getClusterStateWithDataStreams(Arrays.asList(new Tuple<>(dataStreamName, 2), new Tuple<>(dataStreamName2, 2)),
            otherIndices);
        SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.of(Arrays.asList(
            createEntry(dataStreamName, "repo1", false),
            createEntry(dataStreamName2, "repo2", true)));
        ClusterState snapshotCs = ClusterState.builder(cs).putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress).build();

        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(new String[]{dataStreamName});
        SnapshotInProgressException e = expectThrows(SnapshotInProgressException.class,
            () -> DeleteDataStreamAction.TransportAction.removeDataStream(getMetadataDeleteIndexService(), snapshotCs, req));

        assertThat(e.getMessage(), equalTo("Cannot delete data streams that are being snapshotted: [my-data-stream1]. Try again after " +
            "snapshot finishes or cancel the currently running snapshot."));
    }

    private SnapshotsInProgress.Entry createEntry(String dataStreamName, String repo, boolean partial) {
        return new SnapshotsInProgress.Entry(new Snapshot(repo, new SnapshotId("", "")), false, partial,
            SnapshotsInProgress.State.SUCCESS, Collections.emptyList(), Collections.singletonList(dataStreamName), 0, 1,
            ImmutableOpenMap.of(), null, null, null);
    }

    public void testDeleteNonexistentDataStream() {
        final String dataStreamName = "my-data-stream";
        String[] dataStreamNames = {"foo", "bar", "baz", "eggplant"};
        ClusterState cs = getClusterStateWithDataStreams(org.havenask.common.collect.List.of(
            new Tuple<>(dataStreamNames[0], randomIntBetween(1, 3)),
            new Tuple<>(dataStreamNames[1], randomIntBetween(1, 3)),
            new Tuple<>(dataStreamNames[2], randomIntBetween(1, 3)),
            new Tuple<>(dataStreamNames[3], randomIntBetween(1, 3))
        ), org.havenask.common.collect.List.of());
        DeleteDataStreamAction.Request req = new DeleteDataStreamAction.Request(new String[]{dataStreamName});
        ClusterState newState = DeleteDataStreamAction.TransportAction.removeDataStream(getMetadataDeleteIndexService(), cs, req);
        assertThat(newState.metadata().dataStreams().size(), equalTo(cs.metadata().dataStreams().size()));
        assertThat(newState.metadata().dataStreams().keySet(),
            containsInAnyOrder(cs.metadata().dataStreams().keySet().toArray(Strings.EMPTY_ARRAY)));
    }

    @SuppressWarnings("unchecked")
    private static MetadataDeleteIndexService getMetadataDeleteIndexService() {
        MetadataDeleteIndexService s = mock(MetadataDeleteIndexService.class);
        when(s.deleteIndices(any(ClusterState.class), any(Set.class)))
            .thenAnswer(mockInvocation -> {
                ClusterState currentState = (ClusterState) mockInvocation.getArguments()[0];
                Set<Index> indices = (Set<Index>) mockInvocation.getArguments()[1];

                final Metadata.Builder b = Metadata.builder(currentState.metadata());
                for (Index index : indices) {
                    b.remove(index.getName());
                }

                return ClusterState.builder(currentState).metadata(b.build()).build();
            });

        return s;
    }

    /**
     * Constructs {@code ClusterState} with the specified data streams and indices.
     *
     * @param dataStreams The names of the data streams to create with their respective number of backing indices
     * @param indexNames  The names of indices to create that do not back any data streams
     */
    public static ClusterState getClusterStateWithDataStreams(List<Tuple<String, Integer>> dataStreams, List<String> indexNames) {
        Metadata.Builder builder = Metadata.builder();

        List<IndexMetadata> allIndices = new ArrayList<>();
        for (Tuple<String, Integer> dsTuple : dataStreams) {
            List<IndexMetadata> backingIndices = new ArrayList<>();
            for (int backingIndexNumber = 1; backingIndexNumber <= dsTuple.v2(); backingIndexNumber++) {
                backingIndices.add(createIndexMetadata(DataStream.getDefaultBackingIndexName(dsTuple.v1(), backingIndexNumber), true));
            }
            allIndices.addAll(backingIndices);

            DataStream ds = new DataStream(dsTuple.v1(), createTimestampField("@timestamp"),
                backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList()), dsTuple.v2());
            builder.put(ds);
        }

        for (String indexName : indexNames) {
            allIndices.add(createIndexMetadata(indexName, false));
        }

        for (IndexMetadata index : allIndices) {
            builder.put(index, false);
        }

        return ClusterState.builder(new ClusterName("_name")).metadata(builder).build();
    }

    private static IndexMetadata createIndexMetadata(String name, boolean hidden) {
        Settings.Builder b = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.hidden", hidden);

        return IndexMetadata.builder(name)
            .settings(b)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
    }
}
