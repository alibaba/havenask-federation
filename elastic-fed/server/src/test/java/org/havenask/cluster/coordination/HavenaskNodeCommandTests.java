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

package org.havenask.cluster.coordination;

import org.havenask.cluster.ClusterModule;
import org.havenask.cluster.metadata.DataStream;
import org.havenask.cluster.metadata.IndexGraveyard;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.common.UUIDs;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.collect.List;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.index.Index;
import org.havenask.indices.IndicesModule;
import org.havenask.test.HavenaskTestCase;
import org.havenask.cluster.coordination.HavenaskNodeCommand;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.havenask.cluster.DataStreamTestHelper.createFirstBackingIndex;
import static org.havenask.cluster.DataStreamTestHelper.createTimestampField;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class HavenaskNodeCommandTests extends HavenaskTestCase {

    public void testLoadStateWithoutMissingCustoms() throws IOException {
        runLoadStateTest(false, false);
    }

    public void testLoadStateWithoutMissingCustomsButPreserved() throws IOException {
        runLoadStateTest(false, true);
    }

    public void testLoadStateWithMissingCustomsButPreserved() throws IOException {
        runLoadStateTest(true, true);
    }

    public void testLoadStateWithMissingCustomsAndNotPreserved() throws IOException {
        runLoadStateTest(true, false);
    }

    private void runLoadStateTest(boolean hasMissingCustoms, boolean preserveUnknownCustoms) throws IOException {
        final Metadata latestMetadata = randomMeta();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        Metadata.FORMAT.toXContent(builder, latestMetadata);
        builder.endObject();

        Metadata loadedMetadata;
        try (XContentParser parser = createParser(hasMissingCustoms ? HavenaskNodeCommand.namedXContentRegistry : xContentRegistry(),
            JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            loadedMetadata = Metadata.fromXContent(parser);
        }
        assertThat(loadedMetadata.clusterUUID(), not(equalTo("_na_")));
        assertThat(loadedMetadata.clusterUUID(), equalTo(latestMetadata.clusterUUID()));
        assertThat(loadedMetadata.dataStreams(), equalTo(latestMetadata.dataStreams()));

        // make sure the index tombstones are the same too
        if (hasMissingCustoms) {
            assertNotNull(loadedMetadata.custom(IndexGraveyard.TYPE));
            assertThat(loadedMetadata.custom(IndexGraveyard.TYPE), instanceOf(HavenaskNodeCommand.UnknownMetadataCustom.class));

            if (preserveUnknownCustoms) {
                // check that we reserialize unknown metadata correctly again
                final Path tempdir = createTempDir();
                Metadata.FORMAT.write(loadedMetadata, tempdir);
                final Metadata reloadedMetadata = Metadata.FORMAT.loadLatestState(logger, xContentRegistry(), tempdir);
                assertThat(reloadedMetadata.indexGraveyard(), equalTo(latestMetadata.indexGraveyard()));
            }
        }  else {
            assertThat(loadedMetadata.indexGraveyard(), equalTo(latestMetadata.indexGraveyard()));
        }
    }

    private Metadata randomMeta() {
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.generateClusterUuidIfNeeded();
        int numDelIndices = randomIntBetween(0, 5);
        final IndexGraveyard.Builder graveyard = IndexGraveyard.builder();
        for (int i = 0; i < numDelIndices; i++) {
            graveyard.addTombstone(new Index(randomAlphaOfLength(10) + "del-idx-" + i, UUIDs.randomBase64UUID()));
        }
        if (randomBoolean()) {
            int numDataStreams = randomIntBetween(0, 5);
            for (int i = 0; i < numDataStreams; i++) {
                String dataStreamName = "name" + 1;
                IndexMetadata backingIndex = createFirstBackingIndex(dataStreamName).build();
                mdBuilder.put(new DataStream(dataStreamName, createTimestampField("@timestamp"),
                    List.of(backingIndex.getIndex())));
            }
        }
        mdBuilder.indexGraveyard(graveyard.build());
        return mdBuilder.build();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            Stream.of(ClusterModule.getNamedXWriteables().stream(), IndicesModule.getNamedXContents().stream())
            .flatMap(Function.identity())
            .collect(Collectors.toList())
        );
    }
}
