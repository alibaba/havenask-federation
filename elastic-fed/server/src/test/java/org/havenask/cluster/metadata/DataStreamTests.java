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

import org.havenask.common.UUIDs;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.index.Index;
import org.havenask.test.AbstractSerializingTestCase;
import org.havenask.cluster.metadata.DataStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.havenask.cluster.DataStreamTestHelper.createTimestampField;
import static org.havenask.cluster.metadata.DataStream.getDefaultBackingIndexName;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamTests extends AbstractSerializingTestCase<DataStream> {

    public static List<Index> randomIndexInstances() {
        int numIndices = randomIntBetween(0, 128);
        List<Index> indices = new ArrayList<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            indices.add(new Index(randomAlphaOfLength(10).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random())));
        }
        return indices;
    }

    public static DataStream randomInstance() {
        List<Index> indices = randomIndexInstances();
        long generation = indices.size() + randomLongBetween(1, 128);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        indices.add(new Index(getDefaultBackingIndexName(dataStreamName, generation), UUIDs.randomBase64UUID(random())));
        return new DataStream(dataStreamName, createTimestampField("@timestamp"), indices, generation);
    }

    @Override
    protected DataStream doParseInstance(XContentParser parser) throws IOException {
        return DataStream.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DataStream> instanceReader() {
        return DataStream::new;
    }

    @Override
    protected DataStream createTestInstance() {
        return randomInstance();
    }

    public void testRollover() {
        DataStream ds = randomInstance();
        Index newWriteIndex = new Index(getDefaultBackingIndexName(ds.getName(), ds.getGeneration() + 1), UUIDs.randomBase64UUID(random()));
        DataStream rolledDs = ds.rollover(newWriteIndex);

        assertThat(rolledDs.getName(), equalTo(ds.getName()));
        assertThat(rolledDs.getTimeStampField(), equalTo(ds.getTimeStampField()));
        assertThat(rolledDs.getGeneration(), equalTo(ds.getGeneration() + 1));
        assertThat(rolledDs.getIndices().size(), equalTo(ds.getIndices().size() + 1));
        assertTrue(rolledDs.getIndices().containsAll(ds.getIndices()));
        assertTrue(rolledDs.getIndices().contains(newWriteIndex));
    }

    public void testRemoveBackingIndex() {
        int numBackingIndices = randomIntBetween(2, 32);
        int indexToRemove = randomIntBetween(1, numBackingIndices - 1);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int k = 1; k <= numBackingIndices; k++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, k), UUIDs.randomBase64UUID(random())));
        }
        DataStream original = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);
        DataStream updated = original.removeBackingIndex(indices.get(indexToRemove - 1));
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration()));
        assertThat(updated.getTimeStampField(), equalTo(original.getTimeStampField()));
        assertThat(updated.getIndices().size(), equalTo(numBackingIndices - 1));
        for (int k = 0; k < (numBackingIndices - 1); k++) {
            assertThat(updated.getIndices().get(k), equalTo(original.getIndices().get(k < (indexToRemove - 1) ? k : k + 1)));
        }
    }

    public void testDefaultBackingIndexName() {
        // this test does little more than flag that changing the default naming convention for backing indices
        // will also require changing a lot of hard-coded values in REST tests and docs
        long backingIndexNum = randomLongBetween(1, 1000001);
        String dataStreamName = randomAlphaOfLength(6);
        String defaultBackingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, backingIndexNum);
        String expectedBackingIndexName = String.format(Locale.ROOT, ".ds-%s-%06d", dataStreamName, backingIndexNum);
        assertThat(defaultBackingIndexName, equalTo(expectedBackingIndexName));
    }

    public void testReplaceBackingIndex() {
        int numBackingIndices = randomIntBetween(2, 32);
        int indexToReplace = randomIntBetween(1, numBackingIndices - 1) - 1;
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int i = 1; i <= numBackingIndices; i++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, i), UUIDs.randomBase64UUID(random())));
        }
        DataStream original = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Index newBackingIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        DataStream updated = original.replaceBackingIndex(indices.get(indexToReplace), newBackingIndex);
        assertThat(updated.getName(), equalTo(original.getName()));
        assertThat(updated.getGeneration(), equalTo(original.getGeneration()));
        assertThat(updated.getTimeStampField(), equalTo(original.getTimeStampField()));
        assertThat(updated.getIndices().size(), equalTo(numBackingIndices));
        assertThat(updated.getIndices().get(indexToReplace), equalTo(newBackingIndex));

        for (int i = 0; i < numBackingIndices; i++) {
            if (i != indexToReplace) {
                assertThat(updated.getIndices().get(i), equalTo(original.getIndices().get(i)));
            }
        }
    }

    public void testReplaceBackingIndexThrowsExceptionIfIndexNotPartOfDataStream() {
        int numBackingIndices = randomIntBetween(2, 32);
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int i = 1; i <= numBackingIndices; i++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, i), UUIDs.randomBase64UUID(random())));
        }
        DataStream original = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Index standaloneIndex = new Index("index-foo", UUIDs.randomBase64UUID(random()));
        Index newBackingIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        expectThrows(IllegalArgumentException.class, () -> original.replaceBackingIndex(standaloneIndex, newBackingIndex));
    }

    public void testReplaceBackingIndexThrowsExceptionIfReplacingWriteIndex() {
        int numBackingIndices = randomIntBetween(2, 32);
        int writeIndexPosition = numBackingIndices - 1;
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        List<Index> indices = new ArrayList<>(numBackingIndices);
        for (int i = 1; i <= numBackingIndices; i++) {
            indices.add(new Index(DataStream.getDefaultBackingIndexName(dataStreamName, i), UUIDs.randomBase64UUID(random())));
        }
        DataStream original = new DataStream(dataStreamName, createTimestampField("@timestamp"), indices);

        Index newBackingIndex = new Index("replacement-index", UUIDs.randomBase64UUID(random()));
        expectThrows(IllegalArgumentException.class, () -> original.replaceBackingIndex(indices.get(writeIndexPosition), newBackingIndex));
    }
}
