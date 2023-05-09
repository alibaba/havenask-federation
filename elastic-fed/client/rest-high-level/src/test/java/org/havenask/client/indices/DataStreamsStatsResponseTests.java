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

package org.havenask.client.indices;

import org.havenask.HavenaskException;
import org.havenask.action.admin.indices.datastream.DataStreamsStatsAction;
import org.havenask.action.support.DefaultShardOperationFailedException;
import org.havenask.client.AbstractResponseTestCase;
import org.havenask.common.unit.ByteSizeValue;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class DataStreamsStatsResponseTests extends AbstractResponseTestCase<DataStreamsStatsAction.Response, DataStreamsStatsResponse> {

    private static long randomRecentTimestamp() {
        long base = System.currentTimeMillis();
        return randomLongBetween(base - TimeUnit.HOURS.toMillis(1), base);
    }

    @Override
    protected DataStreamsStatsAction.Response createServerTestInstance(XContentType xContentType) {
        int dataStreamCount = randomInt(10);
        int backingIndicesTotal = 0;
        long totalStoreSize = 0L;
        ArrayList<DataStreamsStatsAction.DataStreamStats> dataStreamStats = new ArrayList<>();
        for (int i = 0; i < dataStreamCount; i++) {
            String dataStreamName = randomAlphaOfLength(8).toLowerCase(Locale.getDefault());
            int backingIndices = randomInt(5);
            backingIndicesTotal += backingIndices;
            long storeSize = randomLongBetween(250, 1000000000);
            totalStoreSize += storeSize;
            long maximumTimestamp = randomRecentTimestamp();
            dataStreamStats.add(new DataStreamsStatsAction.DataStreamStats(dataStreamName, backingIndices,
                new ByteSizeValue(storeSize), maximumTimestamp));
        }
        int totalShards = randomIntBetween(backingIndicesTotal, backingIndicesTotal * 3);
        int successfulShards = randomInt(totalShards);
        int failedShards = totalShards - successfulShards;
        List<DefaultShardOperationFailedException> exceptions = new ArrayList<>();
        for (int i = 0; i < failedShards; i++) {
            exceptions.add(new DefaultShardOperationFailedException(randomAlphaOfLength(8).toLowerCase(Locale.getDefault()),
                randomInt(totalShards), new HavenaskException("boom")));
        }
        return new DataStreamsStatsAction.Response(totalShards, successfulShards, failedShards, exceptions,
            dataStreamCount, backingIndicesTotal, new ByteSizeValue(totalStoreSize),
            dataStreamStats.toArray(new DataStreamsStatsAction.DataStreamStats[0]));
    }

    @Override
    protected DataStreamsStatsResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return DataStreamsStatsResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(DataStreamsStatsAction.Response serverTestInstance, DataStreamsStatsResponse clientInstance) {
        assertEquals(serverTestInstance.getTotalShards(), clientInstance.shards().total());
        assertEquals(serverTestInstance.getSuccessfulShards(), clientInstance.shards().successful());
        assertEquals(serverTestInstance.getFailedShards(), clientInstance.shards().failed());
        assertEquals(serverTestInstance.getShardFailures().length, clientInstance.shards().failures().size());

        assertEquals(serverTestInstance.getDataStreamCount(), clientInstance.getDataStreamCount());
        assertEquals(serverTestInstance.getBackingIndices(), clientInstance.getBackingIndices());
        assertEquals(serverTestInstance.getTotalStoreSize(), clientInstance.getTotalStoreSize());
        assertEquals(serverTestInstance.getDataStreams().length, clientInstance.getDataStreams().size());
        for (DataStreamsStatsAction.DataStreamStats serverStats : serverTestInstance.getDataStreams()) {
            DataStreamsStatsResponse.DataStreamStats clientStats = clientInstance.getDataStreams()
                .get(serverStats.getDataStream());
            assertEquals(serverStats.getDataStream(), clientStats.getDataStream());
            assertEquals(serverStats.getBackingIndices(), clientStats.getBackingIndices());
            assertEquals(serverStats.getStoreSize(), clientStats.getStoreSize());
            assertEquals(serverStats.getMaximumTimestamp(), clientStats.getMaximumTimestamp());
        }
    }
}
