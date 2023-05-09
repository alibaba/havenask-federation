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

package org.havenask.index.seqno;

import org.havenask.action.ActionListener;
import org.havenask.index.IndexingPressure;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.ActionTestUtils;
import org.havenask.action.support.PlainActionFuture;
import org.havenask.action.support.replication.TransportReplicationAction;
import org.havenask.cluster.action.shard.ShardStateAction;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.settings.Settings;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.index.Index;
import org.havenask.index.IndexService;
import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.ShardId;
import org.havenask.indices.IndicesService;
import org.havenask.indices.SystemIndices;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.transport.CapturingTransport;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.mock.orig.Mockito.when;
import static org.havenask.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RetentionLeaseSyncActionTests extends HavenaskTestCase {

    private ThreadPool threadPool;
    private CapturingTransport transport;
    private ClusterService clusterService;
    private TransportService transportService;
    private ShardStateAction shardStateAction;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
                clusterService.getSettings(),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundAddress -> clusterService.localNode(),
                null,
                Collections.emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();
        shardStateAction = new ShardStateAction(clusterService, transportService, null, null, threadPool);
    }

    public void tearDown() throws Exception {
        try {
            IOUtils.close(transportService, clusterService, transport);
        } finally {
            terminate(threadPool);
        }
        super.tearDown();
    }

    public void testRetentionLeaseSyncActionOnPrimary() {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        final RetentionLeaseSyncAction action = new RetentionLeaseSyncAction(
                Settings.EMPTY,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                new ActionFilters(Collections.emptySet()),
                new IndexingPressure(Settings.EMPTY),
                new SystemIndices(emptyMap()));
        final RetentionLeases retentionLeases = mock(RetentionLeases.class);
        final RetentionLeaseSyncAction.Request request = new RetentionLeaseSyncAction.Request(indexShard.shardId(), retentionLeases);
        action.dispatchedShardOperationOnPrimary(request, indexShard,
            ActionTestUtils.assertNoFailureListener(result -> {
                    // the retention leases on the shard should be persisted
                    verify(indexShard).persistRetentionLeases();
                    // we should forward the request containing the current retention leases to the replica
                    assertThat(result.replicaRequest(), sameInstance(request));
                    // we should start with an empty replication response
                    assertNull(result.finalResponseIfSuccessful.getShardInfo());
                }
            ));
    }

    public void testRetentionLeaseSyncActionOnReplica() throws Exception {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        final RetentionLeaseSyncAction action = new RetentionLeaseSyncAction(
                Settings.EMPTY,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                new ActionFilters(Collections.emptySet()),
                new IndexingPressure(Settings.EMPTY),
                new SystemIndices(emptyMap()));
        final RetentionLeases retentionLeases = mock(RetentionLeases.class);
        final RetentionLeaseSyncAction.Request request = new RetentionLeaseSyncAction.Request(indexShard.shardId(), retentionLeases);

        PlainActionFuture<TransportReplicationAction.ReplicaResult> listener = PlainActionFuture.newFuture();
        action.dispatchedShardOperationOnReplica(request, indexShard, listener);
        final TransportReplicationAction.ReplicaResult result = listener.actionGet();
        // the retention leases on the shard should be updated
        verify(indexShard).updateRetentionLeasesOnReplica(retentionLeases);
        // the retention leases on the shard should be persisted
        verify(indexShard).persistRetentionLeases();
        // the result should indicate success
        final AtomicBoolean success = new AtomicBoolean();
        result.runPostReplicaActions(ActionListener.wrap(r -> success.set(true), e -> fail(e.toString())));
        assertTrue(success.get());
    }


    public void testBlocks() {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        final RetentionLeaseSyncAction action = new RetentionLeaseSyncAction(
                Settings.EMPTY,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                new ActionFilters(Collections.emptySet()),
                new IndexingPressure(Settings.EMPTY),
                new SystemIndices(emptyMap()));

        assertNull(action.indexBlockLevel());
    }

}
