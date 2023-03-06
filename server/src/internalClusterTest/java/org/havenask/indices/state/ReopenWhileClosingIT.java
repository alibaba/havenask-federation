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

package org.havenask.indices.state;

import org.havenask.action.ActionFuture;
import org.havenask.action.admin.indices.close.CloseIndexResponse;
import org.havenask.action.admin.indices.close.TransportVerifyShardBeforeCloseAction;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.common.Glob;
import org.havenask.common.Strings;
import org.havenask.common.lease.Releasable;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.concurrent.RunOnce;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.transport.MockTransportService;
import org.havenask.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.havenask.cluster.metadata.MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID;
import static org.havenask.indices.state.CloseIndexIT.assertIndexIsClosed;
import static org.havenask.indices.state.CloseIndexIT.assertIndexIsOpened;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST)
public class ReopenWhileClosingIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return singletonList(MockTransportService.TestPlugin.class);
    }

    @Override
    protected int minimumNumberOfShards() {
        return 2;
    }

    public void testReopenDuringClose() throws Exception {
        List<String> dataOnlyNodes = internalCluster().startDataOnlyNodes(randomIntBetween(2, 3));
        final String indexName = "test";
        createIndexWithDocs(indexName, dataOnlyNodes);

        ensureYellowAndNoInitializingShards(indexName);

        final CountDownLatch block = new CountDownLatch(1);
        final Releasable releaseBlock = interceptVerifyShardBeforeCloseActions(indexName, block::countDown);

        ActionFuture<CloseIndexResponse> closeIndexResponse = client().admin().indices().prepareClose(indexName).execute();
        assertTrue("Waiting for index to have a closing blocked", block.await(60, TimeUnit.SECONDS));
        assertIndexIsBlocked(indexName);
        assertFalse(closeIndexResponse.isDone());

        assertAcked(client().admin().indices().prepareOpen(indexName));

        releaseBlock.close();
        assertFalse(closeIndexResponse.get().isAcknowledged());
        assertIndexIsOpened(indexName);
    }

    public void testReopenDuringCloseOnMultipleIndices() throws Exception {
        List<String> dataOnlyNodes = internalCluster().startDataOnlyNodes(randomIntBetween(2, 3));
        final List<String> indices = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(2, 10); i++) {
            indices.add("index-" + i);
            createIndexWithDocs(indices.get(i), dataOnlyNodes);
        }

        ensureYellowAndNoInitializingShards(indices.toArray(Strings.EMPTY_ARRAY));

        final CountDownLatch block = new CountDownLatch(1);
        final Releasable releaseBlock = interceptVerifyShardBeforeCloseActions(randomFrom(indices), block::countDown);

        ActionFuture<CloseIndexResponse> closeIndexResponse = client().admin().indices().prepareClose("index-*").execute();
        assertTrue("Waiting for index to have a closing blocked", block.await(60, TimeUnit.SECONDS));
        assertFalse(closeIndexResponse.isDone());
        indices.forEach(ReopenWhileClosingIT::assertIndexIsBlocked);

        final List<String> reopenedIndices = randomSubsetOf(randomIntBetween(1, indices.size()), indices);
        assertAcked(client().admin().indices().prepareOpen(reopenedIndices.toArray(Strings.EMPTY_ARRAY)));

        releaseBlock.close();
        assertFalse(closeIndexResponse.get().isAcknowledged());

        indices.forEach(index -> {
            if (reopenedIndices.contains(index)) {
                assertIndexIsOpened(index);
            } else {
                assertIndexIsClosed(index);
            }
        });
    }

    private void createIndexWithDocs(final String indexName, final Collection<String> dataOnlyNodes) {
        createIndex(indexName,
            Settings.builder().put(indexSettings()).put("index.routing.allocation.include._name", String.join(",", dataOnlyNodes)).build());
        final int nbDocs =  scaledRandomIntBetween(1, 100);
        for (int i = 0; i < nbDocs; i++) {
            index(indexName, "_doc", String.valueOf(i), "num", i);
        }
        assertIndexIsOpened(indexName);
    }

    /**
     * Intercepts and blocks the {@link TransportVerifyShardBeforeCloseAction} executed for the given index pattern.
     */
    private Releasable interceptVerifyShardBeforeCloseActions(final String indexPattern, final Runnable onIntercept) {
        final MockTransportService mockTransportService = (MockTransportService) internalCluster()
            .getInstance(TransportService.class, internalCluster().getMasterName());

        final CountDownLatch release = new CountDownLatch(1);
        for (DiscoveryNode node : internalCluster().clusterService().state().getNodes()) {
            mockTransportService.addSendBehavior(internalCluster().getInstance(TransportService.class, node.getName()),
                (connection, requestId, action, request, options) -> {
                    if (action.startsWith(TransportVerifyShardBeforeCloseAction.NAME)) {
                        if (request instanceof TransportVerifyShardBeforeCloseAction.ShardRequest) {
                            final String index = ((TransportVerifyShardBeforeCloseAction.ShardRequest) request).shardId().getIndexName();
                            if (Glob.globMatch(indexPattern, index)) {
                                logger.info("request {} intercepted for index {}", requestId, index);
                                onIntercept.run();
                                try {
                                    release.await();
                                    logger.info("request {} released for index {}", requestId, index);
                                } catch (final InterruptedException e) {
                                    throw new AssertionError(e);
                                }
                            }
                        }

                    }
                    connection.sendRequest(requestId, action, request, options);
                });
        }
        final RunOnce releaseOnce = new RunOnce(release::countDown);
        return releaseOnce::run;
    }

    private static void assertIndexIsBlocked(final String... indices) {
        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        for (String index : indices) {
            assertThat(clusterState.metadata().indices().get(index).getState(), is(IndexMetadata.State.OPEN));
            assertThat(clusterState.routingTable().index(index), notNullValue());
            assertThat("Index " + index + " must have only 1 block with [id=" + INDEX_CLOSED_BLOCK_ID + "]",
                clusterState.blocks().indices().getOrDefault(index, emptySet()).stream()
                    .filter(clusterBlock -> clusterBlock.id() == INDEX_CLOSED_BLOCK_ID).count(), equalTo(1L));
        }
    }
}
