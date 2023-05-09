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

package org.havenask.action.main;

import org.havenask.LegacyESVersion;
import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.block.ClusterBlock;
import org.havenask.cluster.block.ClusterBlockLevel;
import org.havenask.cluster.block.ClusterBlocks;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.rest.RestStatus;
import org.havenask.tasks.Task;
import org.havenask.test.HavenaskTestCase;
import org.havenask.transport.Transport;
import org.havenask.transport.TransportService;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.havenask.action.main.TransportMainAction.OVERRIDE_MAIN_RESPONSE_VERSION_KEY;

public class MainActionTests extends HavenaskTestCase {

    public void testMainActionClusterAvailable() {
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterName clusterName = new ClusterName("havenask");
        final Settings settings = Settings.builder().put("node.name", "my-node").build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY,
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        ClusterBlocks blocks;
        if (randomBoolean()) {
            if (randomBoolean()) {
                blocks = ClusterBlocks.EMPTY_CLUSTER_BLOCK;
            } else {
                blocks = ClusterBlocks.builder()
                    .addGlobalBlock(new ClusterBlock(randomIntBetween(1, 16), "test global block 400", randomBoolean(), randomBoolean(),
                        false, RestStatus.BAD_REQUEST, ClusterBlockLevel.ALL))
                    .build();
            }
        } else {
            blocks = ClusterBlocks.builder()
                .addGlobalBlock(new ClusterBlock(randomIntBetween(1, 16), "test global block 503", randomBoolean(), randomBoolean(),
                    false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL))
                .build();
        }
        ClusterState state = ClusterState.builder(clusterName).blocks(blocks).build();
        when(clusterService.state()).thenReturn(state);

        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null, TransportService
            .NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());
        TransportMainAction action = new TransportMainAction(settings, transportService, mock(ActionFilters.class), clusterService);
        AtomicReference<MainResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), new MainRequest(), new ActionListener<MainResponse>() {
            @Override
            public void onResponse(MainResponse mainResponse) {
                responseRef.set(mainResponse);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("unexpected error", e);
            }
        });

        assertNotNull(responseRef.get());
        verify(clusterService, times(1)).state();
    }

    public void testMainResponseVersionOverrideEnabledByConfigSetting() {
        final ClusterName clusterName = new ClusterName("havenask");
        ClusterState state = ClusterState.builder(clusterName).blocks(mock(ClusterBlocks.class)).build();

        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY,
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));

        TransportService transportService = new TransportService(Settings.EMPTY, mock(Transport.class), null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null, Collections.emptySet());

        final Settings settings = Settings.builder()
            .put("node.name", "my-node")
            .put(OVERRIDE_MAIN_RESPONSE_VERSION_KEY, true)
            .build();

        TransportMainAction action = new TransportMainAction(settings, transportService, mock(ActionFilters.class), clusterService);
        AtomicReference<MainResponse> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), new MainRequest(), new ActionListener<MainResponse>() {
            @Override
            public void onResponse(MainResponse mainResponse) {
                responseRef.set(mainResponse);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("unexpected error", e);
            }
        });

        final MainResponse mainResponse = responseRef.get();
        assertEquals(LegacyESVersion.V_7_10_2.toString(), mainResponse.getVersionNumber());
        assertWarnings(TransportMainAction.OVERRIDE_MAIN_RESPONSE_VERSION_DEPRECATION_MESSAGE);
    }
}
