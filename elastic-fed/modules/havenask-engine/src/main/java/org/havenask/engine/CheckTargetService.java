/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.client.Client;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.routing.RoutingNode;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.component.AbstractLifecycleComponent;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.concurrent.AbstractAsyncTask;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.rpc.HavenaskClient;
import org.havenask.engine.rpc.HeartbeatTargetResponse;
import org.havenask.engine.rpc.TargetInfo;
import org.havenask.engine.search.action.HavenaskSqlClientInfoAction;
import org.havenask.threadpool.ThreadPool;

/**
 * 检查searcher qrs进程加载数据表的情况, 是否和fed元数据保持一致
 */
public class CheckTargetService extends AbstractLifecycleComponent {
    private static final Logger LOGGER = LogManager.getLogger(CheckTargetService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Client client;
    private final NativeProcessControlService nativeProcessControlService;
    private final HavenaskClient searcherClient;
    private final boolean enabled;
    private final boolean isDataNode;
    private final boolean isIngestNode;

    private CheckTask checkTask;
    private boolean running;

    public CheckTargetService(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NativeProcessControlService nativeProcessControlService,
        HavenaskClient searcherClient
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.client = client;
        this.nativeProcessControlService = nativeProcessControlService;
        this.searcherClient = searcherClient;

        Settings settings = clusterService.getSettings();
        isDataNode = DiscoveryNode.isDataNode(settings);
        isIngestNode = DiscoveryNode.isIngestNode(settings);
        enabled = HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.get(settings);
    }

    @Override
    protected void doStart() {
        if (enabled && checkTask == null) {
            checkTask = new CheckTask(threadPool, TimeValue.timeValueSeconds(30));
            checkTask.rescheduleIfNecessary();
            running = true;
        }
    }

    @Override
    protected void doStop() {
        if (checkTask != null) {
            checkTask.close();
            checkTask = null;
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

    class CheckTask extends AbstractAsyncTask {

        protected CheckTask(ThreadPool threadPool, TimeValue interval) {
            super(LOGGER, threadPool, interval, true);
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void runInternal() {
            if (false == running) {
                return;
            }

            ClusterState clusterState = clusterService.state();

            if (isDataNode) {
                try {
                    HeartbeatTargetResponse heartbeatTargetResponse = searcherClient.getHeartbeatTarget();
                    if (heartbeatTargetResponse.getCustomInfo() == null) {
                        throw new IOException("havenask get heartbeat target failed");
                    }
                    TargetInfo targetInfo = heartbeatTargetResponse.getCustomInfo();
                    Set<String> searcherTables = new HashSet<>(targetInfo.table_info.keySet());
                    searcherTables.remove("in0");

                    RoutingNode localRoutingNode = clusterState.getRoutingNodes().node(clusterState.nodes().getLocalNodeId());
                    if (localRoutingNode == null) {
                        return;
                    }
                    Set<String> indices = new HashSet<>();
                    for (ShardRouting shardRouting : localRoutingNode) {
                        indices.add(shardRouting.getIndexName());
                    }

                    Set<String> havenaskIndices = new HashSet<>();
                    indices.forEach((index) -> {
                        IndexMetadata indexMetadata = clusterState.metadata().index(index);
                        if (EngineSettings.isHavenaskEngine(indexMetadata.getSettings())) {
                            havenaskIndices.add(index);
                        }
                    });

                    if (false == searcherTables.equals(havenaskIndices)) {
                        LOGGER.info(
                            "havenask searcher heartbeat target is not equal to data node, update searcher target, "
                                + "searcher tables: {}, data node indices: {}",
                            searcherTables,
                            havenaskIndices
                        );

                        nativeProcessControlService.updateDataNodeTarget();
                        nativeProcessControlService.updateIngestNodeTarget();
                    }

                    // TODO 如果有failed的index, searcher正常加载后, shard状态无法恢复成started
                } catch (Exception e) {
                    LOGGER.warn("havenask check searcher heartbeat target failed", e);
                }
            }

            if (isIngestNode) {
                Set<String> havenaskIndices = new HashSet<>();
                clusterState.metadata().indices().forEach((index) -> {
                    IndexMetadata indexMetadata = index.value;
                    if (EngineSettings.isHavenaskEngine(indexMetadata.getSettings())) {
                        havenaskIndices.add(indexMetadata.getIndex().getName());
                    }
                });

                HavenaskSqlClientInfoAction.Response sqlInfoResponse = client.execute(
                    HavenaskSqlClientInfoAction.INSTANCE,
                    new HavenaskSqlClientInfoAction.Request()
                ).actionGet();
                Map<String, Object> result = sqlInfoResponse.getResult();
                if (result != null
                    && result.get("default") != null
                    && ((Map<String, Object>) (result.get("default"))).get("general") != null
                    && ((Map<String, Object>) ((Map<String, Object>) result.get("default")).get("general")).get("tables") != null) {
                    Map<String, Object> tables = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) result.get("default"))
                        .get("general")).get("tables");
                    Set<String> tablesSet = tables.keySet()
                        .stream()
                        .filter(key -> false == key.endsWith("_summary_"))
                        .collect(Collectors.toSet());
                    if (false == havenaskIndices.equals(tablesSet)) {
                        // qrs记录的数据表跟元数据不一致, 更新searcher/qrs的target
                        LOGGER.info(
                            "havenask indices not equal to qrs tables, update target, havenask indices:{}, qrs " + "tables:{}",
                            havenaskIndices,
                            tablesSet
                        );
                        nativeProcessControlService.updateDataNodeTarget();
                        nativeProcessControlService.updateIngestNodeTarget();
                    }

                }
            }
        }
    }
}
