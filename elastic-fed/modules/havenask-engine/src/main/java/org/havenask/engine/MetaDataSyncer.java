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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.cluster.ClusterChangedEvent;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateApplier;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.routing.RoutingNode;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.Strings;
import org.havenask.common.collect.Tuple;
import org.havenask.common.component.AbstractLifecycleComponent;
import org.havenask.common.network.NetworkAddress;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.concurrent.AbstractAsyncTask;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.engine.index.config.ZoneBiz;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.rpc.HavenaskClient;
import org.havenask.engine.rpc.HeartbeatTargetResponse;
import org.havenask.engine.rpc.SqlClientInfoResponse;
import org.havenask.engine.rpc.TargetInfo;
import org.havenask.engine.rpc.UpdateHeartbeatTargetRequest;
import org.havenask.engine.util.RangeUtil;
import org.havenask.engine.util.Utils;
import org.havenask.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.havenask.engine.HavenaskEnginePlugin.HAVENASK_THREAD_POOL_NAME;

public class MetaDataSyncer extends AbstractLifecycleComponent implements ClusterStateApplier {
    private static final Logger LOGGER = LogManager.getLogger(MetaDataSyncer.class);

    private static final int MAX_SYNC_TIMES = 30;
    private static final int MAX_QRS_SYNC_TIMES = 120;
    private static final int TARGET_VERSION = 1651870394;
    private static final int DEFAULT_PART_COUNT = 1;
    private static final int DEFAULT_PART_ID = 0;
    private static final int DEFAULT_VERSION = 0;
    private static final int IN0_PARTITION_COUNT = 2;
    private static final int BASE_VERSION = 2104320000;
    private static final boolean DEFAULT_SUPPORT_HEARTBEAT = true;
    private static final boolean CLEAN_DISK = false;
    private static final String TABLE_NAME_IN0 = "in0";
    private static final String QRS_ZONE_NAME = "qrs";
    private static final String GENERAL_DEFAULT_SQL = "general.default_sql";
    private static final String SEARCHER_ZONE_NAME = "general";
    private static final String DEFAULT_CM2_CONFIG_LOCAL = "local";
    private static final String BIZS_PATH_POSTFIX = "default/0";
    private static final String TABLE_PATH_POSTFIX = "0";
    private static final String INDEX_ROOT_POSTFIX = "runtimedata";
    private static final String DEFAULT_PARTITION_NAME0 = "0_32767";
    private static final String DEFAULT_PARTITION_NAME1 = "32768_65535";
    private static final String INDEX_SUB_PATH0 = "generation_0/partition_0_32767";
    private static final String INDEX_SUB_PATH1 = "generation_0/partition_32768_65535";
    private static final String HAVENASK_WORKSPACCE = "local_search_12000";
    private static final String HAVENASK_SEARCHER_HOME = "general_p0_r0";
    private static final String HAVENASK_QRS_HOME = "qrs";
    private static final String DEFAULT_BIZ_CONFIG = "zones/general/default_biz.json";
    private static final String[] cm2ConfigBizNames = {
        "general.para_search_2",
        "general.para_search_2.search",
        "general.default_sql",
        "general.para_search_4",
        "general.default.search",
        "general.default_agg",
        "general.default_agg.search",
        "general.default",
        "general.para_search_4.search" };

    private final Path defaultBizsPath;
    private final Path defaultTablePath;
    private final Path defaultRuntimeDataPath;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final HavenaskEngineEnvironment env;
    private final NativeProcessControlService nativeProcessControlService;
    private final HavenaskClient searcherClient;
    private final HavenaskClient qrsClient;

    private SyncTask syncTask;
    private boolean running;
    private final boolean enabled;
    private final boolean isDataNode;
    private final boolean isIngestNode;
    private Set<String> qrsIndexNamesSet = new HashSet<>();
    private int randomVersion;
    private int generalSqlRandomVersion;
    private Random random;

    private AtomicBoolean qrsIsUpdating = new AtomicBoolean(false);
    // synced标识metadata当前是否已经同步
    // pending标识是否需要同步, 解决元数据并发修改和更新的同步问题, 由于同步是异步的, 所以需要pending标识是否需要同步
    private AtomicBoolean synced = new AtomicBoolean(false);
    private AtomicBoolean pending = new AtomicBoolean(false);
    private AtomicReference<TargetInfo> searcherTargetInfo = new AtomicReference<>();
    private int syncTimes = 0;

    public MetaDataSyncer(
        ClusterService clusterService,
        ThreadPool threadPool,
        HavenaskEngineEnvironment env,
        NativeProcessControlService nativeProcessControlService,
        HavenaskClient searcherClient,
        HavenaskClient qrsClient
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.env = env;
        env.setMetaDataSyncer(this);
        this.nativeProcessControlService = nativeProcessControlService;
        this.searcherClient = searcherClient;
        this.qrsClient = qrsClient;
        this.defaultBizsPath = env.getBizsPath().resolve(BIZS_PATH_POSTFIX);
        this.defaultTablePath = env.getTablePath().resolve(TABLE_PATH_POSTFIX);
        this.defaultRuntimeDataPath = env.getRuntimedataPath();

        Settings settings = clusterService.getSettings();
        isDataNode = DiscoveryNode.isDataNode(settings);
        isIngestNode = DiscoveryNode.isIngestNode(settings);
        if (isIngestNode) {
            clusterService.addStateApplier(this);
        }
        enabled = HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.get(settings);

        random = new Random();
        randomVersion = random.nextInt(100000) + 1;
        generalSqlRandomVersion = random.nextInt(100000) + 1;
    }

    @Override
    protected void doStart() {
        if (enabled && (isDataNode || isIngestNode) && syncTask == null) {
            syncTask = new SyncTask(threadPool, TimeValue.timeValueSeconds(1));
            syncTask.rescheduleIfNecessary();
            running = true;
        }
    }

    @Override
    protected void doStop() {
        if (syncTask != null) {
            syncTask.close();
            syncTask = null;
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

    public class SyncTask extends AbstractAsyncTask {

        public SyncTask(ThreadPool threadPool, TimeValue interval) {
            super(LOGGER, threadPool, interval, true);
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        protected void runInternal() {
            if (false == running) {
                return;
            }
            if (isDataNode) {
                ClusterState clusterState = clusterService.state();

                synchronized (this) {
                    // 同步元数据,触发条件:
                    // 1. pending为true
                    // 2. synced为false
                    // 3. syncTimes小于MAX_SYNC_TIMES
                    if (pending.getAndSet(false) == true || synced.get() == false || syncTimes > MAX_SYNC_TIMES) {
                        // update searcher heartbeat target
                        LOGGER.info(
                            "update searcher heartbeat target, synced: {}, pending: {}, syncTimes: {}",
                            synced.get(),
                            pending.get(),
                            syncTimes
                        );

                        try {
                            UpdateHeartbeatTargetRequest searcherTargetRequest = createSearcherUpdateHeartbeatTargetRequest(clusterState);
                            HeartbeatTargetResponse searcherResponse = searcherClient.updateHeartbeatTarget(searcherTargetRequest);

                            boolean searcherEquals = searcherTargetRequest.getTargetInfo().equals(searcherResponse.getSignature());

                            if (searcherEquals) {
                                // TODO: randomVersion的内容如何修改
                                randomVersion = random.nextInt(100000) + 1;
                                LOGGER.trace("searcher Equals success, update version");

                                LOGGER.info("update searcher heartbeat target success");
                                synced.set(true);
                                searcherTargetInfo.set(searcherResponse.getCustomInfo());
                                syncTimes = 0;
                                return;
                            } else {
                                LOGGER.trace(
                                    "update searcher heartbeat target failed, searcherTargetRequest: {}, searcherResponse: {}",
                                    Strings.toString(searcherTargetRequest),
                                    Strings.toString(searcherResponse)
                                );
                            }
                        } catch (Throwable e) {
                            LOGGER.error("update searcher heartbeat target failed", e);
                        }

                        synced.set(false);
                    } else {
                        syncTimes++;
                    }
                }
            }

            if (isIngestNode) {
                if (syncTimes > MAX_SYNC_TIMES && false == qrsIsUpdating.getAndSet(true)) {
                    LOGGER.trace("update qrs target, guaranteed");
                    updateQrsTargetInfo(clusterService.state());
                    syncTimes = 0;
                } else {
                    syncTimes++;
                }
            }
        }

        protected String getThreadPool() {
            return HAVENASK_THREAD_POOL_NAME;
        }
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        // TODO: 是否需要类似searcher的30s触发一次的兜底同步逻辑
        if (isIngestNode && shouldUpdateQrs(event.previousState(), event.state()) && false == qrsIsUpdating.getAndSet(true)) {
            // update qrs target
            updateQrsTargetInfo(event.state());
            syncTimes = 0;
        }
    }

    private boolean shouldUpdateQrs(ClusterState prevClusterState, ClusterState curClusterState) {
        // check 是否有索引级别的增删
        if (isHavenaskIndexChanged(prevClusterState, curClusterState)) {
            return true;
        }

        // TODO: check 分片的搬迁是否要更新qrs
        if (isHavenaskShardChanged(prevClusterState, curClusterState)) {
            return true;
        }

        return false;
    }

    private void updateQrsTargetInfo(ClusterState clusterState) {
        threadPool.executor(HavenaskEnginePlugin.HAVENASK_THREAD_POOL_NAME).execute(() -> {
            boolean qrsEquals = false;
            int qrsSyncTimes = 0;
            try {
                while (false == qrsEquals) {
                    UpdateHeartbeatTargetRequest qrsTargetRequest = createQrsUpdateHeartbeatTargetRequest(clusterState);
                    HeartbeatTargetResponse qrsResponse = qrsClient.updateHeartbeatTarget(qrsTargetRequest);

                    qrsEquals = qrsTargetRequest.getTargetInfo().equals(qrsResponse.getSignature());
                    if (false == qrsEquals) {
                        LOGGER.trace(
                            "update qrs heartbeat target failed, qrsTargetRequest: {}, qrsResponse: {}",
                            Strings.toString(qrsTargetRequest),
                            Strings.toString(qrsResponse)
                        );
                        Thread.sleep(1000);
                        ++qrsSyncTimes;
                        if (qrsSyncTimes > MAX_QRS_SYNC_TIMES) {
                            LOGGER.error("update qrs heartbeat target time out, qrsSyncTimes: {}", qrsSyncTimes);
                            qrsIsUpdating.set(false);
                            return;
                        }
                    }
                }
            } catch (Throwable e) {
                LOGGER.error("update qrs heartbeat target failed", e);
            }

            if (qrsEquals) {
                randomVersion = random.nextInt(100000) + 1;
                generalSqlRandomVersion = random.nextInt(100000) + 1;
                LOGGER.trace("searcher Equals success, update version");

                LOGGER.info("update qrs heartbeat target success");
                qrsIsUpdating.set(false);
            }
        });
    }

    /**
     * 获取searcher target info
     *
     * @return searcher target info
     */
    public TargetInfo getSearcherTargetInfo() {
        return searcherTargetInfo.get();
    }

    /**
     * 设置sync metadata
     */
    public synchronized void setPendingSync() {
        pending.set(true);
        searcherTargetInfo.set(null);
    }

    public UpdateHeartbeatTargetRequest createQrsUpdateHeartbeatTargetRequest(ClusterState clusterState) throws IOException {
        String ip = NetworkAddress.format(clusterState.nodes().getLocalNode().getAddress().address().getAddress());

        int searcherTcpPort = nativeProcessControlService.getSearcherTcpPort();
        int searcherGrpcPort = nativeProcessControlService.getSearcherGrpcPort();
        int qrsTcpPort = nativeProcessControlService.getQrsTcpPort();

        TargetInfo qrsTargetInfo = new TargetInfo();
        qrsTargetInfo.clean_disk = CLEAN_DISK;
        qrsTargetInfo.target_version = TARGET_VERSION;
        qrsTargetInfo.service_info = new TargetInfo.ServiceInfo(QRS_ZONE_NAME, DEFAULT_PART_ID, DEFAULT_PART_COUNT);
        qrsTargetInfo.table_info = new HashMap<>();
        qrsTargetInfo.biz_info = new TargetInfo.BizInfo(defaultBizsPath);
        createConfigLink(HAVENASK_QRS_HOME, "biz", "default", defaultBizsPath, env.getDataPath());
        qrsTargetInfo.catalog_address = ip + ":" + qrsTcpPort;

        List<TargetInfo.ServiceInfo.Cm2Config> cm2ConfigLocalVal = new ArrayList<>();
        for (String bizName : cm2ConfigBizNames) {
            TargetInfo.ServiceInfo.Cm2Config curCm2Config = new TargetInfo.ServiceInfo.Cm2Config();
            curCm2Config.part_count = DEFAULT_PART_COUNT;
            curCm2Config.biz_name = bizName;
            curCm2Config.ip = ip;
            if (GENERAL_DEFAULT_SQL == bizName) {
                curCm2Config.version = generalSqlRandomVersion;
            } else {
                curCm2Config.version = BASE_VERSION + randomVersion;
            }
            curCm2Config.part_id = DEFAULT_PART_ID;
            curCm2Config.tcp_port = searcherTcpPort;
            curCm2Config.support_heartbeat = DEFAULT_SUPPORT_HEARTBEAT;
            curCm2Config.grpc_port = searcherGrpcPort;
            cm2ConfigLocalVal.add(curCm2Config);
        }
        qrsTargetInfo.service_info.cm2_config = new HashMap<>();
        qrsTargetInfo.service_info.cm2_config.put(DEFAULT_CM2_CONFIG_LOCAL, cm2ConfigLocalVal);

        return new UpdateHeartbeatTargetRequest(qrsTargetInfo);
    }

    public UpdateHeartbeatTargetRequest createSearcherUpdateHeartbeatTargetRequest(ClusterState clusterState) throws IOException {
        createConfigLink(HAVENASK_SEARCHER_HOME, "biz", "default", defaultBizsPath, env.getDataPath());
        Path indexRootPath = env.getDataPath().resolve(HAVENASK_WORKSPACCE).resolve(HAVENASK_SEARCHER_HOME).resolve(INDEX_ROOT_POSTFIX);

        TargetInfo searcherTargetInfo = new TargetInfo();
        searcherTargetInfo.clean_disk = CLEAN_DISK;
        searcherTargetInfo.target_version = TARGET_VERSION;
        searcherTargetInfo.service_info = new TargetInfo.ServiceInfo(
            SEARCHER_ZONE_NAME,
            DEFAULT_PART_ID,
            DEFAULT_PART_COUNT,
            DEFAULT_VERSION
        );
        searcherTargetInfo.biz_info = new TargetInfo.BizInfo(defaultBizsPath);

        List<String> subDirNames = getSubDirNames(clusterState);
        for (String tableName : subDirNames) {
            createConfigLink(HAVENASK_SEARCHER_HOME, "table", tableName, defaultTablePath, env.getDataPath());
        }
        subDirNames.add(TABLE_NAME_IN0);

        // update table info
        generateDefaultBizConfig(subDirNames);

        searcherTargetInfo.table_info = new HashMap<>();
        Map<String, Tuple<Integer, Set<Integer>>> indexShards = getIndexShards(clusterState);
        for (String subDir : subDirNames) {
            String configPath = defaultTablePath.toString();
            String indexRoot = indexRootPath.toString();

            TargetInfo.TableInfo curTableInfo;
            if (TABLE_NAME_IN0 == subDir) {
                int tableMode = 0;
                int tableType = 3;

                int totalPartitionCount = IN0_PARTITION_COUNT;
                Map<String, TargetInfo.TableInfo.Partition> partitions = new HashMap<>();
                {
                    Path versionPath = defaultRuntimeDataPath.resolve(subDir).resolve(INDEX_SUB_PATH0);
                    TargetInfo.TableInfo.Partition curPartition = new TargetInfo.TableInfo.Partition();
                    curPartition.inc_version = extractIncVersion(Utils.getIndexMaxVersion(versionPath));
                    partitions.put(DEFAULT_PARTITION_NAME0, curPartition);
                }

                {
                    Path versionPath = defaultRuntimeDataPath.resolve(subDir).resolve(INDEX_SUB_PATH1);
                    TargetInfo.TableInfo.Partition curPartition = new TargetInfo.TableInfo.Partition();
                    curPartition.inc_version = extractIncVersion(Utils.getIndexMaxVersion(versionPath));
                    partitions.put(DEFAULT_PARTITION_NAME1, curPartition);
                }

                curTableInfo = new TargetInfo.TableInfo(tableMode, tableType, configPath, indexRoot, totalPartitionCount, partitions);
            } else {
                int tableMode = 1;
                int tableType = 2;
                Tuple<Integer, Set<Integer>> shards = indexShards.get(subDir);
                int totalPartitionCount = shards.v1();
                Map<String, TargetInfo.TableInfo.Partition> partitions = new HashMap<>();
                shards.v2().forEach(shardId -> {
                    String partitionName = RangeUtil.getRangePartition(totalPartitionCount, shardId);
                    String partitionId = RangeUtil.getRangeName(totalPartitionCount, shardId);
                    TargetInfo.TableInfo.Partition curPartition = new TargetInfo.TableInfo.Partition();
                    Path versionPath = defaultRuntimeDataPath.resolve(subDir).resolve("generation_0").resolve(partitionName);
                    curPartition.inc_version = extractIncVersion(Utils.getIndexMaxVersion(versionPath));
                    partitions.put(partitionId, curPartition);
                });
                curTableInfo = new TargetInfo.TableInfo(tableMode, tableType, configPath, indexRoot, totalPartitionCount, partitions);
            }

            Map<String, TargetInfo.TableInfo> innerMap = new HashMap<>() {
                {
                    put(getMaxGenerationId(defaultRuntimeDataPath, subDir), curTableInfo);
                }
            };
            searcherTargetInfo.table_info.put(subDir, innerMap);
        }

        return new UpdateHeartbeatTargetRequest(searcherTargetInfo);
    }

    private static int extractIncVersion(String versionStr) {
        String pattern = "version\\.(\\d+)";
        Pattern regex = Pattern.compile(pattern);
        Matcher matcher = regex.matcher(versionStr);
        if (matcher.find()) {
            String numberStr = matcher.group(1);
            int number = Integer.parseInt(numberStr);
            return number;
        } else {
            return -1;
        }
    }

    private static void createConfigLink(String zoneName, String prefix, String bizName, Path configPath, Path dataPath)
        throws IOException {
        final String zoneConfig = "zone_config";

        String configPathStr = configPath.toString();
        int lastIndex = configPathStr.lastIndexOf("/");
        String version = configPathStr.substring(lastIndex + 1);

        Path rundir = dataPath.resolve(HAVENASK_WORKSPACCE).resolve(zoneName);
        Path bizConfigDir = rundir.resolve(zoneConfig).resolve(prefix).resolve(bizName);
        if (false == Files.exists(bizConfigDir)) {
            Files.createDirectories(bizConfigDir);
        }
        Path fakeConfigPath = bizConfigDir.resolve(version);

        if (Files.exists(fakeConfigPath)) {
            IOUtils.rm(fakeConfigPath);
        }
        copyDirectory(configPath, fakeConfigPath);
    }

    private static void copyDirectory(Path source, Path destination) throws IOException {
        try {
            // 拷贝所有文件与目录到目标路径
            Files.walk(source).forEach(sourcePath -> {
                try {
                    Path targetPath = destination.resolve(source.relativize(sourcePath));
                    if (Files.isDirectory(sourcePath) && !Files.exists(targetPath)) {
                        Files.createDirectory(targetPath);
                    } else {
                        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to copy file: " + e.getMessage(), e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to copy file: " + e.getMessage(), e);
        }
    }

    private static String getMaxGenerationId(Path indexPath, String tableName) throws IOException {
        Path dest = indexPath.resolve(tableName);
        Pattern pattern = Pattern.compile("generation_(\\d+)");
        int maxId = -1;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dest, "generation_*")) {
            for (Path path : stream) {
                String fileName = path.getFileName().toString();
                Matcher matcher = pattern.matcher(fileName);
                if (matcher.matches()) {
                    int id = Integer.parseInt(matcher.group(1));
                    maxId = Math.max(maxId, id);
                }
            }
        }
        return String.valueOf(maxId);
    }

    private static List<String> getSubDirNames(ClusterState clusterState) {
        List<String> subDirNames = new ArrayList<>();
        RoutingNode localRoutingNode = clusterState.getRoutingNodes().node(clusterState.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            throw new RuntimeException("localRoutingNode is null");
        }

        for (ShardRouting shardRouting : localRoutingNode) {
            IndexMetadata indexMetadata = clusterState.metadata().index(shardRouting.getIndexName());
            if (EngineSettings.isHavenaskEngine(indexMetadata.getSettings())) {
                String tableName = Utils.getHavenaskTableName(shardRouting.shardId());
                subDirNames.add(tableName);
            }
        }
        return subDirNames;
    }

    private static Map<String, Tuple<Integer, Set<Integer>>> getIndexShards(ClusterState clusterState) {
        Map<String, Tuple<Integer, Set<Integer>>> indexShards = new HashMap<>();
        RoutingNode localRoutingNode = clusterState.getRoutingNodes().node(clusterState.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            throw new RuntimeException("localRoutingNode is null");
        }

        for (ShardRouting shardRouting : localRoutingNode) {
            IndexMetadata indexMetadata = clusterState.metadata().index(shardRouting.getIndexName());
            if (EngineSettings.isHavenaskEngine(indexMetadata.getSettings())) {
                if (indexShards.containsKey(shardRouting.getIndexName())) {
                    indexShards.get(shardRouting.getIndexName()).v2().add(shardRouting.getId());
                } else {
                    Set<Integer> shards = new HashSet<>();
                    shards.add(shardRouting.getId());
                    indexShards.put(shardRouting.getIndexName(), new Tuple<>(indexMetadata.getNumberOfShards(), shards));
                }
            }
        }
        return indexShards;
    }

    private synchronized void generateDefaultBizConfig(List<String> indexList) throws IOException {
        Path defaultBizConfigPath = defaultBizsPath.resolve(DEFAULT_BIZ_CONFIG);
        String strZone = Files.readString(defaultBizConfigPath, StandardCharsets.UTF_8);
        ZoneBiz zoneBiz = ZoneBiz.parse(strZone);
        zoneBiz.turing_options_config.dependency_table = new HashSet<>(indexList);
        Files.write(
            defaultBizConfigPath,
            zoneBiz.toString().getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    private static boolean qrsTableCheck(List<String> subDirNames, SqlClientInfoResponse sqlClientInfoResponse) {
        boolean qrsTableSynced = true;
        Map<String, Object> expectedSubNames = sqlClientInfoResponse.getResult()
            .getJSONObject("default")
            .getJSONObject("general")
            .getJSONObject("tables");
        for (String subDir : subDirNames) {
            if (false == expectedSubNames.containsKey(subDir)) {
                qrsTableSynced = false;
                break;
            }
        }
        return qrsTableSynced;
    }

    private boolean isHavenaskIndexChanged(ClusterState prevClusterState, ClusterState curClusterState) {
        Set<String> prevIndexNamesSet = new HashSet<>(Arrays.asList(prevClusterState.metadata().indices().keys().toArray(String.class)));
        Set<String> currentIndexNamesSet = new HashSet<>(Arrays.asList(curClusterState.metadata().indices().keys().toArray(String.class)));
        Set<String> prevDiff = new HashSet<>(prevIndexNamesSet);
        Set<String> curDiff = new HashSet<>(currentIndexNamesSet);
        prevDiff.removeAll(currentIndexNamesSet);
        curDiff.removeAll(prevIndexNamesSet);

        for (String indexName : prevDiff) {
            IndexMetadata indexMetadata = prevClusterState.metadata().index(indexName);
            if (EngineSettings.isHavenaskEngine(indexMetadata.getSettings())) {
                return true;
            }
        }

        for (String indexName : curDiff) {
            IndexMetadata indexMetadata = curClusterState.metadata().index(indexName);
            if (EngineSettings.isHavenaskEngine(indexMetadata.getSettings())) {
                return true;
            }
        }

        return false;
    }

    private boolean isHavenaskShardChanged(ClusterState prevClusterState, ClusterState curClusterState) {

        return false;
    }
}
