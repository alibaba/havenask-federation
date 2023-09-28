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
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.routing.RoutingNode;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.SuppressForbidden;
import org.havenask.common.component.AbstractLifecycleComponent;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.concurrent.AbstractAsyncTask;
import org.havenask.engine.index.engine.EngineSettings;
import org.havenask.engine.rpc.HavenaskClient;
import org.havenask.engine.rpc.HeartbeatTargetResponse;
import org.havenask.engine.rpc.TargetInfo;
import org.havenask.engine.rpc.UpdateHeartbeatTargetRequest;
import org.havenask.engine.util.Utils;
import org.havenask.threadpool.ThreadPool;

import static org.havenask.engine.HavenaskEnginePlugin.HAVENASK_THREAD_POOL_NAME;

public class MetaDataSyncer extends AbstractLifecycleComponent {
    private static final Logger LOGGER = LogManager.getLogger(MetaDataSyncer.class);

    private static final int MAX_SYNC_TIMES = 30;
    private static final int TARGET_VERSION = 1651870394;
    private static final int DEFAULT_PART_COUNT = 1;
    private static final int DEFAULT_PART_ID = 0;
    private static final int DEFAULT_VERSION = 0;
    private static final int DEFAULT_TOTAL_PARTITION_COUNT = 1;
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
    private static final String INDEX_ROOT_POSTFIX = "local_search_12000/general_p0_r0/runtimedata";
    private static final String DEFAULT_PARTITION_NAME = "0_65535";
    private static final String INDEX_SUB_PATH = "generation_0/partition_0_65535";
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

    private int randomVersion;
    private int generalSqlRandomVersion;
    private Random random;

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
        enabled = HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.get(settings);

        random = new Random();
        randomVersion = random.nextInt(100000) + 1;
    }

    @Override
    protected void doStart() {
        if (enabled && isDataNode && syncTask == null) {
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

            // 同步元数据,触发条件:
            // 1. synced为false
            // 2. pending为true
            // 3. syncTimes小于MAX_SYNC_TIMES
            if (synced.get() == false || pending.getAndSet(false) == true || syncTimes > MAX_SYNC_TIMES) {
                // update heartbeat target
                LOGGER.info("update heartbeat target, synced: {}, pending: {}, syncTimes: {}", synced.get(), pending.get(), syncTimes);

                try {
                    // TODO qrs每次request都变化,此处是否正常同步成功
                    UpdateHeartbeatTargetRequest qrsTargetRequest = createQrsUpdateHeartbeatTargetRequest();
                    HeartbeatTargetResponse qrsResponse = qrsClient.updateHeartbeatTarget(qrsTargetRequest);

                    UpdateHeartbeatTargetRequest searcherTargetRequest = createSearcherUpdateHeartbeatTargetRequest();
                    HeartbeatTargetResponse searchResponse = searcherClient.updateHeartbeatTarget(searcherTargetRequest);

                    // TODO check target info equals method
                    if (qrsTargetRequest.getTargetInfo().equals(qrsResponse.getSignature())
                        && searcherTargetRequest.getTargetInfo().equals(searchResponse.getSignature())) {
                        LOGGER.info("update heartbeat target success");

                        synced.set(true);
                        searcherTargetInfo.set(searchResponse.getCustomInfo());
                        syncTimes = 0;
                        // 在每次同步成功时更新两个randomVersion
                        randomVersion = random.nextInt(100000) + 1;
                        generalSqlRandomVersion = random.nextInt(100000) + 1;
                        return;
                    } else {
                        LOGGER.trace(
                            "update heartbeat target failed, qrsTargetRequest: {}, qrsResponse: {}, "
                                + "searcherTargetRequest: {}, searchResponse: {}",
                            qrsTargetRequest,
                            qrsResponse,
                            searcherTargetRequest,
                            searchResponse
                        );
                    }
                } catch (Throwable e) {
                    LOGGER.error("update heartbeat target failed", e);
                }

                synced.set(false);
            } else {
                syncTimes++;
            }
        }

        protected String getThreadPool() {
            return HAVENASK_THREAD_POOL_NAME;
        }
    }

    /**
     * 获取searcher target info
     * @return searcher target info
     */
    public TargetInfo getSearcherTargetInfo() {
        return searcherTargetInfo.get();
    }

    /**
     * 设置sync metadata
     */
    public void setPendingSync() {
        pending.set(true);
    }

    public UpdateHeartbeatTargetRequest createQrsUpdateHeartbeatTargetRequest() throws IOException {
        String ip = getIp();

        int searcherTcpPort = nativeProcessControlService.getSearcherTcpPort();
        int searcherGrpcPort = nativeProcessControlService.getSearcherGrpcPort();
        int qrsTcpPort = nativeProcessControlService.getQrsTcpPort();

        TargetInfo qrsTargetInfo = new TargetInfo();
        qrsTargetInfo.clean_disk = CLEAN_DISK;
        qrsTargetInfo.target_version = TARGET_VERSION;
        qrsTargetInfo.service_info = new TargetInfo.ServiceInfo(QRS_ZONE_NAME, DEFAULT_PART_ID, DEFAULT_PART_COUNT);
        qrsTargetInfo.table_info = new HashMap<>();
        qrsTargetInfo.biz_info = new TargetInfo.BizInfo(defaultBizsPath);
        createConfigLink("qrs", "biz", "default", defaultBizsPath, env.getDataPath());
        qrsTargetInfo.catalog_address = ip + ":" + qrsTcpPort;

        // randomVersion = random.nextInt(100000) + 1;
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

    public UpdateHeartbeatTargetRequest createSearcherUpdateHeartbeatTargetRequest() throws IOException {
        createConfigLink("general_p0_r0", "biz", "default", defaultBizsPath, env.getDataPath());
        Path indexRootPath = env.getDataPath().resolve(INDEX_ROOT_POSTFIX);

        ClusterState state = clusterService.state();
        Metadata metadata = state.metadata();

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

        List<String> subDirNames = new ArrayList<>();
        subDirNames.add(TABLE_NAME_IN0);
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            // TODO 抛出异常？
            return null;
        }

        for (ShardRouting shardRouting : localRoutingNode) {
            IndexMetadata indexMetadata = metadata.index(shardRouting.getIndexName());
            if (EngineSettings.isHavenaskEngine(indexMetadata.getSettings())) {
                String tableName = Utils.getHavenaskTableName(shardRouting.shardId());
                subDirNames.add(tableName);
                createConfigLink("general_p0_r0", "table", tableName, defaultBizsPath, env.getDataPath());
            }
        }

        searcherTargetInfo.table_info = new HashMap<>();
        for (String subDir : subDirNames) {
            Path versionPath = defaultRuntimeDataPath.resolve(subDir).resolve(INDEX_SUB_PATH);
            boolean hasRealTime = false;
            if ("in0" != subDir) {
                hasRealTime = true;
            }
            int tableMode = hasRealTime ? 1 : 0;
            int tableType = hasRealTime ? 2 : 3;
            String configPath = defaultTablePath.toString();
            String indexRoot = indexRootPath.toString();
            int totalPartitionCount = DEFAULT_TOTAL_PARTITION_COUNT;

            TargetInfo.TableInfo.Partition curPartition = new TargetInfo.TableInfo.Partition();
            curPartition.inc_version = extractIncVersion(Utils.getIndexMaxVersion(versionPath));

            TargetInfo.TableInfo curTableInfo = new TargetInfo.TableInfo(
                tableMode,
                tableType,
                configPath,
                indexRoot,
                totalPartitionCount,
                DEFAULT_PARTITION_NAME,
                curPartition
            );

            Map<String, TargetInfo.TableInfo> innerMap = new HashMap<String, TargetInfo.TableInfo>() {
                {
                    put("0", curTableInfo);
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
        // TODO 这个方法中有一些地方有可能抛出异常，暂时没有处理，后续需要跟进
        final String localSearchPostFix = "local_search_12000";
        final String zoneConfig = "zone_config";

        String configPathStr = configPath.toString();
        int lastIndex = configPathStr.lastIndexOf("/");
        String version = configPathStr.substring(lastIndex + 1);

        Path rundir = dataPath.resolve(localSearchPostFix).resolve(zoneName);
        Path bizConfigDir = rundir.resolve(zoneConfig).resolve(prefix).resolve(bizName);
        if (false == Files.exists(bizConfigDir)) {
            Files.createDirectories(bizConfigDir);
            // TODO 可能抛出异常
        }
        Path fakeConfigPath = bizConfigDir.resolve(version);

        // TODO 调用clearFolder和copyFolder可能会抛出异常，完善else逻辑
        if (Files.exists(fakeConfigPath) && clearDirectory(fakeConfigPath)) {
            copyDirectory(configPath, fakeConfigPath);
        }
        //Path doneFile = fakeConfigPath.resolve("suez_deploy.done");
        //createFile(doneFile);
    }

    public static boolean clearDirectory(Path directory) throws IOException {
        boolean success = true;
        try (Stream<Path> pathStream = Files.walk(directory)) {
            pathStream.sorted((p1, p2) -> -p1.compareTo(p2)) // 以相反的顺序排序，以便先删除文件再删除目录
                .forEach(path -> {
                    try {
                        Files.delete(path); // 删除文件或目录
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to clear file: " + e.getMessage(), e);
                    }
                });
        }
        return success;
    }

    private static boolean copyDirectory(Path source, Path destination) throws IOException {
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
            return true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to copy file: " + e.getMessage(), e);
        }
    }

    @SuppressForbidden(reason = "InetAddress.getLocalHost(); Need ip address to create update heartbeat target request.")
    private static String getIp() throws IOException {
        List<String> ipAddresses = new ArrayList<>();
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();
                if (!address.isLoopbackAddress() && address instanceof Inet4Address) {
                    ipAddresses.add(address.getHostAddress());
                }
            }
        }
        if (ipAddresses != null && ipAddresses.size() > 0) {
            // TODO 目前是返回了非回环的第一个地址，若有多个ip地址时是否能够更好地处理？
            return ipAddresses.get(0);
        } else {
            return "";
        }
    }
}
