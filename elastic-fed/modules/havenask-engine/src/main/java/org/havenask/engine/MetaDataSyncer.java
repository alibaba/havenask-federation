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

import org.havenask.cluster.ClusterState;
import org.havenask.cluster.routing.RoutingNode;
import org.havenask.cluster.routing.ShardRouting;
import org.havenask.common.SuppressForbidden;
import org.havenask.engine.rpc.TargetInfo;
import org.havenask.engine.rpc.UpdateHeartbeatTargetRequest;
import org.havenask.engine.util.Utils;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetaDataSyncer {
    private static final int TARGET_VERSION = 1651870394;
    private static final int DEFAULT_PART_COUNT = 1;
    private static final int DEFAULT_PART_ID = 0;
    private static final int DEFAULT_VERSION = 0;
    private static final int DEFAULT_TOTAL_PARTITION_COUNT = 1;
    private static final int BASE_VERSION = 2104320000;
    private static final boolean DEFAULT_SUPPORT_HEARTBEAT = true;
    private static final boolean CLEAN_DISK = false;
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
    private final ClusterState state;
    private final HavenaskEngineEnvironment env;
    private final NativeProcessControlService nativeProcessControlService;

    public MetaDataSyncer(ClusterState state, HavenaskEngineEnvironment env, NativeProcessControlService nativeProcessControlService) {
        this.state = state;
        this.env = env;
        this.nativeProcessControlService = nativeProcessControlService;
        this.defaultBizsPath = env.getBizsPath().resolve(BIZS_PATH_POSTFIX);
        this.defaultTablePath = env.getTablePath().resolve(TABLE_PATH_POSTFIX);
        this.defaultRuntimeDataPath = env.getRuntimedataPath();
    }

    public UpdateHeartbeatTargetRequest createQrsUpdateHeartbeatTargetRequest() throws IOException {
        String ip = getIp();

        int searcherTcpPort = nativeProcessControlService.getSearcherTcpPort();
        int searcherGrpcPort = nativeProcessControlService.getSearcherGrpcPort();
        int qrsTcpPort = nativeProcessControlService.getQrsTcpPort();

        TargetInfo qrsTargetInfo = new TargetInfo();
        qrsTargetInfo.clean_disk = CLEAN_DISK;
        qrsTargetInfo.target_version = TARGET_VERSION;
        qrsTargetInfo.service_info = new TargetInfo.ServiceInfo(QRS_ZONE_NAME, DEFAULT_PART_COUNT, DEFAULT_PART_ID);
        qrsTargetInfo.table_info = new HashMap<>();
        qrsTargetInfo.biz_info = new TargetInfo.BizInfo(defaultBizsPath);
        qrsTargetInfo.catalog_address = ip + ":" + qrsTcpPort;

        Random random = new Random();
        int randomVersion = random.nextInt(100000) + 1;
        List<TargetInfo.ServiceInfo.cm2Config> cm2ConfigLocalVal = new ArrayList<>();
        for (String bizName : cm2ConfigBizNames) {
            TargetInfo.ServiceInfo.cm2Config curCm2Config = new TargetInfo.ServiceInfo.cm2Config();
            curCm2Config.part_count = DEFAULT_PART_COUNT;
            curCm2Config.biz_name = bizName;
            curCm2Config.ip = ip;
            if (GENERAL_DEFAULT_SQL == bizName) {
                curCm2Config.version = random.nextInt(100000) + 1;
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
        Path indexRootPath = env.getDataPath().resolve(INDEX_ROOT_POSTFIX);

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
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            // TODO 抛出异常？
            return null;
        }

        for (ShardRouting shardRouting : localRoutingNode) {
            subDirNames.add(shardRouting.getIndexName());
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
