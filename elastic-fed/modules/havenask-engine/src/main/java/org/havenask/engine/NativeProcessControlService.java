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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.client.Client;
import org.havenask.client.Requests;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.component.AbstractLifecycleComponent;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Setting.Property;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.concurrent.AbstractAsyncTask;
import org.havenask.engine.index.engine.HavenaskEngine;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.index.engine.EngineException;
import org.havenask.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class NativeProcessControlService extends AbstractLifecycleComponent {
    private static final Logger LOGGER = LogManager.getLogger(NativeProcessControlService.class);
    public static final String SEARCHER_ROLE = "searcher";
    public static final String QRS_ROLE = "qrs";
    private static final String START_SEARCHER_COMMAND = "cd %s;python %s/havenask-command/general_search_starter.py -i "
        + "%s -c %s -b /ha3_install -T in0 -p 30468,30480 --enableMultiPartition --role searcher --httpBindPort %d --arpcBindPort %d "
        + "--grpcBindPort %d >> search.log 2>> search.error.log";
    private static final String START_QRS_COMMAND = "cd %s;python %s/havenask-command/general_search_starter.py -i "
        + "%s -c %s -b /ha3_install -T in0 -p 30468,30480 --role qrs --httpBindPort %d --arpcBindPort %d >> qrs.log "
        + "2>> qrs.error.log";
    private static final String UPDATE_SEARCHER_COMMAND = "cd %s;python %s/havenask-command/general_search_updater.py -i "
        + "%s -c %s -T in0 -p 30468,30480 --role searcher >> search.log 2>> search.error.log";
    private static final String UPDATE_QRS_COMMAND = "cd %s;python %s/havenask-command/general_search_updater.py -i "
        + "%s -c %s -T in0 -p 30468,30480 --role qrs >> qrs.log 2>> qrs.error.log";
    private static final String STOP_HAVENASK_COMMAND = "cd %s;python %s/havenask-command/general_search_stop.py"
        + " -c /ha3_install/usr/local/etc/sql/sql_alog.conf >> search.log 2>> search.error.log";
    private static final String CHECK_HAVENASK_ALIVE_COMMAND =
        "ps aux | grep ha_sql | grep 'roleType=%s' | grep -v grep | awk '{print $2}'";
    private static final String GET_TABLE_SIZE_COMMAND = "du -sk %s | awk '{print $1}'";

    public static final Setting<Integer> HAVENASK_SEARCHER_HTTP_PORT_SETTING = Setting.intSetting(
        "node.attr.havenask.searcher.http.port",
        39200,
        Property.NodeScope,
        Property.Final
    );

    public static final Setting<Integer> HAVENASK_SEARCHER_TCP_PORT_SETTING = Setting.intSetting(
        "node.attr.havenask.searcher.tcp.port",
        39300,
        Property.NodeScope,
        Property.Final
    );
    public static final Setting<Integer> HAVENASK_SEARCHER_GRPC_PORT_SETTING = Setting.intSetting(
        "node.attr.havenask.searcher.grpc.port",
        39400,
        Property.NodeScope,
        Property.Final
    );

    public static final Setting<Integer> HAVENASK_QRS_HTTP_PORT_SETTING = Setting.intSetting(
        "node.attr.havenask.qrs.http.port",
        49200,
        Property.NodeScope,
        Property.Final
    );

    public static final Setting<Integer> HAVENASK_QRS_TCP_PORT_SETTING = Setting.intSetting(
        "node.attr.havenask.qrs.tcp.port",
        49300,
        Property.NodeScope,
        Property.Final
    );

    // add timeout setting
    public static final Setting<TimeValue> HAVENASK_COMMAND_TIMEOUT_SETTING = Setting.timeSetting(
        "node.attr.havenask.command.timeout",
        TimeValue.timeValueSeconds(60),
        Property.NodeScope,
        Property.Dynamic
    );

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final boolean enabled;
    private final boolean isDataNode;
    private final boolean isIngestNode;
    private final Environment environment;
    private final NodeEnvironment nodeEnvironment;
    private final HavenaskEngineEnvironment havenaskEngineEnvironment;
    private final int searcherHttpPort;
    private final int searcherTcpPort;
    private final int searcherGrpcPort;
    private final int qrsHttpPort;
    private final int qrsTcpPort;
    private TimeValue commandTimeout;

    protected String startSearcherCommand;
    protected String startQrsCommand;
    protected String stopHavenaskCommand;
    private ProcessControlTask processControlTask;
    private boolean running;
    private final Set<HavenaskEngine> havenaskEngines = new HashSet<>();
    private Client client;

    public NativeProcessControlService(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        HavenaskEngineEnvironment havenaskEngineEnvironment
    ) {
        this.client = client;
        this.clusterService = clusterService;
        Settings settings = clusterService.getSettings();
        isDataNode = DiscoveryNode.isDataNode(settings);
        isIngestNode = DiscoveryNode.isIngestNode(settings);
        enabled = HavenaskEnginePlugin.HAVENASK_ENGINE_ENABLED_SETTING.get(settings);
        this.threadPool = threadPool;
        this.environment = environment;
        this.nodeEnvironment = nodeEnvironment;
        this.havenaskEngineEnvironment = havenaskEngineEnvironment;
        this.searcherHttpPort = HAVENASK_SEARCHER_HTTP_PORT_SETTING.get(settings);
        this.searcherTcpPort = HAVENASK_SEARCHER_TCP_PORT_SETTING.get(settings);
        this.searcherGrpcPort = HAVENASK_SEARCHER_GRPC_PORT_SETTING.get(settings);
        this.qrsHttpPort = HAVENASK_QRS_HTTP_PORT_SETTING.get(settings);
        this.qrsTcpPort = HAVENASK_QRS_TCP_PORT_SETTING.get(settings);
        this.startSearcherCommand = String.format(
            Locale.ROOT,
            START_SEARCHER_COMMAND,
            havenaskEngineEnvironment.getDataPath().toAbsolutePath(),
            environment.binFile().toAbsolutePath(),
            havenaskEngineEnvironment.getRuntimedataPath(),
            havenaskEngineEnvironment.getConfigPath(),
            searcherHttpPort,
            searcherTcpPort,
            searcherGrpcPort
        );
        this.startQrsCommand = String.format(
            Locale.ROOT,
            START_QRS_COMMAND,
            havenaskEngineEnvironment.getDataPath().toAbsolutePath(),
            environment.binFile().toAbsolutePath(),
            havenaskEngineEnvironment.getRuntimedataPath(),
            havenaskEngineEnvironment.getConfigPath(),
            qrsHttpPort,
            qrsTcpPort
        );
        this.stopHavenaskCommand = String.format(
            Locale.ROOT,
            STOP_HAVENASK_COMMAND,
            havenaskEngineEnvironment.getDataPath().toAbsolutePath(),
            environment.binFile().toAbsolutePath()
        );
        this.commandTimeout = HAVENASK_COMMAND_TIMEOUT_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(HAVENASK_COMMAND_TIMEOUT_SETTING, this::setCommandTimeout);
    }

    @Override
    protected void doStart() {
        if (enabled && processControlTask == null) {
            // 启动searcher和qrs进程
            startProcess();

            processControlTask = new ProcessControlTask(threadPool, TimeValue.timeValueSeconds(5));
            processControlTask.rescheduleIfNecessary();
            running = true;
        }
    }

    @Override
    protected synchronized void doStop() {
        if (processControlTask != null) {
            LOGGER.info("stop process control service");
            running = false;
            processControlTask.close();
            processControlTask = null;
        }

        if (enabled && (isDataNode || isIngestNode)) {
            LOGGER.info("stop local searcher, qrs process");
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    Process process = Runtime.getRuntime().exec(new String[] { "sh", "-c", stopHavenaskCommand });
                    process.waitFor();
                    if (process.exitValue() != 0) {
                        try (InputStream inputStream = process.getInputStream()) {
                            byte[] bytes = inputStream.readAllBytes();
                            String result = new String(bytes, StandardCharsets.UTF_8);
                            LOGGER.warn("stop searcher, qrs failed, exit code: {} failed reason: {}", process.exitValue(), result);
                        }
                    } else {
                        LOGGER.info("stop searcher, qrs success");
                    }
                    process.destroy();
                } catch (Exception e) {
                    LOGGER.warn("stop local searcher,qrs failed", e);
                }
                return null;
            });
        }
    }

    @Override
    protected void doClose() {

    }

    /**
     * 进程管理任务
     */
    class ProcessControlTask extends AbstractAsyncTask {

        ProcessControlTask(ThreadPool threadPool, TimeValue interval) {
            super(LOGGER, Objects.requireNonNull(threadPool), Objects.requireNonNull(interval), true);
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        public void runInternal() {
            if (false == running) {
                return;
            }

            if (isDataNode) {
                if (false == checkProcessAlive(SEARCHER_ROLE)) {
                    havenaskEngines.forEach((havenaskEngine) -> {
                        LOGGER.warn(
                            "havenask searcher process is not alive, failed engine, shardId: {}",
                            havenaskEngine.config().getShardId()
                        );
                        EngineException e = new EngineException(
                            havenaskEngine.config().getShardId(),
                            "havenask searcher process is not alive"
                        );
                        havenaskEngine.failEngine("havenask searcher process is not alive", e);
                    });
                    LOGGER.info("start searcher process...");
                    // 启动searcher
                    boolean isRestart = runCommand(startSearcherCommand, commandTimeout);
                    if (isRestart) {
                        LOGGER.info("reroute cluster, set retryFailed to true");
                        client.admin().cluster().reroute(Requests.clusterRerouteRequest().setRetryFailed(true)).actionGet();
                    }
                }
            }

            if (isIngestNode) {
                if (false == checkProcessAlive(QRS_ROLE)) {
                    LOGGER.info("start qrs process...");
                    // 启动qrs
                    runCommand(startQrsCommand, commandTimeout);
                }
            }
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public String toString() {
            return "process_control_task";
        }
    }

    /**
     * 启动searcher\qrs进程
     */
    private void startProcess() {
        if (isDataNode) {
            LOGGER.info("start searcher process...");
            while (false == checkProcessAlive(SEARCHER_ROLE)) {
                // 启动searcher
                boolean runSearcherState = runCommand(startSearcherCommand, commandTimeout);
                if (!runSearcherState) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.warn("start searcher process failed, sleep error", e);
                    }
                }
            }
        }

        if (isIngestNode) {
            LOGGER.info("start qrs process...");
            while (false == checkProcessAlive(QRS_ROLE)) {
                // 启动qrs
                boolean runQrsState = runCommand(startQrsCommand, commandTimeout);
                if (!runQrsState) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.warn("start qrs process failed, sleep error", e);
                    }
                }
            }
        }
    }

    /**
     * 检测进程是否存活
     *
     * @param role 进程角色: searcher 或者 qrs
     * @return 返回进程存活状态
     */
    public static boolean checkProcessAlive(String role) {
        Process process = null;
        String command = String.format(Locale.ROOT, CHECK_HAVENASK_ALIVE_COMMAND, role);
        try {
            process = AccessController.doPrivileged((PrivilegedAction<Process>) () -> {
                try {
                    return Runtime.getRuntime().exec(new String[] { "sh", "-c", command });
                } catch (IOException e) {
                    LOGGER.warn(() -> new ParameterizedMessage("run check command error, command [{}]", command), e);
                    return null;
                }
            });
            if (process == null) {
                LOGGER.warn("run check command error, the process is null, don't know the process [{}] status", role);
                return true;
            }

            try (InputStream inputStream = process.getInputStream()) {
                byte[] bytes = inputStream.readAllBytes();
                String result = new String(bytes, StandardCharsets.UTF_8);
                if (result.trim().equals("")) {
                    LOGGER.info("[{}] pid not found, the process is not alive", role);
                    return false;
                }

                try {
                    if (Integer.valueOf(result.trim()) > 0) {
                        return true;
                    } else {
                        LOGGER.warn("check command get the process [{}] pid error, check result is [{}]", role, result);
                        return false;
                    }
                } catch (NumberFormatException e) {
                    LOGGER.warn("check command get the process [{}] result format error, check result is [{}]", role, result);
                    return false;
                }
            }
        } catch (IOException e) {
            LOGGER.warn(() -> new ParameterizedMessage("check command get the process [{}] input error", role), e);
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
        return true;
    }

    /**
     * @return searcher启动的http port
     */
    public int getSearcherHttpPort() {
        return searcherHttpPort;
    }

    /**
     * @return searcher启动的tcp port
     */
    public int getSearcherTcpPort() {
        return searcherTcpPort;
    }

    /**
     * @return searcher启动的grpc port
     */
    public int getSearcherGrpcPort() {
        return searcherGrpcPort;
    }

    /**
     * @return qrs启动的http port
     */
    public int getQrsHttpPort() {
        return qrsHttpPort;
    }

    /**
     * @return qrs启动的tcp port
     */
    public int getQrsTcpPort() {
        return qrsTcpPort;
    }

    public long getTableSize(Path tablePath) {
        if (isDataNode) {
            // 获取table size, 获取的size大小单位是KB
            final String finalGetTableSizeCommand = String.format(Locale.ROOT, GET_TABLE_SIZE_COMMAND, tablePath);
            String result = runCommandWithResult(finalGetTableSizeCommand);
            try {
                if (result != null && false == result.isEmpty()) {
                    // table size 单位由KB转为Byte
                    long sizeValue = Long.parseLong(result.trim()) * 1024;
                    return sizeValue;
                } else {
                    return 0;
                }
            } catch (Exception e) {
                LOGGER.info(() -> new ParameterizedMessage("get table size error, table path [{}]", tablePath), e);
                return 0;
            }
        }
        return 0;
    }

    private String runCommandWithResult(String command) {
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> {
            try {
                LOGGER.debug("run command: {}", command);
                long start = System.currentTimeMillis();
                Process process = Runtime.getRuntime().exec(new String[] { "sh", "-c", command });
                boolean timeout = process.waitFor(commandTimeout.seconds(), TimeUnit.SECONDS);
                if (false == timeout) {
                    LOGGER.warn("run command timeout, command: {}", command);
                    process.destroy();
                    return null;
                }
                if (process.exitValue() != 0) {
                    return null;
                }
                try (InputStream inputStream = process.getInputStream()) {
                    byte[] bytes = inputStream.readAllBytes();
                    // logger success
                    LOGGER.debug(
                        "run command success, cost [{}], command: [{}]",
                        TimeValue.timeValueMillis(System.currentTimeMillis() - start),
                        command
                    );
                    return new String(bytes, StandardCharsets.UTF_8);
                }
            } catch (Exception e) {
                LOGGER.warn(() -> new ParameterizedMessage("run command {} unexpected failed", command), e);
            }
            return null;
        });
    }

    public static boolean runCommand(String command, TimeValue commandTimeout) {
        return AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
            try {
                LOGGER.debug("run command: {}", command);
                long start = System.currentTimeMillis();
                Process process = Runtime.getRuntime().exec(new String[] { "sh", "-c", command });
                boolean timeout = process.waitFor(commandTimeout.seconds(), TimeUnit.SECONDS);
                if (false == timeout) {
                    LOGGER.warn("run command timeout, command: {}", command);
                    process.destroy();
                    return false;
                }
                if (process.exitValue() != 0) {
                    try (InputStream inputStream = process.getInputStream()) {
                        byte[] bytes = inputStream.readAllBytes();
                        String result = new String(bytes, StandardCharsets.UTF_8);
                        LOGGER.warn("run command {} failed, exit value: {}, failed reason: {}", command, process.exitValue(), result);
                    }
                    return false;
                } else {
                    // logger success
                    LOGGER.info(
                        "run command success, cost [{}], command: [{}]",
                        TimeValue.timeValueMillis(System.currentTimeMillis() - start),
                        command
                    );
                    return true;
                }
            } catch (Exception e) {
                LOGGER.warn(() -> new ParameterizedMessage("run command {} unexpected failed", command), e);
            }
            return false;
        });
    }

    public void setCommandTimeout(TimeValue commandTimeout) {
        this.commandTimeout = commandTimeout;
    }

    /**
     * 记录启动的engine
     * @param engine 启动的engine
     */
    public void addHavenaskEngine(HavenaskEngine engine) {
        LOGGER.debug("add havenask engine, shardId: [{}]", engine.config().getShardId());
        havenaskEngines.add(engine);
    }

    /**
     * remove关闭的engine
     * @param engine 关闭的engine
     */
    public void removeHavenaskEngine(HavenaskEngine engine) {
        LOGGER.debug("remove havenask engine, shardId: [{}]", engine.config().getShardId());
        havenaskEngines.remove(engine);
    }
}
