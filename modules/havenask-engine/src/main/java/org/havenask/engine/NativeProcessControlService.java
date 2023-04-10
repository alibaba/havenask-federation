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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.component.AbstractLifecycleComponent;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Setting.Property;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.concurrent.AbstractAsyncTask;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.threadpool.ThreadPool;

public class NativeProcessControlService extends AbstractLifecycleComponent {
    private static final Logger LOGGER = LogManager.getLogger(NativeProcessControlService.class);
    public static final String SEARCHER_ROLE = "searcher";
    public static final String QRS_ROLE = "qrs";
    private static final String START_SEARCHER_COMMAND = "cd %s;python %s/havenask/script/general_search_starter.py -i "
        + "%s -c %s -b /ha3_install -M in0 --role searcher --httpBindPort %d";
    private static final String START_QRS_COMMAND = "cd %s;python %s/havenask/script/general_search_starter.py -i "
        + "%s -c %s -b /ha3_install -M in0 --role qrs --httpBindPort %d";
    private static final String UPDATE_SEARCHER_COMMAND = "cd %s;python %s/havenask/script/general_search_updater.py -i "
        + "%s -c %s -M in0 --role searcher";
    private static final String UPDATE_QRS_COMMAND = "cd %s;python %s/havenask/script/general_search_updater.py -i "
        + "%s -c %s -M in0 --role qrs";
    private static final String STOP_HAVENASK_COMMAND =
        "python /ha3_install/usr/local/lib/python/site-packages/ha_tools/local_search_stop.py"
            + " -c /ha3_install/usr/local/etc/ha3/ha3_alog.conf";
    private static final String CHECK_HAVENASK_ALIVE_COMMAND =
        "ps aux | grep sap_server_d | grep 'roleType=%s' | grep -v grep | awk '{print $2}'";
    private static final String START_BS_JOB_COMMAND = "python %s/havenask/script/bs_job_starter.py %s %s %s %s ";

    public static final Setting<Integer> HAVENASK_SEARCHER_HTTP_PORT_SETTING = Setting.intSetting(
        "havenask.searcher.http.port",
        39200,
        Property.NodeScope,
        Setting.Property.Final
    );

    public static final Setting<Integer> HAVENASK_QRS_HTTP_PORT_SETTING = Setting.intSetting(
        "havenask.qrs.http.port",
        49200,
        Property.NodeScope,
        Setting.Property.Final
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
    private final int qrsHttpPort;

    protected String startSearcherCommand;
    protected String updateSearcherCommand;
    protected String startQrsCommand;
    protected String updateQrsCommand;
    protected String stopHavenaskCommand;
    protected String startBsJobCommand;
    private ProcessControlTask processControlTask;
    private boolean running;

    public NativeProcessControlService(
        ClusterService clusterService,
        ThreadPool threadPool,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        HavenaskEngineEnvironment havenaskEngineEnvironment
    ) {
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
        this.qrsHttpPort = HAVENASK_QRS_HTTP_PORT_SETTING.get(settings);
        this.startSearcherCommand = String.format(
            Locale.ROOT,
            START_SEARCHER_COMMAND,
            havenaskEngineEnvironment.getDataPath().toAbsolutePath(),
            environment.configFile().toAbsolutePath(),
            havenaskEngineEnvironment.getRuntimedataPath(),
            havenaskEngineEnvironment.getConfigPath(),
            searcherHttpPort
        );
        this.updateSearcherCommand = String.format(
            Locale.ROOT,
            UPDATE_SEARCHER_COMMAND,
            havenaskEngineEnvironment.getDataPath().toAbsolutePath(),
            environment.configFile().toAbsolutePath(),
            havenaskEngineEnvironment.getRuntimedataPath(),
            havenaskEngineEnvironment.getConfigPath()
        );
        this.startQrsCommand = String.format(
            Locale.ROOT,
            START_QRS_COMMAND,
            havenaskEngineEnvironment.getDataPath().toAbsolutePath(),
            environment.configFile().toAbsolutePath(),
            havenaskEngineEnvironment.getRuntimedataPath(),
            havenaskEngineEnvironment.getConfigPath(),
            qrsHttpPort
        );
        this.updateQrsCommand = String.format(
            Locale.ROOT,
            UPDATE_QRS_COMMAND,
            havenaskEngineEnvironment.getDataPath().toAbsolutePath(),
            environment.configFile().toAbsolutePath(),
            havenaskEngineEnvironment.getRuntimedataPath(),
            havenaskEngineEnvironment.getConfigPath()
        );
        this.stopHavenaskCommand = STOP_HAVENASK_COMMAND;
        this.startBsJobCommand = String.format(
            Locale.ROOT,
            START_BS_JOB_COMMAND,
            environment.configFile().toAbsolutePath(),
            havenaskEngineEnvironment.getConfigPath().toAbsolutePath(),
            havenaskEngineEnvironment.getDataPath().toAbsolutePath(),
            havenaskEngineEnvironment.getBsWorkPath().toAbsolutePath(),
            havenaskEngineEnvironment.getRuntimedataPath().toAbsolutePath()
        );
    }

    @Override
    protected void doStart() {
        if (enabled && processControlTask == null) {
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
            LOGGER.info("stop local searcher,qrs process");
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    Process process = Runtime.getRuntime().exec(new String[] { "sh", "-c", stopHavenaskCommand });
                    process.waitFor();
                    if (process.exitValue() != 0) {
                        try (InputStream inputStream = process.getInputStream()) {
                            byte[] bytes = inputStream.readAllBytes();
                            String result = new String(bytes, StandardCharsets.UTF_8);
                            LOGGER.warn("stop searcher\\qrs failed, failed reason: {}", result);
                        }
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
                    LOGGER.info("current searcher process is not started, start searcher process");
                    // 启动searcher
                    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                        try {
                            Process process = Runtime.getRuntime().exec(new String[] { "sh", "-c", startSearcherCommand });
                            process.waitFor();
                            if (process.exitValue() != 0) {
                                try (InputStream inputStream = process.getInputStream()) {
                                    byte[] bytes = inputStream.readAllBytes();
                                    String result = new String(bytes, StandardCharsets.UTF_8);
                                    LOGGER.warn("searcher start failed, exit value: {}, failed reason: {}", process.exitValue(), result);
                                }
                            }
                            process.destroy();
                        } catch (Exception e) {
                            LOGGER.warn("start searcher process failed", e);
                        }
                        return null;
                    });
                }
            }

            if (isIngestNode) {
                if (false == checkProcessAlive(QRS_ROLE)) {
                    LOGGER.info("current qrs process is not started, start qrs process");
                    // 启动qrs
                    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                        try {
                            Process process = Runtime.getRuntime().exec(new String[] { "sh", "-c", startQrsCommand });
                            process.waitFor();
                            if (process.exitValue() != 0) {
                                try (InputStream inputStream = process.getInputStream()) {
                                    byte[] bytes = inputStream.readAllBytes();
                                    String result = new String(bytes, StandardCharsets.UTF_8);
                                    LOGGER.warn("qrs start failed, failed reason: {}", result);
                                }
                            }
                            process.destroy();
                        } catch (Exception e) {
                            LOGGER.warn("start qrs process failed", e);
                        }
                        return null;
                    });
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
     * 检测进程是否存活
     *
     * @param role 进程角色: searcher 或者 qrs
     * @return 返回进程存活状态
     */
    boolean checkProcessAlive(String role) {
        Process process = null;
        String command = String.format(Locale.ROOT, CHECK_HAVENASK_ALIVE_COMMAND, role);
        try {
            process = AccessController.doPrivileged((PrivilegedAction<Process>) () -> {
                try {
                    return Runtime.getRuntime().exec(new String[] { "sh", "-c", command });
                } catch (IOException e) {
                    LOGGER.warn(() -> new ParameterizedMessage("run check script error, command [{}]", command), e);
                    return null;
                }
            });
            if (process == null) {
                LOGGER.warn("run check script error, the process is null, don't know the process [{}] status", role);
                return true;
            }

            try (InputStream inputStream = process.getInputStream()) {
                byte[] bytes = inputStream.readAllBytes();
                String result = new String(bytes, StandardCharsets.UTF_8);
                if (result.trim().equals("")) {
                    LOGGER.info("check script don't get the process [{}] pid, the process is not alive", role);
                    return false;
                }

                try {
                    if (Integer.valueOf(result.trim()) > 0) {
                        return true;
                    } else {
                        LOGGER.warn("check script get the process [{}] pid error, check result is [{}]", role, result);
                        return false;
                    }
                } catch (NumberFormatException e) {
                    LOGGER.warn("check script get the process [{}] result format error, check result is [{}]", role, result);
                    return false;
                }
            }
        } catch (IOException e) {
            LOGGER.warn(() -> new ParameterizedMessage("check script get the process [{}] input error", role), e);
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
     * @return qrs启动的http port
     */
    public int getQrsHttpPort() {
        return qrsHttpPort;
    }

    public void updateDataNodeTarget() {
        if (isDataNode) {
            // 更新datanode searcher的target
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    Process process = Runtime.getRuntime().exec(new String[] { "sh", "-c", updateSearcherCommand });
                    process.waitFor();
                    if (process.exitValue() != 0) {
                        try (InputStream inputStream = process.getInputStream()) {
                            byte[] bytes = inputStream.readAllBytes();
                            String result = new String(bytes, StandardCharsets.UTF_8);
                            LOGGER.warn("searcher update target failed, exit value: {}, failed reason: {}", process.exitValue(), result);
                        }
                    }
                    process.destroy();
                } catch (Exception e) {
                    LOGGER.warn("searcher update target unexpected failed", e);
                }
                return null;
            });
        }
    }

    public void startBsJob(String indexName) {
        if (isDataNode) {
            // 启动bs job
            final String finalStartBsJobCommand = startBsJobCommand + " " + indexName;
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    Process process = Runtime.getRuntime().exec(new String[] { "sh", "-c", finalStartBsJobCommand });
                    process.waitFor();
                    if (process.exitValue() != 0) {
                        try (InputStream inputStream = process.getInputStream()) {
                            byte[] bytes = inputStream.readAllBytes();
                            String result = new String(bytes, StandardCharsets.UTF_8);
                            LOGGER.warn("bs job start failed, exit value: {}, failed reason: {}", process.exitValue(), result);
                        }
                    }
                    process.destroy();
                } catch (Exception e) {
                    LOGGER.warn("start bs job unexpected failed", e);
                }
                return null;
            });
        }
    }

                    
    public synchronized void updateIngestNodeTarget() {
        if (isIngestNode) {
            // 更新ingestnode qrs的target
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    Process process = Runtime.getRuntime().exec(new String[] { "sh", "-c", updateQrsCommand });
                    process.waitFor();
                    if (process.exitValue() != 0) {
                        try (InputStream inputStream = process.getInputStream()) {
                            byte[] bytes = inputStream.readAllBytes();
                            String result = new String(bytes, StandardCharsets.UTF_8);
                            LOGGER.warn("qrs update target failed, failed reason: {}", result);
                        }
                    }
                    process.destroy();
                } catch (Exception e) {
                    LOGGER.warn("qrs update target unexpected failed", e);
                }
                return null;
            });
        }
    }
}
