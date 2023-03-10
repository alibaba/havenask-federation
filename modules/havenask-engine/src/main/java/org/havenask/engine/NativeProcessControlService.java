/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Havenask Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.havenask.engine;

import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.component.AbstractLifecycleComponent;
import org.havenask.common.component.Lifecycle.State;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.concurrent.AbstractAsyncTask;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.threadpool.ThreadPool;

public class NativeProcessControlService extends AbstractLifecycleComponent {
    private static final Logger LOGGER = LogManager.getLogger(NativeProcessControlService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final boolean isDataNode;
    private final boolean isIngestNode;
    private final Environment environment;
    private final NodeEnvironment nodeEnvironment;
    private final HavenaskEngineEnvironment havenaskEngineEnvironment;
    final String SEARCHER_ROLE = "searcher";
    final String QRS_ROLE = "qrs";
    private final String START_SEARCHER_COMMAND = "python /ha3_install/usr/local/lib/python/site-packages/ha_tools/local_search_starter.py -i %s/runtimedata/ -c %s/config -p 30468,30480 -b /ha3_install --qrsHttpArpcBindPort 45800";
    private final String STOP_HAVENASK_COMMAND = "python /ha3_install/usr/local/lib/python/site-packages/ha_tools/local_search_stop.py -c %s/config";
    private final String CHECK_HAVENASK_ALIVE_COMMAND = "ps aux | grep sap_server_d | grep 'roleType=%s' | grep -v grep | awk '{print $2}'";
    protected String startSearcherCommand;
    protected String stopHavenaskCommand;
    private ProcessControlTask processControlTask;
    private boolean running;

    public NativeProcessControlService(ClusterService clusterService, ThreadPool threadPool, Environment environment,
        NodeEnvironment nodeEnvironment, HavenaskEngineEnvironment havenaskEngineEnvironment) {
        this.clusterService = clusterService;
        Settings settings = clusterService.getSettings();
        isDataNode = DiscoveryNode.isDataNode(settings);
        isIngestNode = DiscoveryNode.isIngestNode(settings);
        this.threadPool = threadPool;
        this.environment = environment;
        this.nodeEnvironment = nodeEnvironment;
        this.havenaskEngineEnvironment = havenaskEngineEnvironment;
        this.startSearcherCommand = String.format(Locale.ROOT,
            START_SEARCHER_COMMAND, havenaskEngineEnvironment.getDataPath().toAbsolutePath(), havenaskEngineEnvironment.getDataPath().toAbsolutePath());
        this.stopHavenaskCommand = String.format(Locale.ROOT, STOP_HAVENASK_COMMAND, havenaskEngineEnvironment.getDataPath().toAbsolutePath());
    }

    @Override
    protected void doStart() {
        if (processControlTask == null) {
            processControlTask = new ProcessControlTask(threadPool, TimeValue.timeValueSeconds(10));
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
        }

        if (isDataNode || isIngestNode) {
            AccessController.doPrivileged((PrivilegedAction<Process>) () -> {
                try {
                    return Runtime.getRuntime().exec(new String[]{"sh", "-c", stopHavenaskCommand});
                } catch (IOException e) {
                    // TODO logger error
                    return null;
                }
            });
        }
    }

    @Override
    protected void doClose() {

    }

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

            if (clusterService.getClusterApplierService().lifecycleState() != State.STARTED) {
                LOGGER.warn("cluster service not ready, state={}", clusterService.getClusterApplierService().lifecycleState());
                return;
            }

            if (isDataNode) {
                if (checkProcessAlive(SEARCHER_ROLE)) {
                    LOGGER.info("current searcher process is not started, start searcher process");
                    // 启动searcher
                    AccessController.doPrivileged((PrivilegedAction<Process>) () -> {
                        try {
                            return Runtime.getRuntime().exec(new String[]{"sh", "-c", startSearcherCommand});
                        } catch (IOException e) {
                            // TODO log
                            return null;
                        }
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

    boolean checkProcessAlive(String role) {
        boolean flag = true;
        Process process = null;
        String command = String.format(Locale.ROOT, CHECK_HAVENASK_ALIVE_COMMAND, role);
        try {
            process = AccessController.doPrivileged((PrivilegedAction<Process>) () -> {
                try {
                    return Runtime.getRuntime().exec(new String[]{"sh", "-c", command});
                } catch (IOException e) {
                    return null;
                }
            });
            if (process == null) {
                // TODO can't get process
                return true;
            }

            try (InputStream inputStream = process.getInputStream()) {
                byte[] bytes = inputStream.readAllBytes();
                String result = new String(bytes);
                if (result.trim().equals("")) {
                    return false;
                }

                try {
                    if (Integer.valueOf(result.trim()) > 0) {
                        return true;
                    } else {
                        // TODO log
                        return false;
                    }
                } catch (NumberFormatException e) {
                    // TODO log
                    return false;
                }
            }
        } catch (IOException e) {
            // TODO log
            // pass
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
        return flag;
    }
}
