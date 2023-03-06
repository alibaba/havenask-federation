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
import java.util.Objects;
import java.util.Set;

import com.sun.jna.Platform;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.cluster.node.DiscoveryNodeRole;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.component.AbstractLifecycleComponent;
import org.havenask.common.component.Lifecycle.State;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.concurrent.AbstractAsyncTask;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.threadpool.ThreadPool;

import static org.havenask.cluster.node.DiscoveryNodeRole.INGEST_ROLE;

public class NativeProcessControlService extends AbstractLifecycleComponent {
    private static final Logger LOGGER = LogManager.getLogger(NativeProcessControlService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private boolean isDataNode;
    private boolean isIngestNode;
    private final Environment environment;
    private final NodeEnvironment nodeEnvironment;
    private final String SEARCHER_COMMAND = "/bin/ls";
    private final String QRS_COMMAND = "/bin/pwd";
    private Process searcherProcess;
    private Process qrsProcess;
    private ProcessControlTask processControlTask;
    private boolean running;

    public NativeProcessControlService(ClusterService clusterService, ThreadPool threadPool, Environment environment,
        NodeEnvironment nodeEnvironment) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.environment = environment;
        this.nodeEnvironment = nodeEnvironment;
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

        if (searcherProcess != null) {
            searcherProcess.destroy();
            searcherProcess = null;
        }
        if (qrsProcess != null) {
            qrsProcess.destroy();
            qrsProcess = null;
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

            Set<DiscoveryNodeRole> nodeRoles = clusterService.localNode().getRoles();
            nodeRoles.forEach(discoveryNodeRole -> {
                if (discoveryNodeRole.canContainData()) {
                    isDataNode = true;
                } else if (discoveryNodeRole == INGEST_ROLE) {
                    isIngestNode = true;
                }
            });

            if (isDataNode) {
                if (searcherProcess == null || false == checkPid(searcherProcess.pid())) {
                    LOGGER.info("current searcher process[{}] is not started, start searcher process", searcherProcess);
                    // 启动searcher
                    final ProcessBuilder pb = new ProcessBuilder(SEARCHER_COMMAND);
                    searcherProcess = AccessController.doPrivileged((PrivilegedAction<Process>) () -> {
                        try {
                            return pb.start();
                        } catch (IOException e) {
                            e.printStackTrace();
                            return null;
                        }
                    });
                }
            }

            if (isIngestNode) {
                if (qrsProcess == null || false == checkPid(qrsProcess.pid())) {
                    LOGGER.info("current qrs process[{}] is not started, start qrs process", qrsProcess);
                    // 启动qrs
                    final ProcessBuilder pb = new ProcessBuilder(QRS_COMMAND);
                    qrsProcess = AccessController.doPrivileged((PrivilegedAction<Process>) () -> {
                        try {
                            return pb.start();
                        } catch (IOException e) {
                            e.printStackTrace();
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

    static boolean checkPid(long processId) {
        boolean flag = true;
        Process process = null;
        String command;
        try {
            if (Platform.isWindows()) {
                command ="cmd /c tasklist  /FI \"PID eq " + processId + "\"";
            } else {
                command = "ps aux | awk '{print $2}'| grep -w " + processId;
            }
            String finalCommand = command;
            process = AccessController.doPrivileged((PrivilegedAction<Process>) () -> {
                try {
                    return Runtime.getRuntime().exec(new String[]{"sh", "-c", finalCommand});
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
                return result.contains(String.valueOf(processId));
            }
        } catch (IOException e) {
            // pass
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
        return flag;
    }
}
