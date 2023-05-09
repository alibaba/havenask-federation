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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.cluster.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.action.ActionListener;
import org.havenask.cluster.ClusterChangedEvent;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.coordination.ClusterStatePublisher.AckListener;
import org.havenask.common.UUIDs;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.common.util.concurrent.HavenaskExecutors;
import org.havenask.common.util.concurrent.PrioritizedHavenaskThreadPoolExecutor;
import org.havenask.common.util.concurrent.ThreadContext;
import org.havenask.node.Node;
import org.havenask.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.lucene.util.LuceneTestCase.random;
import static org.havenask.test.HavenaskTestCase.randomInt;

public class FakeThreadPoolMasterService extends MasterService {
    private static final Logger logger = LogManager.getLogger(FakeThreadPoolMasterService.class);

    private final String name;
    private final List<Runnable> pendingTasks = new ArrayList<>();
    private final Consumer<Runnable> onTaskAvailableToRun;
    private boolean scheduledNextTask = false;
    private boolean taskInProgress = false;
    private boolean waitForPublish = false;

    public FakeThreadPoolMasterService(String nodeName, String serviceName, ThreadPool threadPool,
                                       Consumer<Runnable> onTaskAvailableToRun) {
        super(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), nodeName).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), threadPool);
        this.name = serviceName;
        this.onTaskAvailableToRun = onTaskAvailableToRun;
    }

    @Override
    protected PrioritizedHavenaskThreadPoolExecutor createThreadPoolExecutor() {
        return new PrioritizedHavenaskThreadPoolExecutor(name, 1, 1, 1, TimeUnit.SECONDS, HavenaskExecutors.daemonThreadFactory(name),
            null, null) {

            @Override
            public void execute(Runnable command, final TimeValue timeout, final Runnable timeoutCallback) {
                execute(command);
            }

            @Override
            public void execute(Runnable command) {
                pendingTasks.add(command);
                scheduleNextTaskIfNecessary();
            }
        };
    }

    public int getFakeMasterServicePendingTaskCount() {
        return pendingTasks.size();
    }

    private void scheduleNextTaskIfNecessary() {
        if (taskInProgress == false && pendingTasks.isEmpty() == false && scheduledNextTask == false) {
            scheduledNextTask = true;
            onTaskAvailableToRun.accept(new Runnable() {
                @Override
                public String toString() {
                    return "master service scheduling next task";
                }

                @Override
                public void run() {
                    assert taskInProgress == false;
                    assert waitForPublish == false;
                    assert scheduledNextTask;
                    final int taskIndex = randomInt(pendingTasks.size() - 1);
                    logger.debug("next master service task: choosing task {} of {}", taskIndex, pendingTasks.size());
                    final Runnable task = pendingTasks.remove(taskIndex);
                    taskInProgress = true;
                    scheduledNextTask = false;
                    final ThreadContext threadContext = threadPool.getThreadContext();
                    try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                        threadContext.markAsSystemContext();
                        task.run();
                    }
                    if (waitForPublish == false) {
                        taskInProgress = false;
                    }
                    FakeThreadPoolMasterService.this.scheduleNextTaskIfNecessary();
                }
            });
        }
    }

    @Override
    public ClusterState.Builder incrementVersion(ClusterState clusterState) {
        // generate cluster UUID deterministically for repeatable tests
        return ClusterState.builder(clusterState).incrementVersion().stateUUID(UUIDs.randomBase64UUID(random()));
    }

    @Override
    protected void publish(ClusterChangedEvent clusterChangedEvent, TaskOutputs taskOutputs, long startTimeMillis) {
        assert waitForPublish == false;
        waitForPublish = true;
        final AckListener ackListener = taskOutputs.createAckListener(threadPool, clusterChangedEvent.state());
        final ActionListener<Void> publishListener = new ActionListener<Void>() {

            private boolean listenerCalled = false;

            @Override
            public void onResponse(Void aVoid) {
                assert listenerCalled == false;
                listenerCalled = true;
                assert waitForPublish;
                waitForPublish = false;
                try {
                    onPublicationSuccess(clusterChangedEvent, taskOutputs);
                } finally {
                    taskInProgress = false;
                    scheduleNextTaskIfNecessary();
                }
            }

            @Override
            public void onFailure(Exception e) {
                assert listenerCalled == false;
                listenerCalled = true;
                assert waitForPublish;
                waitForPublish = false;
                try {
                    onPublicationFailed(clusterChangedEvent, taskOutputs, startTimeMillis, e);
                } finally {
                    taskInProgress = false;
                    scheduleNextTaskIfNecessary();
                }
            }
        };
        threadPool.generic().execute(threadPool.getThreadContext().preserveContext(new Runnable() {
            @Override
            public void run() {
                clusterStatePublisher.publish(clusterChangedEvent, publishListener, wrapAckListener(ackListener));
            }

            @Override
            public String toString() {
                return "publish change of cluster state from version [" + clusterChangedEvent.previousState().version() + "] in term [" +
                        clusterChangedEvent.previousState().term() + "] to version [" + clusterChangedEvent.state().version()
                        + "] in term [" + clusterChangedEvent.state().term() + "]";
            }
        }));
    }

    protected AckListener wrapAckListener(AckListener ackListener) {
        return ackListener;
    }
}
