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

package org.havenask.persistent.decider;

import org.havenask.action.ActionListener;
import org.havenask.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.havenask.action.admin.cluster.state.ClusterStateResponse;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.settings.Settings;
import org.havenask.persistent.PersistentTasksCustomMetadata;
import org.havenask.persistent.PersistentTasksService;
import org.havenask.persistent.TestPersistentTasksPlugin;
import org.havenask.persistent.TestPersistentTasksPlugin.TestParams;
import org.havenask.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static java.util.Collections.singletonList;
import static org.havenask.persistent.decider.EnableAssignmentDecider.Allocation;
import static org.havenask.persistent.decider.EnableAssignmentDecider.CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@HavenaskIntegTestCase.ClusterScope(minNumDataNodes = 1)
public class EnableAssignmentDeciderIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return singletonList(TestPersistentTasksPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    /**
     * Test that the {@link EnableAssignmentDecider#CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING} setting correctly
     * prevents persistent tasks to be assigned after a cluster restart.
     */
    public void testEnableAssignmentAfterRestart() throws Exception {
        final int numberOfTasks = randomIntBetween(1, 10);
        logger.trace("creating {} persistent tasks", numberOfTasks);

        final CountDownLatch latch = new CountDownLatch(numberOfTasks);
        for (int i = 0; i < numberOfTasks; i++) {
            PersistentTasksService service = internalCluster().getInstance(PersistentTasksService.class);
            service.sendStartRequest("task_" + i, TestPersistentTasksExecutor.NAME, new TestParams(randomAlphaOfLength(10)),
                ActionListener.wrap(latch::countDown));
        }
        latch.await();

        ClusterService clusterService = internalCluster().clusterService(internalCluster().getMasterName());
        PersistentTasksCustomMetadata tasks = clusterService.state().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        assertEquals(numberOfTasks, tasks.tasks().stream().filter(t -> TestPersistentTasksExecutor.NAME.equals(t.getTaskName())).count());

        logger.trace("waiting for the tasks to be running");
        assertBusy(() -> {
            ListTasksResponse listTasks = client().admin().cluster().prepareListTasks()
                                                                    .setActions(TestPersistentTasksExecutor.NAME + "[c]")
                                                                    .get();
            assertThat(listTasks.getTasks().size(), equalTo(numberOfTasks));
        });

        try {
            logger.trace("disable persistent tasks assignment");
            disablePersistentTasksAssignment();

            logger.trace("restart the cluster");
            internalCluster().fullRestart();
            ensureYellow();

            logger.trace("persistent tasks assignment is still disabled");
            assertEnableAssignmentSetting(Allocation.NONE);

            logger.trace("persistent tasks are not assigned");
            tasks = internalCluster().clusterService().state().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
            assertEquals(numberOfTasks, tasks.tasks().stream()
                .filter(t -> TestPersistentTasksExecutor.NAME.equals(t.getTaskName()))
                .filter(t -> t.isAssigned() == false)
                .count());

            ListTasksResponse runningTasks = client().admin().cluster().prepareListTasks()
                .setActions(TestPersistentTasksExecutor.NAME + "[c]")
                .get();
            assertThat(runningTasks.getTasks().size(), equalTo(0));

            logger.trace("enable persistent tasks assignment");
            if (randomBoolean()) {
                enablePersistentTasksAssignment();
            } else {
                resetPersistentTasksAssignment();
            }

            assertBusy(() -> {
                ListTasksResponse listTasks = client().admin().cluster().prepareListTasks()
                    .setActions(TestPersistentTasksExecutor.NAME + "[c]")
                    .get();
                assertThat(listTasks.getTasks().size(), equalTo(numberOfTasks));
            });

        } finally {
            resetPersistentTasksAssignment();
        }
    }

    private void assertEnableAssignmentSetting(final Allocation expected) {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().setMetadata(true).get();
        Settings settings = clusterStateResponse.getState().getMetadata().settings();

        String value = settings.get(CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey());
        assertThat(Allocation.fromString(value), equalTo(expected));
    }

    private void disablePersistentTasksAssignment() {
        Settings.Builder settings = Settings.builder().put(CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.NONE);
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
    }

    private void enablePersistentTasksAssignment() {
        Settings.Builder settings = Settings.builder().put(CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey(), Allocation.ALL);
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
    }

    private void resetPersistentTasksAssignment() {
        Settings.Builder settings = Settings.builder().putNull(CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey());
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings));
    }

}
