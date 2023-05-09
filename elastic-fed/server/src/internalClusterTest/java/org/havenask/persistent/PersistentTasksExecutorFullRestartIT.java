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

package org.havenask.persistent;

import org.havenask.action.support.PlainActionFuture;
import org.havenask.common.UUIDs;
import org.havenask.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.havenask.persistent.TestPersistentTasksPlugin.TestParams;
import org.havenask.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST, minNumDataNodes = 1)
public class PersistentTasksExecutorFullRestartIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPersistentTasksPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    protected boolean ignoreExternalCluster() {
        return true;
    }

    public void testFullClusterRestart() throws Exception {
        PersistentTasksService service = internalCluster().getInstance(PersistentTasksService.class);
        int numberOfTasks = randomIntBetween(1, 10);
        String[] taskIds = new String[numberOfTasks];
        List<PlainActionFuture<PersistentTask<TestParams>>> futures = new ArrayList<>(numberOfTasks);

        for (int i = 0; i < numberOfTasks; i++) {
            PlainActionFuture<PersistentTask<TestParams>> future = new PlainActionFuture<>();
            futures.add(future);
            taskIds[i] = UUIDs.base64UUID();
            service.sendStartRequest(taskIds[i], TestPersistentTasksExecutor.NAME, new TestParams("Blah"), future);
        }

        for (int i = 0; i < numberOfTasks; i++) {
            assertThat(futures.get(i).get().getId(), equalTo(taskIds[i]));
        }

        PersistentTasksCustomMetadata tasksInProgress = internalCluster().clusterService().state().getMetadata()
                .custom(PersistentTasksCustomMetadata.TYPE);
        assertThat(tasksInProgress.tasks().size(), equalTo(numberOfTasks));

        // Make sure that at least one of the tasks is running
        assertBusy(() -> {
            // Wait for the task to start
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get()
                    .getTasks().size(), greaterThan(0));
        });

        // Restart cluster
        internalCluster().fullRestart();
        ensureYellow();

        tasksInProgress = internalCluster().clusterService().state().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        assertThat(tasksInProgress.tasks().size(), equalTo(numberOfTasks));
        // Check that cluster state is correct
        for (int i = 0; i < numberOfTasks; i++) {
            PersistentTask<?> task = tasksInProgress.getTask(taskIds[i]);
            assertNotNull(task);
        }

        logger.info("Waiting for {} tasks to start", numberOfTasks);
        assertBusy(() -> {
            // Wait for all tasks to start
            assertThat(client().admin().cluster().prepareListTasks().setActions(TestPersistentTasksExecutor.NAME + "[c]").get()
                            .getTasks().size(), equalTo(numberOfTasks));
        });

        logger.info("Complete all tasks");
        // Complete the running task and make sure it finishes properly
        assertThat(new TestPersistentTasksPlugin.TestTasksRequestBuilder(client()).setOperation("finish").get().getTasks().size(),
                equalTo(numberOfTasks));

        assertBusy(() -> {
            // Make sure the task is removed from the cluster state
            assertThat(((PersistentTasksCustomMetadata) internalCluster().clusterService().state().getMetadata()
                    .custom(PersistentTasksCustomMetadata.TYPE)).tasks(), empty());
        });

    }
}
