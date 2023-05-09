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

package org.havenask.gradle.testclusters;

import org.havenask.gradle.Architecture;
import org.havenask.gradle.DistributionDownloadPlugin;
import org.havenask.gradle.Jdk;
import org.havenask.gradle.JdkDownloadPlugin;
import org.havenask.gradle.OS;
import org.havenask.gradle.ReaperPlugin;
import org.havenask.gradle.ReaperService;
import org.havenask.gradle.info.BuildParams;
import org.havenask.gradle.info.GlobalBuildInfoPlugin;
import org.havenask.gradle.internal.InternalDistributionDownloadPlugin;
import org.havenask.gradle.util.GradleUtils;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.execution.TaskActionListener;
import org.gradle.api.execution.TaskExecutionListener;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.invocation.Gradle;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskState;

import javax.inject.Inject;
import java.io.File;

import static org.havenask.gradle.util.GradleUtils.noop;

public class TestClustersPlugin implements Plugin<Project> {

    public static final String EXTENSION_NAME = "testClusters";
    public static final String THROTTLE_SERVICE_NAME = "testClustersThrottle";

    private static final String LIST_TASK_NAME = "listTestClusters";
    private static final String REGISTRY_SERVICE_NAME = "testClustersRegistry";
    private static final String LEGACY_JAVA_VENDOR = "adoptopenjdk";
    private static final String LEGACY_JAVA_VERSION = "8u242+b08";
    private static final Logger logger = Logging.getLogger(TestClustersPlugin.class);

    @Inject
    protected FileSystemOperations getFileSystemOperations() {
        throw new UnsupportedOperationException();
    }

    @Inject
    protected ArchiveOperations getArchiveOperations() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(JdkDownloadPlugin.class);
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        if (BuildParams.isInternal()) {
            project.getPlugins().apply(InternalDistributionDownloadPlugin.class);
        } else {
            project.getPlugins().apply(DistributionDownloadPlugin.class);
        }
        project.getRootProject().getPluginManager().apply(ReaperPlugin.class);

        ReaperService reaper = project.getRootProject().getExtensions().getByType(ReaperService.class);

        // register legacy jdk distribution for testing pre-7.0 BWC clusters
        Jdk bwcJdk = JdkDownloadPlugin.getContainer(project).create("bwc_jdk", jdk -> {
            jdk.setVendor(LEGACY_JAVA_VENDOR);
            jdk.setVersion(LEGACY_JAVA_VERSION);
            jdk.setPlatform(OS.current().name().toLowerCase());
            jdk.setArchitecture(Architecture.current().name().toLowerCase());
        });

        // enable the DSL to describe clusters
        NamedDomainObjectContainer<HavenaskCluster> container = createTestClustersContainerExtension(project, reaper, bwcJdk);

        // provide a task to be able to list defined clusters.
        createListClustersTask(project, container);

        // register cluster registry as a global build service
        project.getGradle().getSharedServices().registerIfAbsent(REGISTRY_SERVICE_NAME, TestClustersRegistry.class, noop());

        // register throttle so we only run at most max-workers/2 nodes concurrently
        project.getGradle()
            .getSharedServices()
            .registerIfAbsent(
                THROTTLE_SERVICE_NAME,
                TestClustersThrottle.class,
                spec -> spec.getMaxParallelUsages().set(Math.max(1, project.getGradle().getStartParameter().getMaxWorkerCount() / 2))
            );

        // register cluster hooks
        project.getRootProject().getPluginManager().apply(TestClustersHookPlugin.class);
    }

    private NamedDomainObjectContainer<HavenaskCluster> createTestClustersContainerExtension(
        Project project,
        ReaperService reaper,
        Jdk bwcJdk
    ) {
        // Create an extensions that allows describing clusters
        NamedDomainObjectContainer<HavenaskCluster> container = project.container(
            HavenaskCluster.class,
            name -> new HavenaskCluster(
                name,
                project,
                reaper,
                new File(project.getBuildDir(), "testclusters"),
                getFileSystemOperations(),
                getArchiveOperations(),
                bwcJdk
            )
        );
        project.getExtensions().add(EXTENSION_NAME, container);
        return container;
    }

    private void createListClustersTask(Project project, NamedDomainObjectContainer<HavenaskCluster> container) {
        // Task is never up to date so we can pass an lambda for the task action
        project.getTasks().register(LIST_TASK_NAME, task -> {
            task.setGroup("Havenask cluster formation");
            task.setDescription("Lists all Havenask clusters configured for this project");
            task.doLast(
                (Task t) -> container.forEach(cluster -> logger.lifecycle("   * {}: {}", cluster.getName(), cluster.getNumberOfNodes()))
            );
        });

    }

    static class TestClustersHookPlugin implements Plugin<Project> {
        @Override
        public void apply(Project project) {
            if (project != project.getRootProject()) {
                throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
            }

            Provider<TestClustersRegistry> registryProvider = GradleUtils.getBuildService(
                project.getGradle().getSharedServices(),
                REGISTRY_SERVICE_NAME
            );
            TestClustersRegistry registry = registryProvider.get();

            // When we know what tasks will run, we claim the clusters of those task to differentiate between clusters
            // that are defined in the build script and the ones that will actually be used in this invocation of gradle
            // we use this information to determine when the last task that required the cluster executed so that we can
            // terminate the cluster right away and free up resources.
            configureClaimClustersHook(project.getGradle(), registry);

            // Before each task, we determine if a cluster needs to be started for that task.
            configureStartClustersHook(project.getGradle(), registry);

            // After each task we determine if there are clusters that are no longer needed.
            configureStopClustersHook(project.getGradle(), registry);
        }

        private static void configureClaimClustersHook(Gradle gradle, TestClustersRegistry registry) {
            // Once we know all the tasks that need to execute, we claim all the clusters that belong to those and count the
            // claims so we'll know when it's safe to stop them.
            gradle.getTaskGraph().whenReady(taskExecutionGraph -> {
                taskExecutionGraph.getAllTasks()
                    .stream()
                    .filter(task -> task instanceof TestClustersAware)
                    .map(task -> (TestClustersAware) task)
                    .flatMap(task -> task.getClusters().stream())
                    .forEach(registry::claimCluster);
            });
        }

        private static void configureStartClustersHook(Gradle gradle, TestClustersRegistry registry) {
            gradle.addListener(new TaskActionListener() {
                @Override
                public void beforeActions(Task task) {
                    if (task instanceof TestClustersAware == false) {
                        return;
                    }
                    // we only start the cluster before the actions, so we'll not start it if the task is up-to-date
                    TestClustersAware awareTask = (TestClustersAware) task;
                    awareTask.beforeStart();
                    awareTask.getClusters().forEach(registry::maybeStartCluster);
                }

                @Override
                public void afterActions(Task task) {}
            });
        }

        private static void configureStopClustersHook(Gradle gradle, TestClustersRegistry registry) {
            gradle.addListener(new TaskExecutionListener() {
                @Override
                public void afterExecute(Task task, TaskState state) {
                    if (task instanceof TestClustersAware == false) {
                        return;
                    }
                    // always unclaim the cluster, even if _this_ task is up-to-date, as others might not have been
                    // and caused the cluster to start.
                    ((TestClustersAware) task).getClusters().forEach(cluster -> registry.stopCluster(cluster, state.getFailure() != null));
                }

                @Override
                public void beforeExecute(Task task) {}
            });
        }
    }
}
