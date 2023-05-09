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

import org.havenask.gradle.FileSystemOperationsAware;
import org.havenask.gradle.test.Fixture;
import org.havenask.gradle.util.GradleUtils;
import org.gradle.api.Task;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.internal.BuildServiceRegistryInternal;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.WorkResult;
import org.gradle.api.tasks.testing.Test;
import org.gradle.internal.resources.ResourceLock;
import org.gradle.internal.resources.SharedResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Customized version of Gradle {@link Test} task which tracks a collection of {@link HavenaskCluster} as a task input. We must do this
 * as a custom task type because the current {@link org.gradle.api.tasks.TaskInputs} runtime API does not have a way to register
 * {@link Nested} inputs.
 */
@CacheableTask
public class StandaloneRestIntegTestTask extends Test implements TestClustersAware, FileSystemOperationsAware {

    private Collection<HavenaskCluster> clusters = new HashSet<>();

    public StandaloneRestIntegTestTask() {
        this.getOutputs()
            .doNotCacheIf(
                "Caching disabled for this task since it uses a cluster shared by other tasks",
                /*
                 * Look for any other tasks which use the same cluster as this task. Since tests often have side effects for the cluster
                 * they execute against, this state can cause issues when trying to cache tests results of tasks that share a cluster. To
                 * avoid any undesired behavior we simply disable the cache if we detect that this task uses a cluster shared between
                 * multiple tasks.
                 */
                t -> getProject().getTasks()
                    .withType(StandaloneRestIntegTestTask.class)
                    .stream()
                    .filter(task -> task != this)
                    .anyMatch(task -> Collections.disjoint(task.getClusters(), getClusters()) == false)
            );

        this.getOutputs()
            .doNotCacheIf(
                "Caching disabled for this task since it is configured to preserve data directory",
                // Don't cache the output of this task if it's not running from a clean data directory.
                t -> getClusters().stream().anyMatch(cluster -> cluster.isPreserveDataDir())
            );
    }

    @Override
    public int getMaxParallelForks() {
        return 1;
    }

    @Nested
    @Override
    public Collection<HavenaskCluster> getClusters() {
        return clusters;
    }

    @Override
    @Internal
    public List<ResourceLock> getSharedResources() {
        List<ResourceLock> locks = new ArrayList<>(super.getSharedResources());
        BuildServiceRegistryInternal serviceRegistry = getServices().get(BuildServiceRegistryInternal.class);
        Provider<TestClustersThrottle> throttleProvider = GradleUtils.getBuildService(
            serviceRegistry,
            TestClustersPlugin.THROTTLE_SERVICE_NAME
        );
        SharedResource resource = serviceRegistry.forService(throttleProvider);

        int nodeCount = clusters.stream().mapToInt(cluster -> cluster.getNodes().size()).sum();
        if (nodeCount > 0) {
            locks.add(resource.getResourceLock(Math.min(nodeCount, resource.getMaxUsages())));
        }

        return Collections.unmodifiableList(locks);
    }

    @Override
    public Task dependsOn(Object... dependencies) {
        super.dependsOn(dependencies);
        for (Object dependency : dependencies) {
            if (dependency instanceof Fixture) {
                finalizedBy(((Fixture) dependency).getStopTask());
            }
        }
        return this;
    }

    @Override
    public void setDependsOn(Iterable<?> dependencies) {
        super.setDependsOn(dependencies);
        for (Object dependency : dependencies) {
            if (dependency instanceof Fixture) {
                finalizedBy(((Fixture) dependency).getStopTask());
            }
        }
    }

    public WorkResult delete(Object... objects) {
        return getFileSystemOperations().delete(d -> d.delete(objects));
    }
}
