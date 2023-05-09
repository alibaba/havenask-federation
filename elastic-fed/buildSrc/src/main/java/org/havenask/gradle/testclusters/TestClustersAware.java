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

import org.havenask.gradle.Jdk;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.Nested;

import java.util.Collection;
import java.util.concurrent.Callable;

public interface TestClustersAware extends Task {

    @Nested
    Collection<HavenaskCluster> getClusters();

    default void useCluster(HavenaskCluster cluster) {
        if (cluster.getPath().equals(getProject().getPath()) == false) {
            throw new TestClustersException("Task " + getPath() + " can't use test cluster from" + " another project " + cluster);
        }

        // Add configured distributions as task dependencies so they are built before starting the cluster
        cluster.getNodes().stream().flatMap(node -> node.getDistributions().stream()).forEach(distro -> dependsOn(distro.getExtracted()));

        // Add legacy BWC JDK runtime as a dependency so it's downloaded before starting the cluster if necessary
        cluster.getNodes().stream().map(node -> (Callable<Jdk>) node::getBwcJdk).forEach(this::dependsOn);

        cluster.getNodes().forEach(node -> dependsOn((Callable<Collection<Configuration>>) node::getPluginAndModuleConfigurations));
        getClusters().add(cluster);
    }

    default void beforeStart() {}

}
