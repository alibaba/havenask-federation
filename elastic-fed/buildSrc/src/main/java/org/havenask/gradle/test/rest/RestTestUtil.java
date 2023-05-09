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

package org.havenask.gradle.test.rest;

import org.havenask.gradle.VersionProperties;
import org.havenask.gradle.info.BuildParams;
import org.havenask.gradle.test.RestIntegTestTask;
import org.havenask.gradle.testclusters.HavenaskCluster;
import org.havenask.gradle.testclusters.TestClustersPlugin;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.bundling.Zip;

/**
 * Utility class to configure the necessary tasks and dependencies.
 */
public class RestTestUtil {

    private RestTestUtil() {}

    static HavenaskCluster createTestCluster(Project project, SourceSet sourceSet) {
        // eagerly create the testCluster container so it is easily available for configuration
        @SuppressWarnings("unchecked")
        NamedDomainObjectContainer<HavenaskCluster> testClusters = (NamedDomainObjectContainer<HavenaskCluster>) project.getExtensions()
            .getByName(TestClustersPlugin.EXTENSION_NAME);
        return testClusters.create(sourceSet.getName());
    }

    /**
     * Creates a task with the source set name of type {@link RestIntegTestTask}
     */
    static Provider<RestIntegTestTask> registerTask(Project project, SourceSet sourceSet) {
        // lazily create the test task
        Provider<RestIntegTestTask> testProvider = project.getTasks().register(sourceSet.getName(), RestIntegTestTask.class, testTask -> {
            testTask.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
            testTask.setDescription("Runs the REST tests against an external cluster");
            testTask.mustRunAfter(project.getTasks().named("test"));
            testTask.setTestClassesDirs(sourceSet.getOutput().getClassesDirs());
            testTask.setClasspath(sourceSet.getRuntimeClasspath());
            // if this a module or plugin, it may have an associated zip file with it's contents, add that to the test cluster
            project.getPluginManager().withPlugin("havenask.havenaskplugin", plugin -> {
                Zip bundle = (Zip) project.getTasks().getByName("bundlePlugin");
                testTask.dependsOn(bundle);
                if (project.getPath().contains("modules:")) {
                    testTask.getClusters().forEach(c -> c.module(bundle.getArchiveFile()));
                } else {
                    testTask.getClusters().forEach(c -> c.plugin(project.getObjects().fileProperty().value(bundle.getArchiveFile())));
                }
            });
        });

        return testProvider;
    }

    /**
     * Setup the dependencies needed for the REST tests.
     */
    static void setupDependencies(Project project, SourceSet sourceSet) {
        if (BuildParams.isInternal()) {
            project.getDependencies().add(sourceSet.getImplementationConfigurationName(), project.project(":test:framework"));
        } else {
            project.getDependencies()
                .add(sourceSet.getImplementationConfigurationName(), "org.havenask.test:framework:" + VersionProperties.getHavenask());
        }

    }

}
