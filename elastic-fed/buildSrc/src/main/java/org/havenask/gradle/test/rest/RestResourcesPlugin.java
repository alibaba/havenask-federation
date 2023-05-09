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

package org.havenask.gradle.test.rest;

import org.havenask.gradle.VersionProperties;
import org.havenask.gradle.info.BuildParams;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;

import java.util.Map;

/**
 * <p>
 * Gradle plugin to help configure {@link CopyRestApiTask}'s and {@link CopyRestTestsTask} that copies the artifacts needed for the Rest API
 * spec and YAML based rest tests.
 * </p>
 * <strong>Rest API specification:</strong> <br>
 * When the {@link RestResourcesPlugin} has been applied the {@link CopyRestApiTask} will automatically copy the core Rest API specification
 * if there are any Rest YAML tests present in source, or copied from {@link CopyRestTestsTask} output.
 * It is recommended (but not required) to also explicitly declare which core specs your project depends on to help optimize the caching
 * behavior.
 * <i>For example:</i>
 * <pre>
 * restResources {
 *   restApi {
 *     includeCore 'index', 'cat'
 *   }
 * }
 * </pre>
 * <br>
 * <strong>Rest YAML tests :</strong> <br>
 * When the {@link RestResourcesPlugin} has been applied the {@link CopyRestTestsTask} will copy the Rest YAML tests if explicitly
 * configured with `includeCore` through the `restResources.restTests` extension.
 *
 * Additionally you can specify which sourceSetName resources should be copied to. The default is the yamlRestTest source set.
 * @see CopyRestApiTask
 * @see CopyRestTestsTask
 */
public class RestResourcesPlugin implements Plugin<Project> {

    private static final String EXTENSION_NAME = "restResources";

    @Override
    public void apply(Project project) {
        RestResourcesExtension extension = project.getExtensions().create(EXTENSION_NAME, RestResourcesExtension.class);

        // tests
        Configuration testConfig = project.getConfigurations().create("restTestConfig");
        project.getConfigurations().create("restTests");
        Provider<CopyRestTestsTask> copyRestYamlTestTask = project.getTasks()
            .register("copyYamlTestsTask", CopyRestTestsTask.class, task -> {
                task.includeCore.set(extension.restTests.getIncludeCore());
                task.coreConfig = testConfig;
                task.sourceSetName = SourceSet.TEST_SOURCE_SET_NAME;
                if (BuildParams.isInternal()) {
                    // core
                    Dependency restTestdependency = project.getDependencies()
                        .project(Map.of("path", ":rest-api-spec", "configuration", "restTests"));
                    project.getDependencies().add(task.coreConfig.getName(), restTestdependency);
                } else {
                    Dependency dependency = project.getDependencies()
                        .create("org.havenask:rest-api-spec:" + VersionProperties.getHavenask());
                    project.getDependencies().add(task.coreConfig.getName(), dependency);
                }
                task.dependsOn(task.coreConfig);
            });

        // api
        Configuration specConfig = project.getConfigurations().create("restSpec"); // name chosen for passivity
        project.getConfigurations().create("restSpecs");
        Provider<CopyRestApiTask> copyRestYamlSpecTask = project.getTasks()
            .register("copyRestApiSpecsTask", CopyRestApiTask.class, task -> {
                task.includeCore.set(extension.restApi.getIncludeCore());
                task.dependsOn(copyRestYamlTestTask);
                task.coreConfig = specConfig;
                task.sourceSetName = SourceSet.TEST_SOURCE_SET_NAME;
                if (BuildParams.isInternal()) {
                    Dependency restSpecDependency = project.getDependencies()
                        .project(Map.of("path", ":rest-api-spec", "configuration", "restSpecs"));
                    project.getDependencies().add(task.coreConfig.getName(), restSpecDependency);
                } else {
                    Dependency dependency = project.getDependencies()
                        .create("org.havenask:rest-api-spec:" + VersionProperties.getHavenask());
                    project.getDependencies().add(task.coreConfig.getName(), dependency);
                }
                task.dependsOn(task.coreConfig);
            });

        project.afterEvaluate(p -> {
            SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
            SourceSet testSourceSet = sourceSets.findByName(SourceSet.TEST_SOURCE_SET_NAME);
            if (testSourceSet != null) {
                project.getTasks().named(testSourceSet.getProcessResourcesTaskName()).configure(t -> t.dependsOn(copyRestYamlSpecTask));
            }
        });
    }
}
