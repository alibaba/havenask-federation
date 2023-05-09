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

package org.havenask.gradle.precommit;

import org.havenask.gradle.util.GradleUtils;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;

/**
 * Base plugin for adding a precommit task.
 */
public abstract class PrecommitPlugin implements Plugin<Project> {

    public static final String PRECOMMIT_TASK_NAME = "precommit";

    @Override
    public final void apply(Project project) {
        project.getPluginManager().apply(PrecommitTaskPlugin.class);
        TaskProvider<? extends Task> task = createTask(project);
        TaskProvider<Task> precommit = project.getTasks().named(PRECOMMIT_TASK_NAME);
        precommit.configure(t -> t.dependsOn(task));

        project.getPluginManager()
            .withPlugin(
                "java",
                p -> {
                    // We want to get any compilation error before running the pre-commit checks.
                    for (SourceSet sourceSet : GradleUtils.getJavaSourceSets(project)) {
                        task.configure(t -> t.shouldRunAfter(sourceSet.getClassesTaskName()));
                    }
                }
            );
    }

    public abstract TaskProvider<? extends Task> createTask(Project project);

}
