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

package org.havenask.gradle.internal;

import org.apache.commons.io.FileUtils;
import org.apache.tools.ant.taskdefs.condition.Os;
import org.havenask.gradle.BwcVersions;
import org.havenask.gradle.LoggedExec;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.logging.LogLevel;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.havenask.gradle.util.JavaUtil.getJavaHome;

/**
 * By registering bwc tasks via this extension we can support declaring custom bwc tasks from the build script
 * without relying on groovy closures and sharing common logic for tasks created by the BwcSetup plugin already.
 * */
public class BwcSetupExtension {

    private final Project project;

    private final Provider<BwcVersions.UnreleasedVersionInfo> unreleasedVersionInfo;
    private Provider<File> checkoutDir;

    public BwcSetupExtension(
        Project project,
        Provider<BwcVersions.UnreleasedVersionInfo> unreleasedVersionInfo,
        Provider<File> checkoutDir
    ) {
        this.project = project;
        this.unreleasedVersionInfo = unreleasedVersionInfo;
        this.checkoutDir = checkoutDir;
    }

    TaskProvider<LoggedExec> bwcTask(String name, Action<LoggedExec> configuration) {
        return createRunBwcGradleTask(project, name, configuration);
    }

    private TaskProvider<LoggedExec> createRunBwcGradleTask(Project project, String name, Action<LoggedExec> configAction) {
        return project.getTasks().register(name, LoggedExec.class, loggedExec -> {
            // TODO revisit
            loggedExec.dependsOn("checkoutBwcBranch");
            loggedExec.setSpoolOutput(true);
            loggedExec.setWorkingDir(checkoutDir.get());
            loggedExec.doFirst(t -> {
                // Execution time so that the checkouts are available
                String javaVersionsString = readFromFile(new File(checkoutDir.get(), ".ci/java-versions.properties"));
                loggedExec.environment(
                    "JAVA_HOME",
                    getJavaHome(
                        Integer.parseInt(
                            Arrays.asList(javaVersionsString.split("\n"))
                                .stream()
                                .filter(l -> l.trim().startsWith("HAVENASK_BUILD_JAVA="))
                                .map(l -> l.replace("HAVENASK_BUILD_JAVA=java", "").trim())
                                .map(l -> l.replace("HAVENASK_BUILD_JAVA=openjdk", "").trim())
                                .collect(Collectors.joining("!!"))
                        )
                    )
                );
                loggedExec.environment(
                    "RUNTIME_JAVA_HOME",
                    getJavaHome(
                        Integer.parseInt(
                            Arrays.asList(javaVersionsString.split("\n"))
                                .stream()
                                .filter(l -> l.trim().startsWith("HAVENASK_RUNTIME_JAVA="))
                                .map(l -> l.replace("HAVENASK_RUNTIME_JAVA=java", "").trim())
                                .map(l -> l.replace("HAVENASK_RUNTIME_JAVA=openjdk", "").trim())
                                .collect(Collectors.joining("!!"))
                        )
                    )
                );
            });

            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                loggedExec.executable("cmd");
                loggedExec.args("/C", "call", new File(checkoutDir.get(), "gradlew").toString());
            } else {
                loggedExec.executable(new File(checkoutDir.get(), "gradlew").toString());
            }
            if (project.getGradle().getStartParameter().isOffline()) {
                loggedExec.args("--offline");
            }
            // TODO resolve
            String buildCacheUrl = System.getProperty("org.havenask.build.cache.url");
            if (buildCacheUrl != null) {
                loggedExec.args("-Dorg.havenask.build.cache.url=" + buildCacheUrl);
            }

            loggedExec.args("-Dbuild.snapshot=true");
            loggedExec.args("-Dscan.tag.NESTED");
            final LogLevel logLevel = project.getGradle().getStartParameter().getLogLevel();
            List<LogLevel> nonDefaultLogLevels = Arrays.asList(LogLevel.QUIET, LogLevel.WARN, LogLevel.INFO, LogLevel.DEBUG);
            if (nonDefaultLogLevels.contains(logLevel)) {
                loggedExec.args("--" + logLevel.name().toLowerCase(Locale.ENGLISH));
            }
            final String showStacktraceName = project.getGradle().getStartParameter().getShowStacktrace().name();
            assert Arrays.asList("INTERNAL_EXCEPTIONS", "ALWAYS", "ALWAYS_FULL").contains(showStacktraceName);
            if (showStacktraceName.equals("ALWAYS")) {
                loggedExec.args("--stacktrace");
            } else if (showStacktraceName.equals("ALWAYS_FULL")) {
                loggedExec.args("--full-stacktrace");
            }
            if (project.getGradle().getStartParameter().isParallelProjectExecutionEnabled()) {
                loggedExec.args("--parallel");
            }
            loggedExec.setStandardOutput(new IndentingOutputStream(System.out, unreleasedVersionInfo.get().version));
            loggedExec.setErrorOutput(new IndentingOutputStream(System.err, unreleasedVersionInfo.get().version));
            configAction.execute(loggedExec);
        });
    }

    private static class IndentingOutputStream extends OutputStream {

        public final byte[] indent;
        private final OutputStream delegate;

        IndentingOutputStream(OutputStream delegate, Object version) {
            this.delegate = delegate;
            indent = (" [" + version + "] ").getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void write(int b) throws IOException {
            int[] arr = { b };
            write(arr, 0, 1);
        }

        public void write(int[] bytes, int offset, int length) throws IOException {
            for (int i = 0; i < bytes.length; i++) {
                delegate.write(bytes[i]);
                if (bytes[i] == '\n') {
                    delegate.write(indent);
                }
            }
        }
    }

    private static String readFromFile(File file) {
        try {
            return FileUtils.readFileToString(file).trim();
        } catch (IOException ioException) {
            throw new GradleException("Cannot read java properties file.", ioException);
        }
    }
}
