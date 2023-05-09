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

import org.havenask.gradle.Architecture;
import org.havenask.gradle.BwcVersions;
import org.havenask.gradle.DistributionDependency;
import org.havenask.gradle.DistributionDownloadPlugin;
import org.havenask.gradle.DistributionResolution;
import org.havenask.gradle.HavenaskDistribution;
import org.havenask.gradle.Version;
import org.havenask.gradle.VersionProperties;
import org.havenask.gradle.info.BuildParams;
import org.havenask.gradle.info.GlobalBuildInfoPlugin;
import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;

import java.util.function.Function;

import static org.havenask.gradle.util.GradleUtils.projectDependency;

/**
 * An internal havenask build plugin that registers additional
 * distribution resolution strategies to the 'havenask.download-distribution' plugin
 * to resolve distributions from a local snapshot or a locally built bwc snapshot.
 */
public class InternalDistributionDownloadPlugin implements Plugin<Project> {

    private BwcVersions bwcVersions = null;

    @Override
    public void apply(Project project) {
        // this is needed for isInternal
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        if (!BuildParams.isInternal()) {
            throw new GradleException(
                "Plugin 'havenask.internal-distribution-download' is not supported. "
                    + "Use 'havenask.distribution-download' plugin instead."
            );
        }
        project.getPluginManager().apply(DistributionDownloadPlugin.class);
        this.bwcVersions = BuildParams.getBwcVersions();
        registerInternalDistributionResolutions(DistributionDownloadPlugin.getRegistrationsContainer(project));
    }

    /**
     * Registers internal distribution resolutions.
     * <p>
     * Havenask distributions are resolved as project dependencies either representing
     * the current version pointing to a project either under `:distribution:archives` or :distribution:packages`.
     * <p>
     * BWC versions are resolved as project to projects under `:distribution:bwc`.
     */
    private void registerInternalDistributionResolutions(NamedDomainObjectContainer<DistributionResolution> resolutions) {

        resolutions.register("localBuild", distributionResolution -> distributionResolution.setResolver((project, distribution) -> {
            if (VersionProperties.getHavenask().equals(distribution.getVersion())) {
                // non-external project, so depend on local build
                return new ProjectBasedDistributionDependency(
                    config -> projectDependency(project, distributionProjectPath(distribution), config)
                );
            }
            return null;
        }));

        resolutions.register("bwc", distributionResolution -> distributionResolution.setResolver((project, distribution) -> {
            BwcVersions.UnreleasedVersionInfo unreleasedInfo = bwcVersions.unreleasedInfo(Version.fromString(distribution.getVersion()));
            if (unreleasedInfo != null) {
                if (!distribution.getBundledJdk()) {
                    throw new GradleException(
                        "Configuring a snapshot bwc distribution ('"
                            + distribution.getName()
                            + "') "
                            + "without a bundled JDK is not supported."
                    );
                }
                String projectConfig = getProjectConfig(distribution, unreleasedInfo);
                return new ProjectBasedDistributionDependency(
                    (config) -> projectDependency(project, unreleasedInfo.gradleProjectPath, projectConfig)
                );
            }
            return null;
        }));
    }

    /**
     * Will be removed once this is backported to all unreleased branches.
     */
    private static String getProjectConfig(HavenaskDistribution distribution, BwcVersions.UnreleasedVersionInfo info) {
        String distributionProjectName = distributionProjectName(distribution);
        if (distribution.getType().shouldExtract()) {
            return (info.gradleProjectPath.equals(":distribution") || info.version.before("7.10.0"))
                ? distributionProjectName
                : "expanded-" + distributionProjectName;
        } else {
            return distributionProjectName;

        }

    }

    private static String distributionProjectPath(HavenaskDistribution distribution) {
        String projectPath = ":distribution";
        switch (distribution.getType()) {
            case INTEG_TEST_ZIP:
                projectPath += ":archives:integ-test-zip";
                break;

            case DOCKER:
                projectPath += ":docker:";
                projectPath += distributionProjectName(distribution);
                break;

            default:
                projectPath += distribution.getType() == HavenaskDistribution.Type.ARCHIVE ? ":archives:" : ":packages:";
                projectPath += distributionProjectName(distribution);
                break;
        }
        return projectPath;
    }

    /**
     * Works out the gradle project name that provides a distribution artifact.
     *
     * @param distribution the distribution from which to derive a project name
     * @return the name of a project. It is not the full project path, only the name.
     */
    private static String distributionProjectName(HavenaskDistribution distribution) {
        HavenaskDistribution.Platform platform = distribution.getPlatform();
        Architecture architecture = distribution.getArchitecture();
        String projectName = "";

        final String archString = platform == HavenaskDistribution.Platform.WINDOWS || architecture == Architecture.X64
            ? ""
            : "-" + architecture.toString().toLowerCase();

        if (distribution.getBundledJdk() == false) {
            projectName += "no-jdk-";
        }
        switch (distribution.getType()) {
            case ARCHIVE:
                if (Version.fromString(distribution.getVersion()).onOrAfter("7.0.0")) {
                    projectName += platform.toString() + archString + (platform == HavenaskDistribution.Platform.WINDOWS ? "-zip" : "-tar");
                } else {
                    projectName = "zip";
                }
                break;

            case DOCKER:
                projectName += "docker" + archString + "-export";
                break;

            default:
                projectName += distribution.getType();
                break;
        }
        return projectName;
    }

    private static class ProjectBasedDistributionDependency implements DistributionDependency {

        private Function<String, Dependency> function;

        ProjectBasedDistributionDependency(Function<String, Dependency> function) {
            this.function = function;
        }

        @Override
        public Object getDefaultNotation() {
            return function.apply("default");
        }

        @Override
        public Object getExtractedNotation() {
            return function.apply("extracted");
        }
    }
}
