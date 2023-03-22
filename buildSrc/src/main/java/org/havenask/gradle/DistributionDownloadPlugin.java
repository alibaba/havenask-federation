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

package org.havenask.gradle;

import org.havenask.gradle.HavenaskDistribution.Platform;
import org.havenask.gradle.HavenaskDistribution.Type;
import org.havenask.gradle.docker.DockerSupportPlugin;
import org.havenask.gradle.docker.DockerSupportService;
import org.havenask.gradle.transform.SymbolicLinkPreservingUntarTransform;
import org.havenask.gradle.transform.UnzipTransform;
import org.havenask.gradle.util.GradleUtils;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.artifacts.repositories.IvyArtifactRepository;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.internal.artifacts.ArtifactAttributes;
import org.gradle.api.provider.Provider;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A plugin to manage getting and extracting distributions of Havenask.
 * <p>
 * The plugin provides hooks to register custom distribution resolutions.
 * This plugin resolves distributions from the Havenask downloads service if
 * no registered resolution strategy can resolve to a distribution.
 */
public class DistributionDownloadPlugin implements Plugin<Project> {

    static final String RESOLUTION_CONTAINER_NAME = "havenask_distributions_resolutions";
    private static final String CONTAINER_NAME = "havenask_distributions";
    private static final String FAKE_IVY_GROUP = "havenask-distribution";
    private static final String FAKE_SNAPSHOT_IVY_GROUP = "havenask-distribution-snapshot";
    private static final String DOWNLOAD_REPO_NAME = "havenask-downloads";
    private static final String SNAPSHOT_REPO_NAME = "havenask-snapshots";
    public static final String DISTRO_EXTRACTED_CONFIG_PREFIX = "havenask_distro_extracted_";

    // for downloading Elasticsearch OSS distributions to run BWC
    private static final String FAKE_IVY_GROUP_ES = "elasticsearch-distribution";
    private static final String DOWNLOAD_REPO_NAME_ES = "elasticsearch-downloads";
    private static final String SNAPSHOT_REPO_NAME_ES = "elasticsearch-snapshots";
    private static final String FAKE_SNAPSHOT_IVY_GROUP_ES = "elasticsearch-distribution-snapshot";

    private static final String RELEASE_PATTERN_LAYOUT = "/core/havenask/[revision]/[module]-min-[revision](-[classifier]).[ext]";
    private static final String SNAPSHOT_PATTERN_LAYOUT =
        "/snapshots/core/havenask/[revision]/[module]-min-[revision](-[classifier])-latest.[ext]";

    private NamedDomainObjectContainer<HavenaskDistribution> distributionsContainer;
    private NamedDomainObjectContainer<DistributionResolution> distributionsResolutionStrategiesContainer;

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(DockerSupportPlugin.class);
        Provider<DockerSupportService> dockerSupport = GradleUtils.getBuildService(
            project.getGradle().getSharedServices(),
            DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME
        );

        project.getDependencies().registerTransform(UnzipTransform.class, transformSpec -> {
            transformSpec.getFrom().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.ZIP_TYPE);
            transformSpec.getTo().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
        });

        ArtifactTypeDefinition tarArtifactTypeDefinition = project.getDependencies().getArtifactTypes().maybeCreate("tar.gz");
        project.getDependencies().registerTransform(SymbolicLinkPreservingUntarTransform.class, transformSpec -> {
            transformSpec.getFrom().attribute(ArtifactAttributes.ARTIFACT_FORMAT, tarArtifactTypeDefinition.getName());
            transformSpec.getTo().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
        });

        setupResolutionsContainer(project);
        setupDistributionContainer(project, dockerSupport);
        setupDownloadServiceRepo(project);
        project.afterEvaluate(this::setupDistributions);
    }

    private void setupDistributionContainer(Project project, Provider<DockerSupportService> dockerSupport) {
        distributionsContainer = project.container(HavenaskDistribution.class, name -> {
            Configuration fileConfiguration = project.getConfigurations().create("havenask_distro_file_" + name);
            Configuration extractedConfiguration = project.getConfigurations().create(DISTRO_EXTRACTED_CONFIG_PREFIX + name);
            extractedConfiguration.getAttributes().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
            return new HavenaskDistribution(name, project.getObjects(), dockerSupport, fileConfiguration, extractedConfiguration);
        });
        project.getExtensions().add(CONTAINER_NAME, distributionsContainer);
    }

    private void setupResolutionsContainer(Project project) {
        distributionsResolutionStrategiesContainer = project.container(DistributionResolution.class);
        // We want this ordered in the same resolution strategies are added
        distributionsResolutionStrategiesContainer.whenObjectAdded(
            resolveDependencyNotation -> resolveDependencyNotation.setPriority(distributionsResolutionStrategiesContainer.size())
        );
        project.getExtensions().add(RESOLUTION_CONTAINER_NAME, distributionsResolutionStrategiesContainer);
    }

    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<HavenaskDistribution> getContainer(Project project) {
        return (NamedDomainObjectContainer<HavenaskDistribution>) project.getExtensions().getByName(CONTAINER_NAME);
    }

    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<DistributionResolution> getRegistrationsContainer(Project project) {
        return (NamedDomainObjectContainer<DistributionResolution>) project.getExtensions().getByName(RESOLUTION_CONTAINER_NAME);
    }

    // pkg private for tests
    void setupDistributions(Project project) {
        for (HavenaskDistribution distribution : distributionsContainer) {
            distribution.finalizeValues();
            DependencyHandler dependencies = project.getDependencies();
            // for the distribution as a file, just depend on the artifact directly
            DistributionDependency distributionDependency = resolveDependencyNotation(project, distribution);
            dependencies.add(distribution.configuration.getName(), distributionDependency.getDefaultNotation());
            // no extraction allowed for rpm, deb or docker
            if (distribution.getType().shouldExtract()) {
                // The extracted configuration depends on the artifact directly but has
                // an artifact transform registered to resolve it as an unpacked folder.
                dependencies.add(distribution.getExtracted().getName(), distributionDependency.getExtractedNotation());
            }
        }
    }

    private DistributionDependency resolveDependencyNotation(Project p, HavenaskDistribution distribution) {
        return distributionsResolutionStrategiesContainer.stream()
            .sorted(Comparator.comparingInt(DistributionResolution::getPriority))
            .map(r -> r.getResolver().resolve(p, distribution))
            .filter(d -> d != null)
            .findFirst()
            .orElseGet(() -> DistributionDependency.of(dependencyNotation(distribution)));
    }

    private static void addIvyRepo(Project project, String name, String url, String group, String... patternLayout) {
        final List<IvyArtifactRepository> repos = Arrays.stream(patternLayout).map(pattern -> project.getRepositories().ivy(repo -> {
            repo.setName(name);
            repo.setUrl(url);
            repo.metadataSources(IvyArtifactRepository.MetadataSources::artifact);
            repo.patternLayout(layout -> layout.artifact(pattern));
        })).collect(Collectors.toList());

        project.getRepositories().exclusiveContent(exclusiveContentRepository -> {
            exclusiveContentRepository.filter(config -> config.includeGroup(group));
            exclusiveContentRepository.forRepositories(repos.toArray(new IvyArtifactRepository[repos.size()]));
        });
    }

    private static void addIvyRepo2(Project project, String name, String url, String group) {
        IvyArtifactRepository ivyRepo = project.getRepositories().ivy(repo -> {
            repo.setName(name);
            repo.setUrl(url);
            repo.metadataSources(IvyArtifactRepository.MetadataSources::artifact);
            repo.patternLayout(layout -> layout.artifact("/downloads/elasticsearch/elasticsearch-oss-[revision](-[classifier]).[ext]"));
        });
        project.getRepositories().exclusiveContent(exclusiveContentRepository -> {
            exclusiveContentRepository.filter(config -> config.includeGroup(group));
            exclusiveContentRepository.forRepositories(ivyRepo);
        });
    }

    private static void setupDownloadServiceRepo(Project project) {
        if (project.getRepositories().findByName(DOWNLOAD_REPO_NAME) != null) {
            return;
        }
        addIvyRepo(
            project,
            DOWNLOAD_REPO_NAME,
            "https://artifacts.havenask.org",
            FAKE_IVY_GROUP,
            "/releases" + RELEASE_PATTERN_LAYOUT,
            "/release-candidates" + RELEASE_PATTERN_LAYOUT
        );

        addIvyRepo(project, SNAPSHOT_REPO_NAME, "https://artifacts.havenask.org", FAKE_SNAPSHOT_IVY_GROUP, SNAPSHOT_PATTERN_LAYOUT);

        addIvyRepo2(project, DOWNLOAD_REPO_NAME_ES, "https://artifacts-no-kpi.elastic.co", FAKE_IVY_GROUP_ES);
        addIvyRepo2(project, SNAPSHOT_REPO_NAME_ES, "https://snapshots-no-kpi.elastic.co", FAKE_SNAPSHOT_IVY_GROUP_ES);
    }

    /**
     * Returns a dependency object representing the given distribution.
     * <p>
     * The returned object is suitable to be passed to {@link DependencyHandler}.
     * The concrete type of the object will be a set of maven coordinates as a {@link String}.
     * Maven coordinates point to either the integ-test-zip coordinates on maven central, or a set of artificial
     * coordinates that resolve to the Elastic download service through an ivy repository.
     */
    private String dependencyNotation(HavenaskDistribution distribution) {
        Version distroVersion = Version.fromString(distribution.getVersion());
        if (distribution.getType() == Type.INTEG_TEST_ZIP) {
            if (distroVersion.onOrAfter("1.0.0")) {
                return "org.havenask.distribution.integ-test-zip:havenask:" + distribution.getVersion() + "@zip";
            } else {
                return "org.elasticsearch.distribution.integ-test-zip:elasticsearch:" + distribution.getVersion() + "@zip";
            }
        }

        String extension = distribution.getType().toString();
        String classifier = ":" + (Architecture.current() == Architecture.AARCH64 ? "aarch64" : "x86_64");
        if (distribution.getType() == Type.ARCHIVE) {
            extension = distribution.getPlatform() == Platform.WINDOWS ? "zip" : "tar.gz";
            if (distroVersion.onOrAfter("7.0.0")) {
                classifier = ":"
                    + distribution.getPlatform()
                    + "-"
                    + (Architecture.current() == Architecture.AARCH64 ? "aarch64" : "x86_64");
            } else {
                classifier = "";
            }
        } else if (distribution.getType() == Type.DEB) {
            if (distroVersion.onOrAfter("7.0.0")) {
                classifier = ":amd64";
            } else {
                classifier = "";
            }
        } else if (distribution.getType() == Type.RPM && distroVersion.before("7.0.0")) {
            classifier = "";
        }

        String group;
        if (distroVersion.onOrAfter("1.0.0")) {
            group = distribution.getVersion().endsWith("-SNAPSHOT") ? FAKE_SNAPSHOT_IVY_GROUP : FAKE_IVY_GROUP;
            return group + ":havenask" + ":" + distribution.getVersion() + classifier + "@" + extension;
        } else {
            group = distribution.getVersion().endsWith("-SNAPSHOT") ? FAKE_SNAPSHOT_IVY_GROUP_ES : FAKE_IVY_GROUP_ES;
            return group + ":elasticsearch-oss" + ":" + distribution.getVersion() + classifier + "@" + extension;
        }
    }
}
