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

import org.havenask.gradle.transform.SymbolicLinkPreservingUntarTransform;
import org.havenask.gradle.transform.UnzipTransform;
import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.RepositoryHandler;
import org.gradle.api.artifacts.repositories.IvyArtifactRepository;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.internal.artifacts.ArtifactAttributes;

public class JdkDownloadPlugin implements Plugin<Project> {

    public static final String VENDOR_ADOPTOPENJDK = "adoptopenjdk";
    public static final String VENDOR_OPENJDK = "openjdk";
    public static final String VENDOR_AZUL = "azul";

    private static final String REPO_NAME_PREFIX = "jdk_repo_";
    private static final String EXTENSION_NAME = "jdks";
    public static final String JDK_TRIMMED_PREFIX = "jdk-?\\d.*";

    @Override
    public void apply(Project project) {
        Attribute<Boolean> jdkAttribute = Attribute.of("jdk", Boolean.class);
        project.getDependencies().getAttributesSchema().attribute(jdkAttribute);
        project.getDependencies().getArtifactTypes().maybeCreate(ArtifactTypeDefinition.ZIP_TYPE);
        project.getDependencies().registerTransform(UnzipTransform.class, transformSpec -> {
            transformSpec.getFrom()
                .attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.ZIP_TYPE)
                .attribute(jdkAttribute, true);
            transformSpec.getTo()
                .attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE)
                .attribute(jdkAttribute, true);
            transformSpec.parameters(parameters -> parameters.setTrimmedPrefixPattern(JDK_TRIMMED_PREFIX));
        });

        ArtifactTypeDefinition tarArtifactTypeDefinition = project.getDependencies().getArtifactTypes().maybeCreate("tar.gz");
        project.getDependencies().registerTransform(SymbolicLinkPreservingUntarTransform.class, transformSpec -> {
            transformSpec.getFrom()
                .attribute(ArtifactAttributes.ARTIFACT_FORMAT, tarArtifactTypeDefinition.getName())
                .attribute(jdkAttribute, true);
            transformSpec.getTo()
                .attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE)
                .attribute(jdkAttribute, true);
            transformSpec.parameters(parameters -> parameters.setTrimmedPrefixPattern(JDK_TRIMMED_PREFIX));
        });

        NamedDomainObjectContainer<Jdk> jdksContainer = project.container(Jdk.class, name -> {
            Configuration configuration = project.getConfigurations().create("jdk_" + name);
            configuration.setCanBeConsumed(false);
            configuration.getAttributes().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
            configuration.getAttributes().attribute(jdkAttribute, true);
            Jdk jdk = new Jdk(name, configuration, project.getObjects());
            configuration.defaultDependencies(dependencies -> {
                jdk.finalizeValues();
                setupRepository(project, jdk);
                dependencies.add(project.getDependencies().create(dependencyNotation(jdk)));
            });
            return jdk;
        });
        project.getExtensions().add(EXTENSION_NAME, jdksContainer);
    }

    private void setupRepository(Project project, Jdk jdk) {
        RepositoryHandler repositories = project.getRepositories();

        /*
         * Define the appropriate repository for the given JDK vendor and version
         *
         * For Oracle/OpenJDK/AdoptOpenJDK we define a repository per-version.
         */
        String repoName = REPO_NAME_PREFIX + jdk.getVendor() + "_" + jdk.getVersion();
        String repoUrl;
        String artifactPattern;

        String vendor = jdk.getVendor();
        if (jdk.getArchitecture().equals("aarch64") && (jdk.getPlatform().equals("mac") || jdk.getPlatform().equals("darwin"))) {
            vendor = VENDOR_AZUL;
        }

        if (vendor.equals(VENDOR_ADOPTOPENJDK)) {
            repoUrl = "https://api.adoptopenjdk.net/v3/binary/version/";
            if (jdk.getMajor().equals("8")) {
                // legacy pattern for JDK 8
                artifactPattern = "jdk"
                    + jdk.getBaseVersion()
                    + "-"
                    + jdk.getBuild()
                    + "/[module]/[classifier]/jdk/hotspot/normal/adoptopenjdk";
            } else {
                // current pattern since JDK 9
                artifactPattern = "jdk-"
                    + jdk.getBaseVersion()
                    + "+"
                    + jdk.getBuild()
                    + "/[module]/[classifier]/jdk/hotspot/normal/adoptopenjdk";
            }
        } else if (vendor.equals(VENDOR_OPENJDK)) {
            repoUrl = "https://download.java.net";
            if (jdk.getHash() != null) {
                // current pattern since 12.0.1
                artifactPattern = "java/GA/jdk"
                    + jdk.getBaseVersion()
                    + "/"
                    + jdk.getHash()
                    + "/"
                    + jdk.getBuild()
                    + "/GPL/openjdk-[revision]_[module]-[classifier]_bin.[ext]";
            } else {
                // simpler legacy pattern from JDK 9 to JDK 12 that we are advocating to Oracle to bring back
                artifactPattern = "java/GA/jdk"
                    + jdk.getMajor()
                    + "/"
                    + jdk.getBuild()
                    + "/GPL/openjdk-[revision]_[module]-[classifier]_bin.[ext]";
            }
        } else if (vendor.equals(VENDOR_AZUL)) {
            repoUrl = "https://cdn.azul.com";
            // The following is an absolute hack until AdoptOpenJdk provides Apple aarch64 builds
            switch (jdk.getMajor()) {
                case "15":
                    artifactPattern = "zulu/bin/zulu" + jdk.getMajor() + ".28.1013-ca-jdk15.0.1-macosx_[classifier].[ext]";
                    break;
                case "11":
                    artifactPattern = "zulu/bin/zulu" + jdk.getMajor() + ".43.1021-ca-jdk11.0.9.1-macosx_[classifier].[ext]";
                    break;
                default:
                    throw new GradleException("Unknown Azul JDK major version  [" + jdk.getMajor() + "]");
            }
        } else {
            throw new GradleException("Unknown JDK vendor [" + vendor + "]");
        }

        // Define the repository if we haven't already
        if (repositories.findByName(repoName) == null) {
            repositories.ivy(repo -> {
                repo.setName(repoName);
                repo.setUrl(repoUrl);
                repo.metadataSources(IvyArtifactRepository.MetadataSources::artifact);
                repo.patternLayout(layout -> layout.artifact(artifactPattern));
                repo.content(repositoryContentDescriptor -> repositoryContentDescriptor.includeGroup(groupName(jdk)));
            });
        }
    }

    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<Jdk> getContainer(Project project) {
        return (NamedDomainObjectContainer<Jdk>) project.getExtensions().getByName(EXTENSION_NAME);
    }

    private static String dependencyNotation(Jdk jdk) {
        String platformDep = jdk.getPlatform().equals("darwin") || jdk.getPlatform().equals("mac")
            ? (jdk.getVendor().equals(VENDOR_ADOPTOPENJDK) ? "mac" : "osx")
            : jdk.getPlatform();
        String extension = jdk.getPlatform().equals("windows") ? "zip" : "tar.gz";

        return groupName(jdk) + ":" + platformDep + ":" + jdk.getBaseVersion() + ":" + jdk.getArchitecture() + "@" + extension;
    }

    private static String groupName(Jdk jdk) {
        return jdk.getVendor() + "_" + jdk.getMajor();
    }

}
