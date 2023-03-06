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
*
* Modifications Copyright Havenask Contributors. See
* GitHub history for details.
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

package org.havenask.gradle.fixtures


import org.havenask.gradle.HavenaskDistribution
import org.havenask.gradle.Version
import org.havenask.gradle.VersionProperties
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner

class DistributionDownloadFixture {

    public static final String INIT_SCRIPT = "repositories-init.gradle"

    static BuildResult withMockedDistributionDownload(GradleRunner gradleRunner, Closure<BuildResult> buildRunClosure) {
        return withMockedDistributionDownload(VersionProperties.getHavenask(), HavenaskDistribution.CURRENT_PLATFORM,
                gradleRunner, buildRunClosure)
    }

    static BuildResult withMockedDistributionDownload(String version, HavenaskDistribution.Platform platform,
                                                      GradleRunner gradleRunner, Closure<BuildResult> buildRunClosure) {
        String urlPath = urlPath(version, platform);
        return WiremockFixture.withWireMock(urlPath, filebytes(urlPath)) { server ->
            File initFile = new File(gradleRunner.getProjectDir(), INIT_SCRIPT)
            initFile.text = """allprojects { p ->
                p.repositories.all { repo ->
                    repo.allowInsecureProtocol = true
                    repo.setUrl('${server.baseUrl()}')
                }
            }"""
            List<String> givenArguments = gradleRunner.getArguments()
            GradleRunner effectiveRunner = gradleRunner.withArguments(givenArguments + ['-I', initFile.getAbsolutePath()])
            buildRunClosure.delegate = effectiveRunner
            return buildRunClosure.call(effectiveRunner)
        }
    }

    private static String urlPath(String version, HavenaskDistribution.Platform platform) {
        String fileType = ((platform == HavenaskDistribution.Platform.LINUX ||
                platform == HavenaskDistribution.Platform.DARWIN)) ? "tar.gz" : "zip"
        "/releases/core/havenask/${version}/havenask-${version}-${platform}-x86_64.$fileType"

    }

    private static byte[] filebytes(String urlPath) throws IOException {
        String suffix = urlPath.endsWith("zip") ? "zip" : "tar.gz";
        return DistributionDownloadFixture.getResourceAsStream("/org/havenask/gradle/fake_havenask." + suffix).getBytes()
    }
}
