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
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.havenask.gradle.doc

import org.havenask.gradle.OS
import org.havenask.gradle.Version
import org.havenask.gradle.VersionProperties
import org.gradle.api.Plugin
import org.gradle.api.Project

/**
 * Sets up tests for documentation.
 */
class DocsTestPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        project.pluginManager.apply('havenask.testclusters')
        project.pluginManager.apply('havenask.standalone-rest-test')
        project.pluginManager.apply('havenask.rest-test')

        String distribution = System.getProperty('tests.distribution', 'archive')
        // The distribution can be configured with -Dtests.distribution on the command line
        project.testClusters.integTest.testDistribution = distribution.toUpperCase()

        project.testClusters.integTest.nameCustomization = { it.replace("integTest", "node") }
        // Docs are published separately so no need to assemble
        project.tasks.assemble.enabled = false
        Map<String, String> commonDefaultSubstitutions = [
                /* These match up with the asciidoc syntax for substitutions but
                 * the values may differ. In particular {version} needs to resolve
                 * to the version being built for testing but needs to resolve to
                 * the last released version for docs. */
            '\\{version\\}': Version.fromString(VersionProperties.getHavenask()).toString(),
            '\\{version_qualified\\}': VersionProperties.getHavenask(),
            '\\{lucene_version\\}' : VersionProperties.lucene.replaceAll('-snapshot-\\w+$', ''),
            '\\{build_type\\}' : OS.conditionalString().onWindows({"zip"}).onUnix({"tar"}).supply(),
        ]
        project.tasks.register('listSnippets', SnippetsTask) {
            group 'Docs'
            description 'List each snippet'
            defaultSubstitutions = commonDefaultSubstitutions
            perSnippet { println(it.toString()) }
        }
        project.tasks.register('listConsoleCandidates', SnippetsTask) {
            group 'Docs'
            description
            'List snippets that probably should be marked // CONSOLE'
            defaultSubstitutions = commonDefaultSubstitutions
            perSnippet {
                if (RestTestsFromSnippetsTask.isConsoleCandidate(it)) {
                    println(it.toString())
                }
            }
        }

        project.tasks.register('buildRestTests', RestTestsFromSnippetsTask) {
            defaultSubstitutions = commonDefaultSubstitutions
        }
    }
}
