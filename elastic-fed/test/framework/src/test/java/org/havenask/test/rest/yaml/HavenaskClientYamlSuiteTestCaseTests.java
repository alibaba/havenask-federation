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

package org.havenask.test.rest.yaml;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import org.havenask.test.HavenaskTestCase;
import org.havenask.test.rest.yaml.HavenaskClientYamlSuiteTestCase;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;

public class HavenaskClientYamlSuiteTestCaseTests extends HavenaskTestCase {

    public void testLoadAllYamlSuites() throws Exception {
        Map<String,Set<Path>> yamlSuites = HavenaskClientYamlSuiteTestCase.loadSuites("");
        assertEquals(2, yamlSuites.size());
    }

    public void testLoadSingleYamlSuite() throws Exception {
        Map<String,Set<Path>> yamlSuites = HavenaskClientYamlSuiteTestCase.loadSuites("suite1/10_basic");
        assertSingleFile(yamlSuites, "suite1", "10_basic.yml");

        //extension .yaml is optional
        yamlSuites = HavenaskClientYamlSuiteTestCase.loadSuites("suite1/10_basic");
        assertSingleFile(yamlSuites, "suite1", "10_basic.yml");
    }

    public void testLoadMultipleYamlSuites() throws Exception {
        //single directory
        Map<String,Set<Path>> yamlSuites = HavenaskClientYamlSuiteTestCase.loadSuites("suite1");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(1));
        assertThat(yamlSuites.containsKey("suite1"), equalTo(true));
        assertThat(yamlSuites.get("suite1").size(), greaterThan(1));

        //multiple directories
        yamlSuites = HavenaskClientYamlSuiteTestCase.loadSuites("suite1", "suite2");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(2));
        assertThat(yamlSuites.containsKey("suite1"), equalTo(true));
        assertEquals(2, yamlSuites.get("suite1").size());
        assertThat(yamlSuites.containsKey("suite2"), equalTo(true));
        assertEquals(2, yamlSuites.get("suite2").size());

        //multiple paths, which can be both directories or yaml test suites (with optional file extension)
        yamlSuites = HavenaskClientYamlSuiteTestCase.loadSuites("suite2/10_basic", "suite1");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(2));
        assertThat(yamlSuites.containsKey("suite2"), equalTo(true));
        assertThat(yamlSuites.get("suite2").size(), equalTo(1));
        assertSingleFile(yamlSuites.get("suite2"), "suite2", "10_basic.yml");
        assertThat(yamlSuites.containsKey("suite1"), equalTo(true));
        assertThat(yamlSuites.get("suite1").size(), greaterThan(1));

        //files can be loaded from classpath and from file system too
        Path dir = createTempDir();
        Path file = dir.resolve("test_loading.yml");
        Files.createFile(file);
    }

    private static void assertSingleFile(Map<String, Set<Path>> yamlSuites, String dirName, String fileName) {
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(1));
        assertThat(yamlSuites.containsKey(dirName), equalTo(true));
        assertSingleFile(yamlSuites.get(dirName), dirName, fileName);
    }

    private static void assertSingleFile(Set<Path> files, String dirName, String fileName) {
        assertThat(files.size(), equalTo(1));
        Path file = files.iterator().next();
        assertThat(file.getFileName().toString(), equalTo(fileName));
        assertThat(file.toAbsolutePath().getParent().getFileName().toString(), equalTo(dirName));
    }
}
