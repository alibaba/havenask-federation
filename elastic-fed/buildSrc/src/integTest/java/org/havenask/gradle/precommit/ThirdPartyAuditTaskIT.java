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

import org.havenask.gradle.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.BuildResult;
import org.junit.Before;
import org.junit.Ignore;

import static org.havenask.gradle.test.TestClasspathUtils.setupJarJdkClasspath;

@Ignore
public class ThirdPartyAuditTaskIT extends GradleIntegrationTestCase {

    @Before
    public void setUp() throws Exception {
        // Build the sample jars
        getGradleRunner("thirdPartyAudit").withArguments(":sample_jars:build", "-s").build();
        // propagate jdkjarhell jar
        setupJarJdkClasspath(getProjectDir("thirdPartyAudit"));
    }

    public void testHavenaskIgnored() {
        BuildResult result = getGradleRunner("thirdPartyAudit").withArguments(
            ":clean",
            ":empty",
            "-s",
            "-PcompileOnlyGroup=havenask.gradle:broken-log4j",
            "-PcompileOnlyVersion=0.0.1",
            "-PcompileGroup=havenask.gradle:dummy-io",
            "-PcompileVersion=0.0.1"
        ).build();
        assertTaskNoSource(result, ":empty");
        assertNoDeprecationWarning(result);
    }

    public void testWithEmptyRules() {
        getGradleRunner("thirdPartyAudit").withArguments(
            ":clean",
            ":empty",
            "-s",
            "-PcompileOnlyGroup=other.gradle:broken-log4j",
            "-PcompileOnlyVersion=0.0.1",
            "-PcompileGroup=other.gradle:dummy-io",
            "-PcompileVersion=0.0.1"
        ).buildAndFail();
    }

    @Ignore
    public void testViolationFoundAndCompileOnlyIgnored() {
        BuildResult result = getGradleRunner("thirdPartyAudit").withArguments(
            ":clean",
            ":absurd",
            "-s",
            "-PcompileOnlyGroup=other.gradle:broken-log4j",
            "-PcompileOnlyVersion=0.0.1",
            "-PcompileGroup=other.gradle:dummy-io",
            "-PcompileVersion=0.0.1"
        ).buildAndFail();

        assertTaskFailed(result, ":absurd");
        assertOutputContains(result.getOutput(), "Classes with violations:", "  * TestingIO", "> Audit of third party dependencies failed");
        assertOutputDoesNotContain(result.getOutput(), "Missing classes:");
        assertNoDeprecationWarning(result);
    }

    @Ignore
    public void testClassNotFoundAndCompileOnlyIgnored() {
        BuildResult result = getGradleRunner("thirdPartyAudit").withArguments(
            ":clean",
            ":absurd",
            "-s",
            "-PcompileGroup=other.gradle:broken-log4j",
            "-PcompileVersion=0.0.1",
            "-PcompileOnlyGroup=other.gradle:dummy-io",
            "-PcompileOnlyVersion=0.0.1"
        ).buildAndFail();
        assertTaskFailed(result, ":absurd");

        assertOutputContains(
            result.getOutput(),
            "Missing classes:",
            "  * org.apache.logging.log4j.LogManager",
            "> Audit of third party dependencies failed"
        );
        assertOutputDoesNotContain(result.getOutput(), "Classes with violations:");
        assertNoDeprecationWarning(result);
    }

    public void testJarHellWithJDK() {
        BuildResult result = getGradleRunner("thirdPartyAudit").withArguments(
            ":clean",
            ":absurd",
            "-s",
            "-PcompileGroup=other.gradle:jarhellJdk",
            "-PcompileVersion=0.0.1",
            "-PcompileOnlyGroup=other.gradle:dummy-io",
            "-PcompileOnlyVersion=0.0.1"
        ).buildAndFail();
        assertTaskFailed(result, ":absurd");

        assertOutputContains(
            result.getOutput(),
            "> Audit of third party dependencies failed:",
            "   Jar Hell with the JDK:",
            "    * java.lang.String"
        );
        assertOutputDoesNotContain(result.getOutput(), "Classes with violations:");
        assertNoDeprecationWarning(result);
    }

    public void testHavenaskIgnoredWithViolations() {
        BuildResult result = getGradleRunner("thirdPartyAudit").withArguments(
            ":clean",
            ":absurd",
            "-s",
            "-PcompileOnlyGroup=havenask.gradle:broken-log4j",
            "-PcompileOnlyVersion=0.0.1",
            "-PcompileGroup=havenask.gradle:dummy-io",
            "-PcompileVersion=0.0.1"
        ).build();
        assertTaskNoSource(result, ":absurd");
        assertNoDeprecationWarning(result);
    }

}
