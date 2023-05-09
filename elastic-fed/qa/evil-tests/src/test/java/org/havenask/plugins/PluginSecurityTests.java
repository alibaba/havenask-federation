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

package org.havenask.plugins;

import org.havenask.test.HavenaskTestCase;

import java.nio.file.Path;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

/** Tests plugin manager security check */
public class PluginSecurityTests extends HavenaskTestCase {

    /** Test that we can parse the set of permissions correctly for a simple policy */
    public void testParsePermissions() throws Exception {
        assumeTrue(
                "test cannot run with security manager enabled",
                System.getSecurityManager() == null);
        Path scratch = createTempDir();
        Path testFile = this.getDataPath("security/simple-plugin-security.policy");
        Set<String> actual = PluginSecurity.parsePermissions(testFile, scratch);
        assertThat(actual, contains(PluginSecurity.formatPermission(new RuntimePermission("queuePrintJob"))));
    }

    /** Test that we can parse the set of permissions correctly for a complex policy */
    public void testParseTwoPermissions() throws Exception {
        assumeTrue(
                "test cannot run with security manager enabled",
                System.getSecurityManager() == null);
        Path scratch = createTempDir();
        Path testFile = this.getDataPath("security/complex-plugin-security.policy");
        Set<String> actual = PluginSecurity.parsePermissions(testFile, scratch);
        assertThat(actual, containsInAnyOrder(
            PluginSecurity.formatPermission(new RuntimePermission("getClassLoader")),
            PluginSecurity.formatPermission(new RuntimePermission("closeClassLoader"))));
    }

    /** Test that we can format some simple permissions properly */
    public void testFormatSimplePermission() throws Exception {
        assertEquals(
                "java.lang.RuntimePermission queuePrintJob",
                PluginSecurity.formatPermission(new RuntimePermission("queuePrintJob")));
    }

    /** Test that we can format an unresolved permission properly */
    public void testFormatUnresolvedPermission() throws Exception {
        assumeTrue(
                "test cannot run with security manager enabled",
                System.getSecurityManager() == null);
        Path scratch = createTempDir();
        Path testFile = this.getDataPath("security/unresolved-plugin-security.policy");
        Set<String> permissions = PluginSecurity.parsePermissions(testFile, scratch);
        assertThat(permissions, contains("org.fake.FakePermission fakeName"));
    }
}
