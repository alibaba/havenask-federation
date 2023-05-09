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

package org.havenask.packaging.test;

import org.havenask.packaging.util.Platforms;
import org.havenask.packaging.util.ServerUtils;
import org.havenask.packaging.util.Shell;
import org.junit.BeforeClass;

import static org.havenask.packaging.util.FileUtils.assertPathsExist;
import static org.havenask.packaging.util.FileUtils.fileWithGlobExist;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class SysVInitTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("rpm or deb", distribution.isPackage());
        assumeTrue(Platforms.isSysVInit());
        assumeFalse(Platforms.isSystemd());
    }

    @Override
    public void startHavenask() throws Exception {
        sh.run("service havenask start");
        ServerUtils.waitForHavenask(installation);
        sh.run("service havenask status");
    }

    @Override
    public void stopHavenask() {
        sh.run("service havenask stop");
    }

    public void test10Install() throws Exception {
        install();
    }

    public void test20Start() throws Exception {
        startHavenask();
        assertThat(installation.logs, fileWithGlobExist("gc.log*"));
        ServerUtils.runHavenaskTests();
        sh.run("service havenask status"); // returns 0 exit status when ok
    }

    public void test21Restart() throws Exception {
        sh.run("service havenask restart");
        sh.run("service havenask status"); // returns 0 exit status when ok
    }

    public void test22Stop() throws Exception {
        stopHavenask();
        Shell.Result status = sh.runIgnoreExitCode("service havenask status");
        assertThat(status.exitCode, anyOf(equalTo(3), equalTo(4)));
    }

    public void test30PidDirCreation() throws Exception {
        // Simulates the behavior of a system restart:
        // the PID directory is deleted by the operating system
        // but it should not block ES from starting
        // see https://github.com/elastic/elasticsearch/issues/11594

        sh.run("rm -rf " + installation.pidDir);
        startHavenask();
        assertPathsExist(installation.pidDir.resolve("havenask.pid"));
        stopHavenask();
    }

    public void test31MaxMapTooSmall() throws Exception {
        sh.run("sysctl -q -w vm.max_map_count=262140");
        startHavenask();
        Shell.Result result = sh.run("sysctl -n vm.max_map_count");
        String maxMapCount = result.stdout.trim();
        sh.run("service havenask stop");
        assertThat(maxMapCount, equalTo("262144"));
    }

    public void test32MaxMapBigEnough() throws Exception {
        // Ensures that if $MAX_MAP_COUNT is greater than the set
        // value on the OS we do not attempt to update it.
        sh.run("sysctl -q -w vm.max_map_count=262145");
        startHavenask();
        Shell.Result result = sh.run("sysctl -n vm.max_map_count");
        String maxMapCount = result.stdout.trim();
        sh.run("service havenask stop");
        assertThat(maxMapCount, equalTo("262145"));
    }

}
