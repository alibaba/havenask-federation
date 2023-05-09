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

import org.havenask.packaging.util.Distribution;
import org.havenask.packaging.util.Shell;
import org.junit.BeforeClass;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.havenask.packaging.util.FileExistenceMatchers.fileDoesNotExist;
import static org.havenask.packaging.util.FileExistenceMatchers.fileExists;
import static org.havenask.packaging.util.FileUtils.append;
import static org.havenask.packaging.util.FileUtils.assertPathsDoNotExist;
import static org.havenask.packaging.util.Packages.SYSTEMD_SERVICE;
import static org.havenask.packaging.util.Packages.SYSVINIT_SCRIPT;
import static org.havenask.packaging.util.Packages.assertInstalled;
import static org.havenask.packaging.util.Packages.assertRemoved;
import static org.havenask.packaging.util.Packages.installPackage;
import static org.havenask.packaging.util.Packages.remove;
import static org.havenask.packaging.util.Packages.verifyPackageInstallation;
import static org.havenask.packaging.util.Platforms.isSystemd;
import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeTrue;

public class RpmPreservationTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only rpm", distribution.packaging == Distribution.Packaging.RPM);
        assumeTrue("only bundled jdk", distribution().hasJdk);
    }

    public void test10Install() throws Exception {
        assertRemoved(distribution());
        installation = installPackage(sh, distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
    }

    public void test20Remove() throws Exception {
        remove(distribution());

        // config was removed
        assertThat(installation.config, fileDoesNotExist());

        // sysvinit service file was removed
        assertThat(SYSVINIT_SCRIPT, fileDoesNotExist());

        // defaults file was removed
        assertThat(installation.envFile, fileDoesNotExist());
    }

    public void test30PreserveConfig() throws Exception {
        final Shell sh = new Shell();

        installation = installPackage(sh, distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);

        sh.run("echo foobar | " + installation.executables().keystoreTool + " add --stdin foo.bar");
        Stream.of("havenask.yml", "jvm.options", "log4j2.properties")
            .map(each -> installation.config(each))
            .forEach(path -> append(path, "# foo"));
        append(installation.config(Paths.get("jvm.options.d", "heap.options")), "# foo");

        remove(distribution());
        assertRemoved(distribution());

        if (isSystemd()) {
            assertThat(sh.runIgnoreExitCode("systemctl is-enabled havenask.service").exitCode, is(1));
        }

        assertPathsDoNotExist(
            installation.bin,
            installation.lib,
            installation.modules,
            installation.plugins,
            installation.logs,
            installation.pidDir,
            installation.envFile,
            SYSVINIT_SCRIPT,
            SYSTEMD_SERVICE
        );

        assertThat(installation.config, fileExists());
        assertThat(installation.config("havenask.keystore"), fileExists());

        Stream.of("havenask.yml", "jvm.options", "log4j2.properties").forEach(this::assertConfFilePreserved);
        assertThat(installation.config(Paths.get("jvm.options.d", "heap.options")), fileExists());
    }

    private void assertConfFilePreserved(String configFile) {
        final Path original = installation.config(configFile);
        final Path saved = installation.config(configFile + ".rpmsave");
        assertConfFilePreserved(original, saved);
    }

    private void assertConfFilePreserved(final Path original, final Path saved) {
        assertThat(original, fileDoesNotExist());
        assertThat(saved, fileExists());
    }

}
