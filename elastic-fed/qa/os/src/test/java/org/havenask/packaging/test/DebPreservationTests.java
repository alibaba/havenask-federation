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
import org.junit.BeforeClass;

import java.nio.file.Paths;

import static org.havenask.packaging.util.FileExistenceMatchers.fileExists;
import static org.havenask.packaging.util.FileUtils.append;
import static org.havenask.packaging.util.FileUtils.assertPathsDoNotExist;
import static org.havenask.packaging.util.FileUtils.assertPathsExist;
import static org.havenask.packaging.util.Packages.SYSVINIT_SCRIPT;
import static org.havenask.packaging.util.Packages.assertInstalled;
import static org.havenask.packaging.util.Packages.assertRemoved;
import static org.havenask.packaging.util.Packages.installPackage;
import static org.havenask.packaging.util.Packages.remove;
import static org.havenask.packaging.util.Packages.verifyPackageInstallation;
import static org.junit.Assume.assumeTrue;

public class DebPreservationTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only deb", distribution.packaging == Distribution.Packaging.DEB);
        assumeTrue("only bundled jdk", distribution.hasJdk);
    }

    public void test10Install() throws Exception {
        assertRemoved(distribution());
        installation = installPackage(sh, distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
    }

    public void test20Remove() throws Exception {
        append(installation.config(Paths.get("jvm.options.d", "heap.options")), "# foo");

        remove(distribution());

        // some config files were not removed
        assertPathsExist(
            installation.config,
            installation.config("havenask.yml"),
            installation.config("jvm.options"),
            installation.config("log4j2.properties"),
            installation.config(Paths.get("jvm.options.d", "heap.options"))
        );

        // keystore was removed

        assertPathsDoNotExist(installation.config("havenask.keystore"), installation.config(".havenask.keystore.initial_md5sum"));

        // sysvinit service file was not removed
        assertThat(SYSVINIT_SCRIPT, fileExists());

        // defaults file was not removed
        assertThat(installation.envFile, fileExists());
    }
}
