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

import junit.framework.TestCase;
import org.havenask.packaging.util.Distribution;
import org.havenask.packaging.util.FileUtils;
import org.havenask.packaging.util.Shell;
import org.junit.BeforeClass;

import java.util.regex.Pattern;

import static org.havenask.packaging.util.FileUtils.getDistributionFile;
import static org.junit.Assume.assumeTrue;

public class DebMetadataTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only deb", distribution.packaging == Distribution.Packaging.DEB);
    }

    public void test05CheckLintian() {
        String extraArgs = "";
        if (sh.run("lintian --help").stdout.contains("fail-on-warnings")) {
            extraArgs = "--fail-on-warnings ";
        }
        sh.run("lintian " + extraArgs + FileUtils.getDistributionFile(distribution()));
    }

    public void test06Dependencies() {

        final Shell sh = new Shell();

        final Shell.Result result = sh.run("dpkg -I " + getDistributionFile(distribution()));

        TestCase.assertTrue(Pattern.compile("(?m)^ Depends:.*bash.*").matcher(result.stdout).find());
    }
}
