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

package org.havenask.qa.die_with_dignity;

import org.havenask.client.Request;
import org.havenask.common.io.PathUtils;
import org.havenask.common.settings.Settings;
import org.havenask.test.rest.HavenaskRestTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class DieWithDignityIT extends HavenaskRestTestCase {
    public void testDieWithDignity() throws Exception {
        expectThrows(
            IOException.class,
            () -> client().performRequest(new Request("GET", "/_die_with_dignity"))
        );

        // the Havenask process should die and disappear from the output of jps
        assertBusy(() -> {
            final String jpsPath = PathUtils.get(System.getProperty("runtime.java.home"), "bin/jps").toString();
            final Process process = new ProcessBuilder().command(jpsPath, "-v").start();

            try (InputStream is = process.getInputStream();
                 BufferedReader in = new BufferedReader(new InputStreamReader(is, "UTF-8"))) {
                String line;
                while ((line = in.readLine()) != null) {
                    assertThat(line, line, not(containsString("-Ddie.with.dignity.test")));
                }
            }
        });

        // parse the logs and ensure that Havenask died with the expected cause
        final List<String> lines = Files.readAllLines(PathUtils.get(System.getProperty("log")));

        final Iterator<String> it = lines.iterator();

        boolean fatalError = false;
        boolean fatalErrorInThreadExiting = false;
        try {
            while (it.hasNext() && (fatalError == false || fatalErrorInThreadExiting == false)) {
                final String line = it.next();
                if (line.matches(".*ERROR.*o\\.h\\.ExceptionsHelper.*javaRestTest-0.*fatal error.*")) {
                    fatalError = true;
                } else if (line.matches(".*ERROR.*o\\.h\\.b\\.HavenaskUncaughtExceptionHandler.*javaRestTest-0.*"
                    + "fatal error in thread \\[Thread-\\d+\\], exiting.*")) {
                    fatalErrorInThreadExiting = true;
                    assertTrue(it.hasNext());
                    assertThat(it.next(), containsString("java.lang.OutOfMemoryError: die with dignity"));
                }
            }

            assertTrue(fatalError);
            assertTrue(fatalErrorInThreadExiting);

        } catch (AssertionError ae) {
            Path path = PathUtils.get(System.getProperty("log"));
            debugLogs(path);
            throw ae;
        }
    }

    private boolean containsAll(String line, String... subStrings) {
        for (String subString : subStrings) {
            if (line.matches(subString) == false) {
                return false;
            }
        }
        return true;
    }

    private void debugLogs(Path path) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            reader.lines().forEach(line -> logger.info(line));
        }
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // as the cluster is dead its state can not be wiped successfully so we have to bypass wiping the cluster
        return true;
    }

    @Override
    protected final Settings restClientSettings() {
        return Settings.builder().put(super.restClientSettings())
            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(HavenaskRestTestCase.CLIENT_SOCKET_TIMEOUT, "1s")
            .build();
    }

}
