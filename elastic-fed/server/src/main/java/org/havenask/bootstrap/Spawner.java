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

package org.havenask.bootstrap;

import org.apache.lucene.util.Constants;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.env.Environment;
import org.havenask.plugins.Platforms;
import org.havenask.plugins.PluginInfo;
import org.havenask.plugins.PluginsService;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Spawns native module controller processes if present. Will only work prior to a system call filter being installed.
 */
final class Spawner implements Closeable {

    /*
     * References to the processes that have been spawned, so that we can destroy them.
     */
    private final List<Process> processes = new ArrayList<>();
    private AtomicBoolean spawned = new AtomicBoolean();

    @Override
    public void close() throws IOException {
        IOUtils.close(() -> processes.stream().map(s -> (Closeable) s::destroy).iterator());
    }

    /**
     * Spawns the native controllers for each module.
     *
     * @param environment The node environment
     * @param inheritIo   Should the stdout and stderr of the spawned process inherit the
     *                    stdout and stderr of the JVM spawning it?
     * @throws IOException if an I/O error occurs reading the module or spawning a native process
     */
    void spawnNativeControllers(final Environment environment, final boolean inheritIo) throws IOException {
        if (!spawned.compareAndSet(false, true)) {
            throw new IllegalStateException("native controllers already spawned");
        }
        if (!Files.exists(environment.modulesFile())) {
            throw new IllegalStateException("modules directory [" + environment.modulesFile() + "] not found");
        }
        /*
         * For each module, attempt to spawn the controller daemon. Silently ignore any module that doesn't include a controller for the
         * correct platform.
         */
        List<Path> paths = PluginsService.findPluginDirs(environment.modulesFile());
        for (final Path modules : paths) {
            final PluginInfo info = PluginInfo.readFromProperties(modules);
            final Path spawnPath = Platforms.nativeControllerPath(modules);
            if (!Files.isRegularFile(spawnPath)) {
                continue;
            }
            if (!info.hasNativeController()) {
                final String message = String.format(
                    Locale.ROOT,
                    "module [%s] does not have permission to fork native controller",
                    modules.getFileName());
                throw new IllegalArgumentException(message);
            }
            final Process process = spawnNativeController(spawnPath, environment.tmpFile(), inheritIo);
            processes.add(process);
        }
    }

    /**
     * Attempt to spawn the controller daemon for a given module. The spawned process will remain connected to this JVM via its stdin,
     * stdout, and stderr streams, but the references to these streams are not available to code outside this package.
     */
    private Process spawnNativeController(final Path spawnPath, final Path tmpPath, final boolean inheritIo) throws IOException {
        final String command;
        if (Constants.WINDOWS) {
            /*
             * We have to get the short path name or starting the process could fail due to max path limitations. The underlying issue here
             * is that starting the process on Windows ultimately involves the use of CreateProcessW. CreateProcessW has a limitation that
             * if its first argument (the application name) is null, then its second argument (the command line for the process to start) is
             * restricted in length to 260 characters (cf. https://msdn.microsoft.com/en-us/library/windows/desktop/ms682425.aspx). Since
             * this is exactly how the JDK starts the process on Windows (cf.
             * http://hg.openjdk.java.net/jdk8/jdk8/jdk/file/687fd7c7986d/src/windows/native/java/lang/ProcessImpl_md.c#l319), this
             * limitation is in force. As such, we use the short name to avoid any such problems.
             */
            command = Natives.getShortPathName(spawnPath.toString());
        } else {
            command = spawnPath.toString();
        }
        final ProcessBuilder pb = new ProcessBuilder(command);

        // the only environment variable passes on the path to the temporary directory
        pb.environment().clear();
        pb.environment().put("TMPDIR", tmpPath.toString());

        // The process _shouldn't_ write any output via its stdout or stderr, but if it does then
        // it will block if nothing is reading that output. To avoid this we can inherit the
        // JVM's stdout and stderr (which are redirected to files in standard installations).
        if (inheritIo) {
            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        }

        // the output stream of the process object corresponds to the daemon's stdin
        return pb.start();
    }

    /**
     * The collection of processes representing spawned native controllers.
     *
     * @return the processes
     */
    List<Process> getProcesses() {
        return Collections.unmodifiableList(processes);
    }

}
