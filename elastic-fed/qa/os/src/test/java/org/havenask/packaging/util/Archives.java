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

package org.havenask.packaging.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.havenask.packaging.util.FileExistenceMatchers.fileDoesNotExist;
import static org.havenask.packaging.util.FileExistenceMatchers.fileExists;
import static org.havenask.packaging.util.FileMatcher.Fileness.Directory;
import static org.havenask.packaging.util.FileMatcher.Fileness.File;
import static org.havenask.packaging.util.FileMatcher.file;
import static org.havenask.packaging.util.FileMatcher.p644;
import static org.havenask.packaging.util.FileMatcher.p660;
import static org.havenask.packaging.util.FileMatcher.p755;
import static org.havenask.packaging.util.FileUtils.getCurrentVersion;
import static org.havenask.packaging.util.FileUtils.getDefaultArchiveInstallPath;
import static org.havenask.packaging.util.FileUtils.getDistributionFile;
import static org.havenask.packaging.util.FileUtils.lsGlob;
import static org.havenask.packaging.util.FileUtils.mv;
import static org.havenask.packaging.util.FileUtils.slurp;
import static org.havenask.packaging.util.Platforms.isDPKG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

/**
 * Installation and verification logic for archive distributions
 */
public class Archives {

    protected static final Logger logger = LogManager.getLogger(Archives.class);

    // in the future we'll run as a role user on Windows
    public static final String ARCHIVE_OWNER = Platforms.WINDOWS ? System.getenv("username") : "havenask";

    /** This is an arbitrarily chosen value that gives Havenask time to log Bootstrap
     *  errors to the console if they occur before the logging framework is initialized. */
    public static final String HAVENASK_STARTUP_SLEEP_TIME_SECONDS = "10";

    public static Installation installArchive(Shell sh, Distribution distribution) throws Exception {
        return installArchive(sh, distribution, getDefaultArchiveInstallPath(), getCurrentVersion());
    }

    public static Installation installArchive(Shell sh, Distribution distribution, Path fullInstallPath, String version) throws Exception {
        final Path distributionFile = getDistributionFile(distribution);
        final Path baseInstallPath = fullInstallPath.getParent();
        final Path extractedPath = baseInstallPath.resolve("havenask-" + version);

        assertThat("distribution file must exist: " + distributionFile.toString(), Files.exists(distributionFile), is(true));
        assertThat("havenask must not already be installed", lsGlob(baseInstallPath, "havenask*"), empty());

        logger.info("Installing file: " + distributionFile);
        final String installCommand;
        if (distribution.packaging == Distribution.Packaging.TAR) {
            if (Platforms.WINDOWS) {
                throw new IllegalStateException("Distribution " + distribution + " is not supported on windows");
            }
            installCommand = "tar -C " + baseInstallPath + " -xzpf " + distributionFile;

        } else if (distribution.packaging == Distribution.Packaging.ZIP) {
            if (Platforms.WINDOWS == false) {
                throw new IllegalStateException("Distribution " + distribution + " is not supported on linux");
            }
            installCommand = String.format(
                Locale.ROOT,
                "Add-Type -AssemblyName 'System.IO.Compression.Filesystem'; [IO.Compression.ZipFile]::ExtractToDirectory('%s', '%s')",
                distributionFile,
                baseInstallPath
            );

        } else {
            throw new RuntimeException("Distribution " + distribution + " is not a known archive type");
        }

        sh.run(installCommand);
        assertThat("archive was extracted", Files.exists(extractedPath), is(true));

        mv(extractedPath, fullInstallPath);

        assertThat("extracted archive moved to install location", Files.exists(fullInstallPath));
        final List<Path> installations = lsGlob(baseInstallPath, "havenask*");
        assertThat("only the intended installation exists", installations, hasSize(1));
        assertThat("only the intended installation exists", installations.get(0), is(fullInstallPath));

        Platforms.onLinux(() -> setupArchiveUsersLinux(fullInstallPath));

        sh.chown(fullInstallPath);

        return Installation.ofArchive(sh, distribution, fullInstallPath);
    }

    private static void setupArchiveUsersLinux(Path installPath) {
        final Shell sh = new Shell();

        if (sh.runIgnoreExitCode("getent group havenask").isSuccess() == false) {
            if (isDPKG()) {
                sh.run("addgroup --system havenask");
            } else {
                sh.run("groupadd -r havenask");
            }
        }

        if (sh.runIgnoreExitCode("id havenask").isSuccess() == false) {
            if (isDPKG()) {
                sh.run(
                    "adduser "
                        + "--quiet "
                        + "--system "
                        + "--no-create-home "
                        + "--ingroup havenask "
                        + "--disabled-password "
                        + "--shell /bin/false "
                        + "havenask"
                );
            } else {
                sh.run(
                    "useradd "
                        + "--system "
                        + "-M "
                        + "--gid havenask "
                        + "--shell /sbin/nologin "
                        + "--comment 'havenask user' "
                        + "havenask"
                );
            }
        }
    }

    public static void verifyArchiveInstallation(Installation installation, Distribution distribution) {
        verifyInstallation(installation, distribution, ARCHIVE_OWNER);
    }

    private static void verifyInstallation(Installation es, Distribution distribution, String owner) {
        Stream.of(es.home, es.config, es.plugins, es.modules, es.logs).forEach(dir -> assertThat(dir, file(Directory, owner, owner, p755)));

        assertThat(Files.exists(es.data), is(false));

        assertThat(es.bin, file(Directory, owner, owner, p755));
        assertThat(es.lib, file(Directory, owner, owner, p755));
        assertThat(Files.exists(es.config("havenask.keystore")), is(false));

        Stream.of("havenask", "havenask-env", "havenask-keystore", "havenask-plugin", "havenask-shard", "havenask-node")
            .forEach(executable -> {

                assertThat(es.bin(executable), file(File, owner, owner, p755));

                if (distribution.packaging == Distribution.Packaging.ZIP) {
                    assertThat(es.bin(executable + ".bat"), file(File, owner));
                }
            });

        if (distribution.packaging == Distribution.Packaging.ZIP) {
            Stream.of("havenask-service.bat", "havenask-service-mgr.exe", "havenask-service-x64.exe")
                .forEach(executable -> assertThat(es.bin(executable), file(File, owner)));
        }

        Stream.of("havenask.yml", "jvm.options", "log4j2.properties")
            .forEach(configFile -> assertThat(es.config(configFile), file(File, owner, owner, p660)));

        Stream.of("NOTICE.txt", "LICENSE.txt", "README.md")
            .forEach(doc -> assertThat(es.home.resolve(doc), file(File, owner, owner, p644)));
    }

    public static Shell.Result startHavenask(Installation installation, Shell sh) {
        return runHavenaskStartCommand(installation, sh, null, true);
    }

    public static Shell.Result startHavenaskWithTty(Installation installation, Shell sh, String keystorePassword, boolean daemonize)
        throws Exception {
        final Path pidFile = installation.home.resolve("havenask.pid");
        final Installation.Executables bin = installation.executables();

        List<String> command = new ArrayList<>();
        command.add("sudo -E -u %s %s -p %s");

        // requires the "expect" utility to be installed
        // TODO: daemonization isn't working with expect versions prior to 5.45, but centos-6 has 5.45.1.15
        // TODO: try using pty4j to make daemonization work
        if (daemonize) {
            command.add("-d");
        }

        String script = String.format(
            Locale.ROOT,
            "expect -c \"$(cat<<EXPECT\n"
                + "spawn -ignore HUP "
                + String.join(" ", command)
                + "\n"
                + "expect \"Havenask keystore password:\"\n"
                + "send \"%s\\r\"\n"
                + "expect eof\n"
                + "EXPECT\n"
                + ")\"",
            ARCHIVE_OWNER,
            bin.havenask,
            pidFile,
            keystorePassword
        );

        sh.getEnv().put("HAVENASK_STARTUP_SLEEP_TIME", HAVENASK_STARTUP_SLEEP_TIME_SECONDS);
        return sh.runIgnoreExitCode(script);
    }

    public static Shell.Result runHavenaskStartCommand(Installation installation, Shell sh, String keystorePassword, boolean daemonize) {
        final Path pidFile = installation.home.resolve("havenask.pid");

        assertThat(pidFile, fileDoesNotExist());

        final Installation.Executables bin = installation.executables();

        if (Platforms.WINDOWS == false) {
            // If jayatana is installed then we try to use it. Havenask should ignore it even when we try.
            // If it doesn't ignore it then Havenask will fail to start because of security errors.
            // This line is attempting to emulate the on login behavior of /usr/share/upstart/sessions/jayatana.conf
            if (Files.exists(Paths.get("/usr/share/java/jayatanaag.jar"))) {
                sh.getEnv().put("JAVA_TOOL_OPTIONS", "-javaagent:/usr/share/java/jayatanaag.jar");
            }

            // We need to give Havenask enough time to print failures to stderr before exiting
            sh.getEnv().put("HAVENASK_STARTUP_SLEEP_TIME", HAVENASK_STARTUP_SLEEP_TIME_SECONDS);

            List<String> command = new ArrayList<>();
            command.add("sudo -E -u ");
            command.add(ARCHIVE_OWNER);
            command.add(bin.havenask.toString());
            if (daemonize) {
                command.add("-d");
            }
            command.add("-p");
            command.add(pidFile.toString());
            if (keystorePassword != null) {
                command.add("<<<'" + keystorePassword + "'");
            }
            return sh.runIgnoreExitCode(String.join(" ", command));
        }

        if (daemonize) {
            final Path stdout = getPowershellOutputPath(installation);
            final Path stderr = getPowershellErrorPath(installation);

            String powerShellProcessUserSetup;
            if (System.getenv("username").equals("vagrant")) {
                // the tests will run as Administrator in vagrant.
                // we don't want to run the server as Administrator, so we provide the current user's
                // username and password to the process which has the effect of starting it not as Administrator.
                powerShellProcessUserSetup = "$password = ConvertTo-SecureString 'vagrant' -AsPlainText -Force; "
                    + "$processInfo.Username = 'vagrant'; "
                    + "$processInfo.Password = $password; ";
            } else {
                powerShellProcessUserSetup = "";
            }
            // this starts the server in the background. the -d flag is unsupported on windows
            return sh.run(
                "$processInfo = New-Object System.Diagnostics.ProcessStartInfo; "
                    + "$processInfo.FileName = '"
                    + bin.havenask
                    + "'; "
                    + "$processInfo.Arguments = '-p "
                    + installation.home.resolve("havenask.pid")
                    + "'; "
                    + powerShellProcessUserSetup
                    + "$processInfo.RedirectStandardOutput = $true; "
                    + "$processInfo.RedirectStandardError = $true; "
                    + "$processInfo.RedirectStandardInput = $true; "
                    + sh.env.entrySet()
                        .stream()
                        .map(entry -> "$processInfo.Environment.Add('" + entry.getKey() + "', '" + entry.getValue() + "'); ")
                        .collect(joining())
                    + "$processInfo.UseShellExecute = $false; "
                    + "$process = New-Object System.Diagnostics.Process; "
                    + "$process.StartInfo = $processInfo; "
                    +

                    // set up some asynchronous output handlers
                    "$outScript = { $EventArgs.Data | Out-File -Encoding UTF8 -Append '"
                    + stdout
                    + "' }; "
                    + "$errScript = { $EventArgs.Data | Out-File -Encoding UTF8 -Append '"
                    + stderr
                    + "' }; "
                    + "$stdOutEvent = Register-ObjectEvent -InputObject $process "
                    + "-Action $outScript -EventName 'OutputDataReceived'; "
                    + "$stdErrEvent = Register-ObjectEvent -InputObject $process "
                    + "-Action $errScript -EventName 'ErrorDataReceived'; "
                    +

                    "$process.Start() | Out-Null; "
                    + "$process.BeginOutputReadLine(); "
                    + "$process.BeginErrorReadLine(); "
                    + "$process.StandardInput.WriteLine('"
                    + keystorePassword
                    + "'); "
                    + "Wait-Process -Timeout "
                    + HAVENASK_STARTUP_SLEEP_TIME_SECONDS
                    + " -Id $process.Id; "
                    + "$process.Id;"
            );
        } else {
            List<String> command = new ArrayList<>();
            if (keystorePassword != null) {
                command.add("echo '" + keystorePassword + "' |");
            }
            command.add(bin.havenask.toString());
            command.add("-p");
            command.add(installation.home.resolve("havenask.pid").toString());
            return sh.runIgnoreExitCode(String.join(" ", command));
        }
    }

    public static void assertHavenaskStarted(Installation installation) throws Exception {
        final Path pidFile = installation.home.resolve("havenask.pid");
        ServerUtils.waitForHavenask(installation);

        assertThat("Starting Havenask produced a pid file at " + pidFile, pidFile, fileExists());
        String pid = slurp(pidFile).trim();
        assertThat(pid, is(not(emptyOrNullString())));
    }

    public static void stopHavenask(Installation installation) throws Exception {
        Path pidFile = installation.home.resolve("havenask.pid");
        assertThat(pidFile, fileExists());
        String pid = slurp(pidFile).trim();
        assertThat(pid, is(not(emptyOrNullString())));

        final Shell sh = new Shell();
        Platforms.onLinux(() -> sh.run("kill -SIGTERM " + pid + "; tail --pid=" + pid + " -f /dev/null"));
        Platforms.onWindows(() -> {
            sh.run("Get-Process -Id " + pid + " | Stop-Process -Force; Wait-Process -Id " + pid);

            // Clear the asynchronous event handlers
            sh.runIgnoreExitCode(
                "Get-EventSubscriber | "
                    + "where {($_.EventName -eq 'OutputDataReceived' -Or $_.EventName -eq 'ErrorDataReceived' |"
                    + "Unregister-EventSubscriber -Force"
            );
        });
        if (Files.exists(pidFile)) {
            Files.delete(pidFile);
        }
    }

    public static Path getPowershellErrorPath(Installation installation) {
        return installation.logs.resolve("output.err");
    }

    private static Path getPowershellOutputPath(Installation installation) {
        return installation.logs.resolve("output.out");
    }

}
