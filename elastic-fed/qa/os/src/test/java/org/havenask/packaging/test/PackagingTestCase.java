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

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;
import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.annotations.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.common.CheckedConsumer;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.packaging.util.Archives;
import org.havenask.packaging.util.Distribution;
import org.havenask.packaging.util.Docker;
import org.havenask.packaging.util.FileUtils;
import org.havenask.packaging.util.Installation;
import org.havenask.packaging.util.Packages;
import org.havenask.packaging.util.Platforms;
import org.havenask.packaging.util.Shell;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.List;

import static org.havenask.packaging.util.Cleanup.cleanEverything;
import static org.havenask.packaging.util.Docker.ensureImageIsLoaded;
import static org.havenask.packaging.util.Docker.removeContainer;
import static org.havenask.packaging.util.FileExistenceMatchers.fileExists;
import static org.havenask.packaging.util.FileUtils.append;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Class that all packaging test cases should inherit from
 */
@RunWith(RandomizedRunner.class)
@TestMethodProviders({ JUnit3MethodProvider.class })
@Timeout(millis = 20 * 60 * 1000) // 20 min
@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public abstract class PackagingTestCase extends Assert {

    /**
     * Annotation for tests which exhibit a known issue and are temporarily disabled.
     */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @TestGroup(enabled = false, sysProperty = "tests.awaitsfix")
    @interface AwaitsFix {
        /** Point to JIRA entry. */
        String bugUrl();
    }

    protected final Logger logger = LogManager.getLogger(getClass());

    // the distribution being tested
    protected static final Distribution distribution;
    static {
        distribution = new Distribution(Paths.get(System.getProperty("tests.distribution")));
    }

    // the java installation already installed on the system
    protected static final String systemJavaHome;
    static {
        Shell sh = new Shell();
        if (Platforms.WINDOWS) {
            systemJavaHome = sh.run("$Env:SYSTEM_JAVA_HOME").stdout.trim();
        } else {
            assert Platforms.LINUX || Platforms.DARWIN;
            systemJavaHome = sh.run("echo $SYSTEM_JAVA_HOME").stdout.trim();
        }
    }

    // the current installation of the distribution being tested
    protected static Installation installation;

    private static boolean failed;

    @Rule
    public final TestWatcher testFailureRule = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            failed = true;
        }
    };

    // a shell to run system commands with
    protected static Shell sh;

    @Rule
    public final TestName testNameRule = new TestName();

    @BeforeClass
    public static void init() throws Exception {
        assumeTrue("only compatible distributions", distribution.packaging.compatible);

        // make sure temp dir exists
        if (Files.exists(getRootTempDir()) == false) {
            Files.createDirectories(getRootTempDir());
        }

        // cleanup from previous test
        cleanup();

        // create shell
        if (distribution().isDocker()) {
            ensureImageIsLoaded(distribution);
            sh = new Docker.DockerShell();
        } else {
            sh = new Shell();
        }
    }

    @AfterClass
    public static void cleanupDocker() {
        if (distribution().isDocker()) {
            // runContainer also calls this, so we don't need this method to be annotated as `@After`
            removeContainer();
        }
    }

    @Before
    public void setup() throws Exception {
        assumeFalse(failed); // skip rest of tests once one fails

        sh.reset();
        if (distribution().hasJdk == false) {
            Platforms.onLinux(() -> sh.getEnv().put("JAVA_HOME", systemJavaHome));
            Platforms.onWindows(() -> sh.getEnv().put("JAVA_HOME", systemJavaHome));
        }
    }

    @After
    public void teardown() throws Exception {
        if (installation != null && failed == false) {
            if (Files.exists(installation.logs)) {
                Path logFile = installation.logs.resolve("havenask.log");
                if (Files.exists(logFile)) {
                    logger.warn("Havenask log:\n" + FileUtils.slurpAllLogs(installation.logs, "havenask.log", "*.log.gz"));
                }

                // move log file so we can avoid false positives when grepping for
                // messages in logs during test
                String prefix = this.getClass().getSimpleName() + "." + testNameRule.getMethodName();
                if (Files.exists(logFile)) {
                    Path newFile = installation.logs.resolve(prefix + ".havenask.log");
                    FileUtils.mv(logFile, newFile);
                }
                for (Path rotatedLogFile : FileUtils.lsGlob(installation.logs, "havenask*.tar.gz")) {
                    Path newRotatedLogFile = installation.logs.resolve(prefix + "." + rotatedLogFile.getFileName());
                    FileUtils.mv(rotatedLogFile, newRotatedLogFile);
                }
            }
            if (Files.exists(Archives.getPowershellErrorPath(installation))) {
                FileUtils.rmWithRetries(Archives.getPowershellErrorPath(installation));
            }
        }

    }

    /** The {@link Distribution} that should be tested in this case */
    protected static Distribution distribution() {
        return distribution;
    }

    protected static void install() throws Exception {
        switch (distribution.packaging) {
            case TAR:
            case ZIP:
                installation = Archives.installArchive(sh, distribution);
                Archives.verifyArchiveInstallation(installation, distribution);
                break;
            case DEB:
            case RPM:
                installation = Packages.installPackage(sh, distribution);
                Packages.verifyPackageInstallation(installation, distribution, sh);
                break;
            case DOCKER:
                installation = Docker.runContainer(distribution);
                Docker.verifyContainerInstallation(installation, distribution);
                break;
            default:
                throw new IllegalStateException("Unknown Havenask packaging type.");
        }
    }

    protected static void cleanup() throws Exception {
        installation = null;
        cleanEverything();
    }

    /**
     * Starts and stops havenask, and performs assertions while it is running.
     */
    protected void assertWhileRunning(Platforms.PlatformAction assertions) throws Exception {
        try {
            awaitHavenaskStartup(runHavenaskStartCommand(null, true, false));
        } catch (Exception e) {
            if (Files.exists(installation.home.resolve("havenask.pid"))) {
                String pid = FileUtils.slurp(installation.home.resolve("havenask.pid")).trim();
                logger.info("Dumping jstack of havenask processb ({}) that failed to start", pid);
                sh.runIgnoreExitCode("jstack " + pid);
            }
            if (Files.exists(installation.logs.resolve("havenask.log"))) {
                logger.warn("Havenask log:\n" + FileUtils.slurpAllLogs(installation.logs, "havenask.log", "*.log.gz"));
            }
            if (Files.exists(installation.logs.resolve("output.out"))) {
                logger.warn("Stdout:\n" + FileUtils.slurpTxtorGz(installation.logs.resolve("output.out")));
            }
            if (Files.exists(installation.logs.resolve("output.err"))) {
                logger.warn("Stderr:\n" + FileUtils.slurpTxtorGz(installation.logs.resolve("output.err")));
            }
            throw e;
        }

        try {
            assertions.run();
        } catch (Exception e) {
            logger.warn("Havenask log:\n" + FileUtils.slurpAllLogs(installation.logs, "havenask.log", "*.log.gz"));
            throw e;
        }
        stopHavenask();
    }

    /**
     * Run the command to start Havenask, but don't wait or test for success.
     * This method is useful for testing failure conditions in startup. To await success,
     * use {@link #startHavenask()}.
     * @param password Password for password-protected keystore, null for no password;
     *                 this option will fail for non-archive distributions
     * @param daemonize Run Havenask in the background
     * @param useTty Use a tty for inputting the password rather than standard input;
     *               this option will fail for non-archive distributions
     * @return Shell results of the startup command.
     * @throws Exception when command fails immediately.
     */
    public Shell.Result runHavenaskStartCommand(String password, boolean daemonize, boolean useTty) throws Exception {
        if (password != null) {
            assertTrue("Only archives support user-entered passwords", distribution().isArchive());
        }

        switch (distribution.packaging) {
            case TAR:
            case ZIP:
                if (useTty) {
                    return Archives.startHavenaskWithTty(installation, sh, password, daemonize);
                } else {
                    return Archives.runHavenaskStartCommand(installation, sh, password, daemonize);
                }
            case DEB:
            case RPM:
                return Packages.runHavenaskStartCommand(sh);
            case DOCKER:
                // nothing, "installing" docker image is running it
                return Shell.NO_OP;
            default:
                throw new IllegalStateException("Unknown Havenask packaging type.");
        }
    }

    public void stopHavenask() throws Exception {
        switch (distribution.packaging) {
            case TAR:
            case ZIP:
                Archives.stopHavenask(installation);
                break;
            case DEB:
            case RPM:
                Packages.stopHavenask(sh);
                break;
            case DOCKER:
                // nothing, "installing" docker image is running it
                break;
            default:
                throw new IllegalStateException("Unknown Havenask packaging type.");
        }
    }

    public void awaitHavenaskStartup(Shell.Result result) throws Exception {
        assertThat("Startup command should succeed", result.exitCode, equalTo(0));
        switch (distribution.packaging) {
            case TAR:
            case ZIP:
                Archives.assertHavenaskStarted(installation);
                break;
            case DEB:
            case RPM:
                Packages.assertHavenaskStarted(sh, installation);
                break;
            case DOCKER:
                Docker.waitForHavenaskToStart();
                break;
            default:
                throw new IllegalStateException("Unknown Havenask packaging type.");
        }
    }

    /**
     * Start Havenask and wait until it's up and running. If you just want to run
     * the start command, use {@link #runHavenaskStartCommand(String, boolean, boolean)}.
     * @throws Exception if Havenask can't start
     */
    public void startHavenask() throws Exception {
        awaitHavenaskStartup(runHavenaskStartCommand(null, true, false));
    }

    public void assertHavenaskFailure(Shell.Result result, String expectedMessage, Packages.JournaldWrapper journaldWrapper) {
        assertHavenaskFailure(result, Collections.singletonList(expectedMessage), journaldWrapper);
    }

    public void assertHavenaskFailure(Shell.Result result, List<String> expectedMessages, Packages.JournaldWrapper journaldWrapper) {
        @SuppressWarnings("unchecked")
        Matcher<String>[] stringMatchers = expectedMessages.stream().map(CoreMatchers::containsString).toArray(Matcher[]::new);
        if (Files.exists(installation.logs.resolve("havenask.log"))) {

            // If log file exists, then we have bootstrapped our logging and the
            // error should be in the logs
            assertThat(installation.logs.resolve("havenask.log"), fileExists());
            String logfile = FileUtils.slurp(installation.logs.resolve("havenask.log"));

            assertThat(logfile, anyOf(stringMatchers));

        } else if (distribution().isPackage() && Platforms.isSystemd()) {

            // For systemd, retrieve the error from journalctl
            assertThat(result.stderr, containsString("Job for havenask.service failed"));
            Shell.Result error = journaldWrapper.getLogs();
            assertThat(error.stdout, anyOf(stringMatchers));

        } else if (Platforms.WINDOWS && Files.exists(Archives.getPowershellErrorPath(installation))) {

            // In Windows, we have written our stdout and stderr to files in order to run
            // in the background
            String wrapperPid = result.stdout.trim();
            sh.runIgnoreExitCode("Wait-Process -Timeout " + Archives.HAVENASK_STARTUP_SLEEP_TIME_SECONDS + " -Id " + wrapperPid);
            sh.runIgnoreExitCode(
                "Get-EventSubscriber | "
                    + "where {($_.EventName -eq 'OutputDataReceived' -Or $_.EventName -eq 'ErrorDataReceived' |"
                    + "Unregister-EventSubscriber -Force"
            );
            assertThat(FileUtils.slurp(Archives.getPowershellErrorPath(installation)), anyOf(stringMatchers));

        } else {

            // Otherwise, error should be on shell stderr
            assertThat(result.stderr, anyOf(stringMatchers));
        }
    }

    public static Path getRootTempDir() {
        if (distribution().isPackage()) {
            // The custom config directory is not under /tmp or /var/tmp because
            // systemd's private temp directory functionally means different
            // processes can have different views of what's in these directories
            return Paths.get("/var/test-tmp").toAbsolutePath();
        } else {
            // vagrant creates /tmp for us in windows so we use that to avoid long paths
            return Paths.get("/tmp").toAbsolutePath();
        }
    }

    private static final FileAttribute<?>[] NEW_DIR_PERMS;
    static {
        if (Platforms.WINDOWS) {
            NEW_DIR_PERMS = new FileAttribute<?>[0];
        } else {
            NEW_DIR_PERMS = new FileAttribute<?>[] { PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x")) };
        }
    }

    public static Path createTempDir(String prefix) throws IOException {
        return Files.createTempDirectory(getRootTempDir(), prefix, NEW_DIR_PERMS);
    }

    /**
     * Run the given action with a temporary copy of the config directory.
     *
     * Files under the path passed to the action may be modified as necessary for the
     * test to execute, and running Havenask with {@link #startHavenask()} will
     * use the temporary directory.
     */
    public void withCustomConfig(CheckedConsumer<Path, Exception> action) throws Exception {
        Path tempDir = createTempDir("custom-config");
        Path tempConf = tempDir.resolve("havenask");
        FileUtils.copyDirectory(installation.config, tempConf);

        Platforms.onLinux(() -> sh.run("chown -R havenask:havenask " + tempDir));

        if (distribution.isPackage()) {
            Files.copy(installation.envFile, tempDir.resolve("havenask.bk"));// backup
            append(installation.envFile, "HAVENASK_PATH_CONF=" + tempConf + "\n");
        } else {
            sh.getEnv().put("HAVENASK_PATH_CONF", tempConf.toString());
        }

        action.accept(tempConf);
        if (distribution.isPackage()) {
            IOUtils.rm(installation.envFile);
            Files.copy(tempDir.resolve("havenask.bk"), installation.envFile);
        } else {
            sh.getEnv().remove("HAVENASK_PATH_CONF");
        }
        IOUtils.rm(tempDir);
    }
}
