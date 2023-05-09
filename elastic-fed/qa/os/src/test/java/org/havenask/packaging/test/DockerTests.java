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

import com.fasterxml.jackson.databind.JsonNode;
import org.havenask.packaging.util.Installation;
import org.havenask.packaging.util.Platforms;
import org.havenask.packaging.util.ServerUtils;
import org.havenask.packaging.util.Shell;
import org.havenask.packaging.util.Shell.Result;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static java.util.Collections.singletonMap;
import static org.havenask.packaging.util.Docker.chownWithPrivilegeEscalation;
import static org.havenask.packaging.util.Docker.copyFromContainer;
import static org.havenask.packaging.util.Docker.existsInContainer;
import static org.havenask.packaging.util.Docker.getContainerLogs;
import static org.havenask.packaging.util.Docker.getImageLabels;
import static org.havenask.packaging.util.Docker.getImageName;
import static org.havenask.packaging.util.Docker.getJson;
import static org.havenask.packaging.util.Docker.mkDirWithPrivilegeEscalation;
import static org.havenask.packaging.util.Docker.removeContainer;
import static org.havenask.packaging.util.Docker.rmDirWithPrivilegeEscalation;
import static org.havenask.packaging.util.Docker.runContainer;
import static org.havenask.packaging.util.Docker.runContainerExpectingFailure;
import static org.havenask.packaging.util.Docker.verifyContainerInstallation;
import static org.havenask.packaging.util.Docker.waitForHavenask;
import static org.havenask.packaging.util.FileMatcher.p600;
import static org.havenask.packaging.util.FileMatcher.p644;
import static org.havenask.packaging.util.FileMatcher.p660;
import static org.havenask.packaging.util.FileUtils.append;
import static org.havenask.packaging.util.FileUtils.rm;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;

public class DockerTests extends PackagingTestCase {
    private Path tempDir;

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only Docker", distribution().isDocker());
    }

    @Before
    public void setupTest() throws IOException {
        installation = runContainer(distribution());
        tempDir = createTempDir(DockerTests.class.getSimpleName());
    }

    @After
    public void teardownTest() {
        removeContainer();
        rm(tempDir);
    }

    /**
     * Checks that the Docker image can be run, and that it passes various checks.
     */
    public void test010Install() {
        verifyContainerInstallation(installation, distribution());
    }

    /**
     * Checks that no plugins are initially active.
     */
    public void test020PluginsListWithNoPlugins() {
        final Installation.Executables bin = installation.executables();
        final Result r = sh.run(bin.pluginTool + " list");

        assertThat("Expected no plugins to be listed", r.stdout, emptyString());
    }

    /**
     * Check that the JDK's cacerts file is a symlink to the copy provided by the operating system.
     */
    public void test040JavaUsesTheOsProvidedKeystore() {
        final String path = sh.run("realpath jdk/lib/security/cacerts").stdout;

        assertThat(path, equalTo("/etc/pki/ca-trust/extracted/java/cacerts"));
    }

    /**
     * Checks that there are Amazon trusted certificates in the cacaerts keystore.
     */
    public void test041AmazonCaCertsAreInTheKeystore() {
        final boolean matches = Arrays.stream(
            sh.run("jdk/bin/keytool -cacerts -storepass changeit -list | grep trustedCertEntry").stdout.split("\n")
        ).anyMatch(line -> line.contains("amazonrootca"));

        assertTrue("Expected Amazon trusted cert in cacerts", matches);
    }

    /**
     * Send some basic index, count and delete requests, in order to check that the installation
     * is minimally functional.
     */
    public void test050BasicApiTests() throws Exception {
        waitForHavenask(installation);

        assertTrue(existsInContainer(installation.logs.resolve("gc.log")));

        ServerUtils.runHavenaskTests();
    }

    /**
     * Check that the default config can be overridden using a bind mount, and that env vars are respected
     */
    public void test070BindMountCustomPathConfAndJvmOptions() throws Exception {
        copyFromContainer(installation.config("havenask.yml"), tempDir.resolve("havenask.yml"));
        copyFromContainer(installation.config("log4j2.properties"), tempDir.resolve("log4j2.properties"));

        // we have to disable Log4j from using JMX lest it will hit a security
        // manager exception before we have configured logging; this will fail
        // startup since we detect usages of logging before it is configured
        final String jvmOptions = "-Xms512m\n-Xmx512m\n-Dlog4j2.disable.jmx=true\n";
        append(tempDir.resolve("jvm.options"), jvmOptions);

        // Make the temp directory and contents accessible when bind-mounted.
        Files.setPosixFilePermissions(tempDir, fromString("rwxrwxrwx"));
        // These permissions are necessary to run the tests under Vagrant
        Files.setPosixFilePermissions(tempDir.resolve("havenask.yml"), p644);
        Files.setPosixFilePermissions(tempDir.resolve("log4j2.properties"), p644);

        // Restart the container
        final Map<Path, Path> volumes = singletonMap(tempDir, Paths.get("/usr/share/havenask/config"));
        final Map<String, String> envVars = singletonMap("HAVENASK_JAVA_OPTS", "-XX:-UseCompressedOops");
        runContainer(distribution(), volumes, envVars);

        waitForHavenask(installation);

        final JsonNode nodes = getJson("_nodes").get("nodes");
        final String nodeId = nodes.fieldNames().next();

        final int heapSize = nodes.at("/" + nodeId + "/jvm/mem/heap_init_in_bytes").intValue();
        final boolean usingCompressedPointers = nodes.at("/" + nodeId + "/jvm/using_compressed_ordinary_object_pointers").asBoolean();

        logger.warn(nodes.at("/" + nodeId + "/jvm/mem/heap_init_in_bytes"));

        assertThat("heap_init_in_bytes", heapSize, equalTo(536870912));
        assertThat("using_compressed_ordinary_object_pointers", usingCompressedPointers, equalTo(false));
    }

    /**
     * Check that the default config can be overridden using a bind mount, and that env vars are respected.
     */
    public void test071BindMountCustomPathWithDifferentUID() throws Exception {
        Platforms.onLinux(() -> {
            final Path tempEsDataDir = tempDir.resolve("esDataDir");
            // Make the local directory and contents accessible when bind-mounted
            mkDirWithPrivilegeEscalation(tempEsDataDir, 1500, 0);

            // Restart the container
            final Map<Path, Path> volumes = singletonMap(tempEsDataDir.toAbsolutePath(), installation.data);

            runContainer(distribution(), volumes, null);

            waitForHavenask(installation);

            final JsonNode nodes = getJson("_nodes");

            assertThat(nodes.at("/_nodes/total").intValue(), equalTo(1));
            assertThat(nodes.at("/_nodes/successful").intValue(), equalTo(1));
            assertThat(nodes.at("/_nodes/failed").intValue(), equalTo(0));

            // Ensure container is stopped before we remove tempEsDataDir, so nothing
            // is using the directory.
            removeContainer();

            rmDirWithPrivilegeEscalation(tempEsDataDir);
        });
    }

    /**
     * Check that it is possible to run Havenask under a different user and group to the default.
     */
    public void test072RunEsAsDifferentUserAndGroup() throws Exception {
        assumeFalse(Platforms.WINDOWS);

        final Path tempEsDataDir = tempDir.resolve("esDataDir");
        final Path tempEsConfigDir = tempDir.resolve("esConfDir");
        final Path tempEsLogsDir = tempDir.resolve("esLogsDir");

        Files.createDirectory(tempEsConfigDir);
        Files.createDirectory(tempEsConfigDir.resolve("jvm.options.d"));
        Files.createDirectory(tempEsDataDir);
        Files.createDirectory(tempEsLogsDir);

        copyFromContainer(installation.config("havenask.yml"), tempEsConfigDir);
        copyFromContainer(installation.config("jvm.options"), tempEsConfigDir);
        copyFromContainer(installation.config("log4j2.properties"), tempEsConfigDir);

        chownWithPrivilegeEscalation(tempEsConfigDir, "501:501");
        chownWithPrivilegeEscalation(tempEsDataDir, "501:501");
        chownWithPrivilegeEscalation(tempEsLogsDir, "501:501");

        // Define the bind mounts
        final Map<Path, Path> volumes = new HashMap<>();
        volumes.put(tempEsDataDir.toAbsolutePath(), installation.data);
        volumes.put(tempEsConfigDir.toAbsolutePath(), installation.config);
        volumes.put(tempEsLogsDir.toAbsolutePath(), installation.logs);

        // Restart the container
        runContainer(distribution(), volumes, null, 501, 501);

        waitForHavenask(installation);
    }

    /**
     * Check that environment variables cannot be used with _FILE environment variables.
     */
    public void test082CannotUseEnvVarsAndFiles() throws Exception {
        final String passwordFilename = "password.txt";

        Files.write(tempDir.resolve(passwordFilename), "other_hunter2\n".getBytes(StandardCharsets.UTF_8));

        Map<String, String> envVars = new HashMap<>();
        envVars.put("HAVENASK_PASSWORD", "hunter2");
        envVars.put("HAVENASK_PASSWORD_FILE", "/run/secrets/" + passwordFilename);

        // File permissions need to be secured in order for the ES wrapper to accept
        // them for populating env var values
        Files.setPosixFilePermissions(tempDir.resolve(passwordFilename), p600);

        final Map<Path, Path> volumes = singletonMap(tempDir, Paths.get("/run/secrets"));

        final Result dockerLogs = runContainerExpectingFailure(distribution, volumes, envVars);

        assertThat(
            dockerLogs.stderr,
            containsString("ERROR: Both HAVENASK_PASSWORD_FILE and HAVENASK_PASSWORD are set. These are mutually exclusive.")
        );
    }

    /**
     * Check that when populating environment variables by setting variables with the suffix "_FILE",
     * the files' permissions are checked.
     */
    public void test083EnvironmentVariablesUsingFilesHaveCorrectPermissions() throws Exception {
        final String passwordFilename = "password.txt";

        Files.write(tempDir.resolve(passwordFilename), "hunter2\n".getBytes(StandardCharsets.UTF_8));

        Map<String, String> envVars = singletonMap("HAVENASK_PASSWORD_FILE", "/run/secrets/" + passwordFilename);

        // Set invalid file permissions
        Files.setPosixFilePermissions(tempDir.resolve(passwordFilename), p660);

        final Map<Path, Path> volumes = singletonMap(tempDir, Paths.get("/run/secrets"));

        // Restart the container
        final Result dockerLogs = runContainerExpectingFailure(distribution(), volumes, envVars);

        assertThat(
            dockerLogs.stderr,
            containsString(
                "ERROR: File /run/secrets/" + passwordFilename + " from HAVENASK_PASSWORD_FILE must have file permissions 400 or 600"
            )
        );
    }

    /**
     * Check that the Havenask-shard tool is shipped in the Docker image and is executable.
     */
    public void test091HavenaskShardCliPackaging() {
        final Installation.Executables bin = installation.executables();

        final Result result = sh.run(bin.shardTool + " -h");
        assertThat(result.stdout, containsString("A CLI tool to remove corrupted parts of unrecoverable shards"));
    }

    /**
     * Check that the Havenask-shard tool is shipped in the Docker image and is executable.
     */
    public void test092HavenaskNodeCliPackaging() {
        final Installation.Executables bin = installation.executables();

        final Result result = sh.run(bin.nodeTool + " -h");
        assertThat(
            "Failed to find expected message about the Havenask-shard CLI tool",
            result.stdout,
            containsString("A CLI tool to " + "do unsafe cluster and index manipulations on current node")
        );
    }

    /**
     * Check that no core dumps have been accidentally included in the Docker image.
     */
    public void test100NoCoreFilesInImage() {
        assertFalse("Unexpected core dump found in Docker image", existsInContainer("/core*"));
    }

    /**
     * Check that there are no files with a GID other than 0.
     */
    public void test101AllFilesAreGroupZero() {
        // Run a `find` command in a new container without Havenask running, so
        // that the results aren't subject to sporadic failures from files appearing /
        // disappearing while `find` is traversing the filesystem.
        //
        // We also create a file under `data/` to ensure that files are created with the
        // expected group.
        final Shell localSh = new Shell();
        final String findResults = localSh.run(
            "docker run --rm --tty " + getImageName() + " bash -c ' touch data/test && find . -not -gid 0 ' "
        ).stdout;

        assertThat("Found some files whose GID != 0", findResults, is(emptyString()));
    }

    /**
     * Check that the Docker image has the expected "Label Schema" labels.
     * @see <a href="http://label-schema.org/">Label Schema website</a>
     */
    public void test110OrgLabelSchemaLabels() throws Exception {
        final Map<String, String> labels = getImageLabels(distribution);

        final Map<String, String> staticLabels = new HashMap<>();
        staticLabels.put("name", "Havenask");
        staticLabels.put("schema-version", "1.0");
        staticLabels.put("url", "https://www.havenask.co/products/havenask");
        staticLabels.put("usage", "https://www.havenask.co/guide/en/havenask/reference/index.html");
        staticLabels.put("vcs-url", "https://github.com/havenask/havenask");
        staticLabels.put("vendor", "Havenask");

        staticLabels.put("license", "Apache-2.0");

        // TODO: we should check the actual version value
        final Set<String> dynamicLabels = new HashSet<>();
        dynamicLabels.add("build-date");
        dynamicLabels.add("vcs-ref");
        dynamicLabels.add("version");

        final String prefix = "org.label-schema";

        staticLabels.forEach((suffix, value) -> {
            String key = prefix + "." + suffix;
            assertThat(labels, hasKey(key));
            assertThat(labels.get(key), equalTo(value));
        });

        dynamicLabels.forEach(label -> {
            String key = prefix + "." + label;
            assertThat(labels, hasKey(key));
        });
    }

    /**
     * Check that the Docker image has the expected "Open Containers Annotations" labels.
     * @see <a href="https://github.com/opencontainers/image-spec/blob/master/annotations.md">Open Containers Annotations</a>
     */
    public void test110OrgOpencontainersLabels() throws Exception {
        final Map<String, String> labels = getImageLabels(distribution);

        final Map<String, String> staticLabels = new HashMap<>();
        staticLabels.put("title", "Havenask");
        staticLabels.put("url", "https://www.havenask.co/products/havenask");
        staticLabels.put("documentation", "https://www.havenask.co/guide/en/havenask/reference/index.html");
        staticLabels.put("source", "https://github.com/havenask/havenask");
        staticLabels.put("vendor", "Havenask");

        staticLabels.put("licenses", "Apache-2.0");

        // TODO: we should check the actual version value
        final Set<String> dynamicLabels = new HashSet<>();
        dynamicLabels.add("created");
        dynamicLabels.add("revision");
        dynamicLabels.add("version");

        final String prefix = "org.opencontainers.image";

        staticLabels.forEach((suffix, value) -> {
            String key = prefix + "." + suffix;
            assertThat(labels, hasKey(key));
            assertThat(labels.get(key), equalTo(value));
        });

        dynamicLabels.forEach(label -> {
            String key = prefix + "." + label;
            assertThat(labels, hasKey(key));
        });
    }

    /**
     * Check that the container logs contain the expected content for Havenask itself.
     */
    public void test120DockerLogsIncludeHavenaskLogs() throws Exception {
        waitForHavenask(installation);
        final Result containerLogs = getContainerLogs();

        assertThat("Container logs don't contain abbreviated class names", containerLogs.stdout, containsString("o.e.n.Node"));
        assertThat("Container logs don't contain INFO level messages", containerLogs.stdout, containsString("INFO"));
    }

    /**
     * Check that the Java process running inside the container has the expected UID, GID and username.
     */
    public void test130JavaHasCorrectOwnership() {
        final List<String> processes = Arrays.stream(sh.run("ps -o uid,gid,user -C java").stdout.split("\n"))
            .skip(1)
            .collect(Collectors.toList());

        assertThat("Expected a single java process", processes, hasSize(1));

        final String[] fields = processes.get(0).trim().split("\\s+");

        assertThat(fields, arrayWithSize(3));
        assertThat("Incorrect UID", fields[0], equalTo("1000"));
        assertThat("Incorrect GID", fields[1], equalTo("0"));
        assertThat("Incorrect username", fields[2], equalTo("havenask"));
    }

    /**
     * Check that the init process running inside the container has the expected PID, UID, GID and user.
     * The PID is particularly important because PID 1 handles signal forwarding and child reaping.
     */
    public void test131InitProcessHasCorrectPID() {
        final List<String> processes = Arrays.stream(sh.run("ps -o pid,uid,gid,command -p 1").stdout.split("\n"))
            .skip(1)
            .collect(Collectors.toList());

        assertThat("Expected a single process", processes, hasSize(1));

        final String[] fields = processes.get(0).trim().split("\\s+", 4);

        assertThat(fields, arrayWithSize(4));
        assertThat("Incorrect PID", fields[0], equalTo("1"));
        assertThat("Incorrect UID", fields[1], equalTo("0"));
        assertThat("Incorrect GID", fields[2], equalTo("0"));
        assertThat("Incorrect init command", fields[3], startsWith("/tini"));
    }

    /**
     * Check that Havenask reports per-node cgroup information.
     */
    public void test140CgroupOsStatsAreAvailable() throws Exception {
        waitForHavenask(installation);

        final JsonNode nodes = getJson("_nodes/stats/os").get("nodes");

        final String nodeId = nodes.fieldNames().next();

        final JsonNode cgroupStats = nodes.at("/" + nodeId + "/os/cgroup");
        assertFalse("Couldn't find /nodes/{nodeId}/os/cgroup in API response", cgroupStats.isMissingNode());

        assertThat("Failed to find [cpu] in node OS cgroup stats", cgroupStats.get("cpu"), not(nullValue()));
        assertThat("Failed to find [cpuacct] in node OS cgroup stats", cgroupStats.get("cpuacct"), not(nullValue()));
    }
}
