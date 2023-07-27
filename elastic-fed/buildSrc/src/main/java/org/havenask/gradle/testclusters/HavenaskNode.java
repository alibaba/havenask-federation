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

package org.havenask.gradle.testclusters;

import org.apache.commons.io.FileUtils;
import org.havenask.gradle.Architecture;
import org.havenask.gradle.DistributionDownloadPlugin;
import org.havenask.gradle.HavenaskDistribution;
import org.havenask.gradle.FileSupplier;
import org.havenask.gradle.Jdk;
import org.havenask.gradle.LazyPropertyList;
import org.havenask.gradle.LazyPropertyMap;
import org.havenask.gradle.LoggedExec;
import org.havenask.gradle.OS;
import org.havenask.gradle.PropertyNormalization;
import org.havenask.gradle.ReaperService;
import org.havenask.gradle.Version;
import org.havenask.gradle.VersionProperties;
import org.havenask.gradle.info.BuildParams;
import org.gradle.api.Action;
import org.gradle.api.Named;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.RegularFile;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.util.PatternFilterable;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class HavenaskNode implements TestClusterConfiguration {

    private static final Logger LOGGER = Logging.getLogger(HavenaskNode.class);
    private static final int HAVENASK_DESTROY_TIMEOUT = 20;
    private static final TimeUnit HAVENASK_DESTROY_TIMEOUT_UNIT = TimeUnit.SECONDS;

    private static final int NODE_UP_TIMEOUT = 2;
    private static final TimeUnit NODE_UP_TIMEOUT_UNIT = TimeUnit.MINUTES;
    private static final int ADDITIONAL_CONFIG_TIMEOUT = 15;
    private static final TimeUnit ADDITIONAL_CONFIG_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private static final List<String> OVERRIDABLE_SETTINGS = Arrays.asList(
        "path.repo",
        "discovery.seed_providers"

    );

    private static final int TAIL_LOG_MESSAGES_COUNT = 40;
    private static final List<String> MESSAGES_WE_DONT_CARE_ABOUT = Arrays.asList(
        "Option UseConcMarkSweepGC was deprecated",
        "is a pre-release version of Havenask",
        "max virtual memory areas vm.max_map_count"
    );
    private static final String HOSTNAME_OVERRIDE = "LinuxDarwinHostname";
    private static final String COMPUTERNAME_OVERRIDE = "WindowsComputername";

    private final String path;
    private final String name;
    private final Project project;
    private final ReaperService reaper;
    private final Jdk bwcJdk;
    private final FileSystemOperations fileSystemOperations;
    private final ArchiveOperations archiveOperations;

    private final AtomicBoolean configurationFrozen = new AtomicBoolean(false);
    private final Path workingDir;

    private final LinkedHashMap<String, Predicate<TestClusterConfiguration>> waitConditions = new LinkedHashMap<>();
    private final Map<String, Configuration> pluginAndModuleConfigurations = new HashMap<>();
    private final List<Provider<File>> plugins = new ArrayList<>();
    private final List<Provider<File>> modules = new ArrayList<>();
    final LazyPropertyMap<String, CharSequence> settings = new LazyPropertyMap<>("Settings", this);
    private final LazyPropertyMap<String, CharSequence> keystoreSettings = new LazyPropertyMap<>("Keystore", this);
    private final LazyPropertyMap<String, File> keystoreFiles = new LazyPropertyMap<>("Keystore files", this, FileEntry::new);
    private final LazyPropertyList<CliEntry> cliSetup = new LazyPropertyList<>("CLI setup commands", this);
    private final LazyPropertyMap<String, CharSequence> systemProperties = new LazyPropertyMap<>("System properties", this);
    private final LazyPropertyMap<String, CharSequence> environment = new LazyPropertyMap<>("Environment", this);
    private final LazyPropertyList<CharSequence> jvmArgs = new LazyPropertyList<>("JVM arguments", this);
    private final LazyPropertyMap<String, File> extraConfigFiles = new LazyPropertyMap<>("Extra config files", this, FileEntry::new);
    private final LazyPropertyList<File> extraJarFiles = new LazyPropertyList<>("Extra jar files", this);
    private final List<Map<String, String>> credentials = new ArrayList<>();
    final LinkedHashMap<String, String> defaultConfig = new LinkedHashMap<>();

    private final Path confPathRepo;
    private final Path confPathLogs;
    private final Path transportPortFile;
    private final Path httpPortsFile;
    private final Path tmpDir;

    private int currentDistro = 0;
    private TestDistribution testDistribution;
    private List<HavenaskDistribution> distributions = new ArrayList<>();
    private volatile Process havenaskProcess;
    private Function<String, String> nameCustomization = Function.identity();
    private boolean isWorkingDirConfigured = false;
    private String httpPort = "0";
    private String transportPort = "0";
    private Path confPathData;
    private String keystorePassword = "";
    private boolean preserveDataDir = false;
    private final Config havenaskConfig;
    private final Config legacyESConfig;
    private Config currentConfig;

    HavenaskNode(
        String path,
        String name,
        Project project,
        ReaperService reaper,
        FileSystemOperations fileSystemOperations,
        ArchiveOperations archiveOperations,
        File workingDirBase,
        Jdk bwcJdk
    ) {
        this.path = path;
        this.name = name;
        this.project = project;
        this.reaper = reaper;
        this.fileSystemOperations = fileSystemOperations;
        this.archiveOperations = archiveOperations;
        this.bwcJdk = bwcJdk;
        workingDir = workingDirBase.toPath().resolve(safeName(name)).toAbsolutePath();
        confPathRepo = workingDir.resolve("repo");
        confPathData = workingDir.resolve("data");
        confPathLogs = workingDir.resolve("logs");
        transportPortFile = confPathLogs.resolve("transport.ports");
        httpPortsFile = confPathLogs.resolve("http.ports");
        tmpDir = workingDir.resolve("tmp");
        waitConditions.put("ports files", this::checkPortsFilesExistWithDelay);
        setTestDistribution(TestDistribution.INTEG_TEST);
        setVersion(VersionProperties.getHavenask());
        havenaskConfig = Config.getHavenaskConfig(workingDir);
        legacyESConfig = Config.getLegacyESConfig(workingDir);
        currentConfig = havenaskConfig;
    }

    /*
     * An object to contain the configuration needed to install
     * either an Havenask or an elasticsearch distribution on
     * this test node.
     *
     * This is added to be able to run BWC testing against a
     * cluster running elasticsearch.
     *
     * legacyESConfig will be removed in a future release.
     */
    private static class Config {
        final String distroName;
        final String command;
        final String keystoreTool;
        final String pluginTool;
        final String envTempDir;
        final String envJavaOpts;
        final String envPathConf;
        final Path configFile;
        final Path stdoutFile;
        final Path stderrFile;
        final Path stdinFile;

        Config(
            String distroName,
            String command,
            String keystoreTool,
            String pluginTool,
            String envTempDir,
            String envJavaOpts,
            String envPathConf,
            Path configFile,
            Path stdoutFile,
            Path stderrFile,
            Path stdinFile
        ) {
            this.distroName = distroName;
            this.command = command;
            this.keystoreTool = keystoreTool;
            this.pluginTool = pluginTool;
            this.envTempDir = envTempDir;
            this.envJavaOpts = envJavaOpts;
            this.envPathConf = envPathConf;
            this.configFile = configFile;
            this.stdoutFile = stdoutFile;
            this.stderrFile = stderrFile;
            this.stdinFile = stdinFile;
        }

        static Config getHavenaskConfig(Path workingDir) {
            Path confPathLogs = workingDir.resolve("logs");
            return new Config(
                "Havenask",
                "havenask",
                "havenask-keystore",
                "havenask-plugin",
                "HAVENASK_TMPDIR",
                "HAVENASK_JAVA_OPTS",
                "HAVENASK_PATH_CONF",
                workingDir.resolve("config/havenask.yml"),
                confPathLogs.resolve("havenask.stdout.log"),
                confPathLogs.resolve("havenask.stderr.log"),
                workingDir.resolve("havenask.stdin")
            );
        }

        static Config getLegacyESConfig(Path workingDir) {
            Path confPathLogs = workingDir.resolve("logs");
            return new Config(
                "Elasticsearch",
                "elasticsearch",
                "elasticsearch-keystore",
                "elasticsearch-plugin",
                "ES_TMPDIR",
                "ES_JAVA_OPTS",
                "ES_PATH_CONF",
                workingDir.resolve("config/elasticsearch.yml"),
                confPathLogs.resolve("es.stdout.log"),
                confPathLogs.resolve("es.stderr.log"),
                workingDir.resolve("es.stdin")
            );
        }
    }

    private void applyConfig() {
        if (getVersion().onOrAfter("1.0.0")) {
            currentConfig = havenaskConfig;
        } else {
            currentConfig = legacyESConfig;
        }
    }

    @Input
    @Optional
    public String getName() {
        return nameCustomization.apply(name);
    }

    @Internal
    public Version getVersion() {
        return Version.fromString(distributions.get(currentDistro).getVersion());
    }

    @Override
    public void setVersion(String version) {
        requireNonNull(version, "null version passed when configuring test cluster `" + this + "`");
        checkFrozen();
        distributions.clear();
        doSetVersion(version);
        applyConfig();
    }

    @Override
    public void setVersions(List<String> versions) {
        requireNonNull(versions, "null version list passed when configuring test cluster `" + this + "`");
        distributions.clear();
        for (String version : versions) {
            doSetVersion(version);
        }
        applyConfig();
    }

    private void doSetVersion(String version) {
        String distroName = "testclusters" + path.replace(":", "-") + "-" + this.name + "-" + version + "-";
        NamedDomainObjectContainer<HavenaskDistribution> container = DistributionDownloadPlugin.getContainer(project);
        if (container.findByName(distroName) == null) {
            container.create(distroName);
        }
        HavenaskDistribution distro = container.getByName(distroName);
        distro.setVersion(version);
        distro.setArchitecture(Architecture.current());
        setDistributionType(distro, testDistribution);
        distributions.add(distro);
    }

    @Internal
    public TestDistribution getTestDistribution() {
        return testDistribution;
    }

    // package private just so test clusters plugin can access to wire up task dependencies
    @Internal
    List<HavenaskDistribution> getDistributions() {
        return distributions;
    }

    @Override
    public void setTestDistribution(TestDistribution testDistribution) {
        requireNonNull(testDistribution, "null distribution passed when configuring test cluster `" + this + "`");
        checkFrozen();
        this.testDistribution = testDistribution;
        for (HavenaskDistribution distribution : distributions) {
            setDistributionType(distribution, testDistribution);
        }
    }

    private void setDistributionType(HavenaskDistribution distribution, TestDistribution testDistribution) {
        if (testDistribution == TestDistribution.INTEG_TEST) {
            distribution.setType(HavenaskDistribution.Type.INTEG_TEST_ZIP);
            // we change the underlying distribution when changing the test distribution of the cluster.
            distribution.setPlatform(null);
            distribution.setBundledJdk(null);
        } else {
            distribution.setType(HavenaskDistribution.Type.ARCHIVE);
        }
    }

    // package protected so only TestClustersAware can access
    @Internal
    Collection<Configuration> getPluginAndModuleConfigurations() {
        return pluginAndModuleConfigurations.values();
    }

    // creates a configuration to depend on the given plugin project, then wraps that configuration
    // to grab the zip as a file provider
    private Provider<RegularFile> maybeCreatePluginOrModuleDependency(String path) {
        Configuration configuration = pluginAndModuleConfigurations.computeIfAbsent(
            path,
            key -> project.getConfigurations()
                .detachedConfiguration(project.getDependencies().project(Map.of("path", path, "configuration", "zip")))
        );
        Provider<File> fileProvider = configuration.getElements()
            .map(
                s -> s.stream()
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("zip configuration of project " + path + " had no files"))
                    .getAsFile()
            );
        return project.getLayout().file(fileProvider);
    }

    @Override
    public void plugin(Provider<RegularFile> plugin) {
        checkFrozen();
        this.plugins.add(plugin.map(RegularFile::getAsFile));
    }

    @Override
    public void plugin(String pluginProjectPath) {
        plugin(maybeCreatePluginOrModuleDependency(pluginProjectPath));
    }

    @Override
    public void module(Provider<RegularFile> module) {
        checkFrozen();
        this.modules.add(module.map(RegularFile::getAsFile));
    }

    @Override
    public void module(String moduleProjectPath) {
        module(maybeCreatePluginOrModuleDependency(moduleProjectPath));
    }

    @Override
    public void keystore(String key, String value) {
        keystoreSettings.put(key, value);
    }

    @Override
    public void keystore(String key, Supplier<CharSequence> valueSupplier) {
        keystoreSettings.put(key, valueSupplier);
    }

    @Override
    public void keystore(String key, File value) {
        keystoreFiles.put(key, value);
    }

    @Override
    public void keystore(String key, File value, PropertyNormalization normalization) {
        keystoreFiles.put(key, value, normalization);
    }

    @Override
    public void keystore(String key, FileSupplier valueSupplier) {
        keystoreFiles.put(key, valueSupplier);
    }

    @Override
    public void keystorePassword(String password) {
        keystorePassword = password;
    }

    @Override
    public void cliSetup(String binTool, CharSequence... args) {
        cliSetup.add(new CliEntry(binTool, args));
    }

    @Override
    public void setting(String key, String value) {
        settings.put(key, value);
    }

    @Override
    public void setting(String key, String value, PropertyNormalization normalization) {
        settings.put(key, value, normalization);
    }

    @Override
    public void setting(String key, Supplier<CharSequence> valueSupplier) {
        settings.put(key, valueSupplier);
    }

    @Override
    public void setting(String key, Supplier<CharSequence> valueSupplier, PropertyNormalization normalization) {
        settings.put(key, valueSupplier, normalization);
    }

    @Override
    public void systemProperty(String key, String value) {
        systemProperties.put(key, value);
    }

    @Override
    public void systemProperty(String key, Supplier<CharSequence> valueSupplier) {
        systemProperties.put(key, valueSupplier);
    }

    @Override
    public void systemProperty(String key, Supplier<CharSequence> valueSupplier, PropertyNormalization normalization) {
        systemProperties.put(key, valueSupplier, normalization);
    }

    @Override
    public void environment(String key, String value) {
        environment.put(key, value);
    }

    @Override
    public void environment(String key, Supplier<CharSequence> valueSupplier) {
        environment.put(key, valueSupplier);
    }

    @Override
    public void environment(String key, Supplier<CharSequence> valueSupplier, PropertyNormalization normalization) {
        environment.put(key, valueSupplier, normalization);
    }

    public void jvmArgs(String... values) {
        jvmArgs.addAll(Arrays.asList(values));
    }

    @Internal
    public Path getConfigDir() {
        return currentConfig.configFile.getParent();
    }

    @Override
    @Input
    public boolean isPreserveDataDir() {
        return preserveDataDir;
    }

    @Override
    public void setPreserveDataDir(boolean preserveDataDir) {
        this.preserveDataDir = preserveDataDir;
    }

    @Override
    public void freeze() {
        requireNonNull(testDistribution, "null testDistribution passed when configuring test cluster `" + this + "`");
        LOGGER.info("Locking configuration of `{}`", this);
        configurationFrozen.set(true);
    }

    /**
     * Returns a stream of lines in the generated logs similar to Files.lines
     *
     * @return stream of log lines
     */
    public Stream<String> logLines() throws IOException {
        return Files.lines(currentConfig.stdoutFile, StandardCharsets.UTF_8);
    }

    @Override
    public synchronized void start() {
        LOGGER.info("Starting `{}`", this);
        if (Files.exists(getExtractedDistributionDir()) == false) {
            throw new TestClustersException("Can not start " + this + ", missing: " + getExtractedDistributionDir());
        }
        if (Files.isDirectory(getExtractedDistributionDir()) == false) {
            throw new TestClustersException("Can not start " + this + ", is not a directory: " + getExtractedDistributionDir());
        }

        try {
            if (isWorkingDirConfigured == false) {
                logToProcessStdout("Configuring working directory: " + workingDir);
                // make sure we always start fresh
                if (Files.exists(workingDir)) {
                    if (preserveDataDir) {
                        Files.list(workingDir)
                            .filter(path -> path.equals(confPathData) == false)
                            .forEach(path -> fileSystemOperations.delete(d -> d.delete(path)));
                    } else {
                        fileSystemOperations.delete(d -> d.delete(workingDir));
                    }
                }
                isWorkingDirConfigured = true;
            }
            setupNodeDistribution(getExtractedDistributionDir());
            createWorkingDir();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create working directory for " + this, e);
        }

        copyExtraJars();

        copyExtraConfigFiles();

        createConfiguration();

        final List<String> pluginsToInstall = new ArrayList<>();
        if (plugins.isEmpty() == false) {
            pluginsToInstall.addAll(plugins.stream().map(Provider::get).map(p -> p.toURI().toString()).collect(Collectors.toList()));
        }

        if (pluginsToInstall.isEmpty() == false) {
            if (getVersion().onOrAfter("7.6.0")) {
                logToProcessStdout("installing " + pluginsToInstall.size() + " plugins in a single transaction");
                final String[] arguments = Stream.concat(Stream.of("install", "--batch"), pluginsToInstall.stream()).toArray(String[]::new);
                runHavenaskBinScript(currentConfig.pluginTool, arguments);
                logToProcessStdout("installed plugins");
            } else {
                logToProcessStdout("installing " + pluginsToInstall.size() + " plugins sequentially");
                pluginsToInstall.forEach(plugin -> runHavenaskBinScript(currentConfig.pluginTool, "install", "--batch", plugin));
                logToProcessStdout("installed plugins");
            }
        }

        logToProcessStdout("Creating " + currentConfig.command + " keystore with password set to [" + keystorePassword + "]");
        if (keystorePassword.length() > 0) {
            runHavenaskBinScriptWithInput(keystorePassword + "\n" + keystorePassword, currentConfig.keystoreTool, "create", "-p");
        } else {
            runHavenaskBinScript(currentConfig.keystoreTool, "-v", "create");
        }

        if (keystoreSettings.isEmpty() == false || keystoreFiles.isEmpty() == false) {
            logToProcessStdout("Adding " + keystoreSettings.size() + " keystore settings and " + keystoreFiles.size() + " keystore files");

            keystoreSettings.forEach((key, value) -> runKeystoreCommandWithPassword(keystorePassword, value.toString(), "add", "-x", key));

            for (Map.Entry<String, File> entry : keystoreFiles.entrySet()) {
                File file = entry.getValue();
                requireNonNull(file, "supplied keystoreFile was null when configuring " + this);
                if (file.exists() == false) {
                    throw new TestClustersException("supplied keystore file " + file + " does not exist, require for " + this);
                }
                runKeystoreCommandWithPassword(keystorePassword, "", "add-file", entry.getKey(), file.getAbsolutePath());
            }
        }

        installModules();

        if (cliSetup.isEmpty() == false) {
            logToProcessStdout("Running " + cliSetup.size() + " setup commands");

            for (CliEntry entry : cliSetup) {
                runHavenaskBinScript(entry.executable, entry.args);
            }
        }

        logToProcessStdout("Starting " + currentConfig.distroName + " process");
        startHavenaskProcess();
    }

    private boolean canUseSharedDistribution() {
        // using original location can be too long due to MAX_PATH restrictions on windows CI
        // TODO revisit when moving to shorter paths on CI by using Teamcity
        return OS.current() != OS.WINDOWS && extraJarFiles.size() == 0 && modules.size() == 0 && plugins.size() == 0;
    }

    private void logToProcessStdout(String message) {
        try {
            if (Files.exists(currentConfig.stdoutFile.getParent()) == false) {
                Files.createDirectories(currentConfig.stdoutFile.getParent());
            }
            Files.write(
                currentConfig.stdoutFile,
                ("[" + Instant.now().toString() + "] [BUILD] " + message + "\n").getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void restart() {
        LOGGER.info("Restarting {}", this);
        stop(false);
        start();
    }

    void goToNextVersion() {
        if (currentDistro + 1 >= distributions.size()) {
            throw new TestClustersException("Ran out of versions to go to for " + this);
        }
        logToProcessStdout("Switch version from " + getVersion() + " to " + distributions.get(currentDistro + 1).getVersion());
        currentDistro += 1;
        applyConfig();
        setting("node.attr.upgraded", "true");
    }

    private boolean isSettingTrue(String name) {
        return Boolean.valueOf(settings.getOrDefault(name, "false").toString());
    }

    private void copyExtraConfigFiles() {
        if (extraConfigFiles.isEmpty() == false) {
            logToProcessStdout("Setting up " + extraConfigFiles.size() + " additional config files");
        }
        extraConfigFiles.forEach((destination, from) -> {
            if (Files.exists(from.toPath()) == false) {
                throw new TestClustersException("Can't create extra config file from " + from + " for " + this + " as it does not exist");
            }
            Path dst = currentConfig.configFile.getParent().resolve(destination);
            try {
                Files.createDirectories(dst.getParent());
                Files.copy(from.toPath(), dst, StandardCopyOption.REPLACE_EXISTING);
                LOGGER.info("Added extra config file {} for {}", destination, this);
            } catch (IOException e) {
                throw new UncheckedIOException("Can't create extra config file for", e);
            }
        });
    }

    /**
     * Copies extra jars to the `/lib` directory.
     * //TODO: Remove this when system modules are available
     */
    private void copyExtraJars() {
        if (extraJarFiles.isEmpty() == false) {
            logToProcessStdout("Setting up " + extraJarFiles.size() + " additional jar dependencies");
        }
        extraJarFiles.forEach(from -> {
            Path destination = getDistroDir().resolve("lib").resolve(from.getName());
            try {
                Files.copy(from.toPath(), destination, StandardCopyOption.REPLACE_EXISTING);
                LOGGER.info("Added extra jar {} to {}", from.getName(), destination);
            } catch (IOException e) {
                throw new UncheckedIOException("Can't copy extra jar dependency " + from.getName() + " to " + destination.toString(), e);
            }
        });
    }

    private void installModules() {
        if (testDistribution == TestDistribution.INTEG_TEST) {
            logToProcessStdout("Installing " + modules.size() + "modules");
            for (Provider<File> module : modules) {
                Path destination = getDistroDir().resolve("modules")
                    .resolve(module.get().getName().replace(".zip", "").replace("-" + getVersion(), "").replace("-SNAPSHOT", ""));
                // only install modules that are not already bundled with the integ-test distribution
                if (Files.exists(destination) == false) {
                    fileSystemOperations.copy(spec -> {
                        if (module.get().getName().toLowerCase().endsWith(".zip")) {
                            spec.from(archiveOperations.zipTree(module));
                        } else if (module.get().isDirectory()) {
                            spec.from(module);
                        } else {
                            throw new IllegalArgumentException("Not a valid module " + module + " for " + this);
                        }
                        spec.into(destination);
                    });
                }
            }
        } else {
            LOGGER.info("Not installing " + modules.size() + "(s) since the " + distributions + " distribution already " + "has them");
        }
    }

    @Override
    public void extraConfigFile(String destination, File from) {
        if (destination.contains("..")) {
            throw new IllegalArgumentException("extra config file destination can't be relative, was " + destination + " for " + this);
        }
        extraConfigFiles.put(destination, from);
    }

    @Override
    public void extraConfigFile(String destination, File from, PropertyNormalization normalization) {
        if (destination.contains("..")) {
            throw new IllegalArgumentException("extra config file destination can't be relative, was " + destination + " for " + this);
        }
        extraConfigFiles.put(destination, from, normalization);
    }

    @Override
    public void extraJarFile(File from) {
        if (from.toString().endsWith(".jar") == false) {
            throw new IllegalArgumentException("extra jar file " + from.toString() + " doesn't appear to be a JAR");
        }
        extraJarFiles.add(from);
    }

    @Override
    public void user(Map<String, String> userSpec) {}

    private void runHavenaskBinScriptWithInput(String input, String tool, CharSequence... args) {
        if (Files.exists(getDistroDir().resolve("bin").resolve(tool)) == false
            && Files.exists(getDistroDir().resolve("bin").resolve(tool + ".bat")) == false) {
            throw new TestClustersException(
                "Can't run bin script: `" + tool + "` does not exist. Is this the distribution you expect it to be ?"
            );
        }
        try (InputStream byteArrayInputStream = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8))) {
            LoggedExec.exec(project, spec -> {
                spec.setEnvironment(getHavenaskEnvironment());
                spec.workingDir(getDistroDir());
                spec.executable(OS.conditionalString().onUnix(() -> "./bin/" + tool).onWindows(() -> "cmd").supply());
                spec.args(OS.<List<CharSequence>>conditional().onWindows(() -> {
                    ArrayList<CharSequence> result = new ArrayList<>();
                    result.add("/c");
                    result.add("bin\\" + tool + ".bat");
                    for (CharSequence arg : args) {
                        result.add(arg);
                    }
                    return result;
                }).onUnix(() -> Arrays.asList(args)).supply());
                spec.setStandardInput(byteArrayInputStream);

            });
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to run " + tool + " for " + this, e);
        }
    }

    private void runKeystoreCommandWithPassword(String keystorePassword, String input, CharSequence... args) {
        final String actualInput = keystorePassword.length() > 0 ? keystorePassword + "\n" + input : input;
        runHavenaskBinScriptWithInput(actualInput, currentConfig.keystoreTool, args);
    }

    private void runHavenaskBinScript(String tool, CharSequence... args) {
        runHavenaskBinScriptWithInput("", tool, args);
    }

    private Map<String, String> getHavenaskEnvironment() {
        Map<String, String> defaultEnv = new HashMap<>();
        getRequiredJavaHome().ifPresent(javaHome -> defaultEnv.put("JAVA_HOME", javaHome));
        defaultEnv.put(currentConfig.envPathConf, currentConfig.configFile.getParent().toString());
        String systemPropertiesString = "";
        if (systemProperties.isEmpty() == false) {
            systemPropertiesString = " "
                + systemProperties.entrySet()
                    .stream()
                    .map(entry -> "-D" + entry.getKey() + "=" + entry.getValue())
                    // HAVENASK_PATH_CONF is also set as an environment variable and for a reference to ${HAVENASK_PATH_CONF}
                    // to work HAVENASK_JAVA_OPTS, we need to make sure that HAVENASK_PATH_CONF before HAVENASK_JAVA_OPTS. Instead,
                    // we replace the reference with the actual value in other environment variables
                    .map(p -> p.replace("${" + currentConfig.envPathConf + "}", currentConfig.configFile.getParent().toString()))
                    .collect(Collectors.joining(" "));
        }
        String jvmArgsString = "";
        if (jvmArgs.isEmpty() == false) {
            jvmArgsString = " " + jvmArgs.stream().peek(argument -> {
                if (argument.toString().startsWith("-D")) {
                    throw new TestClustersException(
                        "Invalid jvm argument `" + argument + "` configure as systemProperty instead for " + this
                    );
                }
            }).collect(Collectors.joining(" "));
        }
        String heapSize = System.getProperty("tests.heap.size", "512m");
        defaultEnv.put(
            currentConfig.envJavaOpts,
            "-Xms" + heapSize + " -Xmx" + heapSize + " -ea -esa " + systemPropertiesString + " " + jvmArgsString + " " +
            // Support passing in additional JVM arguments
                System.getProperty("tests.jvm.argline", "")
        );
        defaultEnv.put(currentConfig.envTempDir, tmpDir.toString());
        // Windows requires this as it defaults to `c:\windows` despite HAVENASK_TMPDIR
        defaultEnv.put("TMP", tmpDir.toString());

        // Override the system hostname variables for testing
        defaultEnv.put("HOSTNAME", HOSTNAME_OVERRIDE);
        defaultEnv.put("COMPUTERNAME", COMPUTERNAME_OVERRIDE);

        Set<String> commonKeys = new HashSet<>(environment.keySet());
        commonKeys.retainAll(defaultEnv.keySet());
        if (commonKeys.isEmpty() == false) {
            throw new IllegalStateException("testcluster does not allow overwriting the following env vars " + commonKeys + " for " + this);
        }

        environment.forEach((key, value) -> defaultEnv.put(key, value.toString()));
        return defaultEnv;
    }

    private java.util.Optional<String> getRequiredJavaHome() {
        // If we are testing the current version of Elasticsearch, use the configured runtime Java
        if (getTestDistribution() == TestDistribution.INTEG_TEST || getVersion().equals(VersionProperties.getHavenaskVersion())) {
            return java.util.Optional.of(BuildParams.getRuntimeJavaHome()).map(File::getAbsolutePath);
        } else if (getVersion().before("7.0.0")) {
            return java.util.Optional.of(bwcJdk.getJavaHomePath().toString());
        } else { // otherwise use the bundled JDK
            return java.util.Optional.empty();
        }
    }

    @Internal
    Jdk getBwcJdk() {
        return getVersion().before("7.0.0") ? bwcJdk : null;
    }

    private void startHavenaskProcess() {
        final ProcessBuilder processBuilder = new ProcessBuilder();
        Path effectiveDistroDir = getDistroDir();
        List<String> command = OS.<List<String>>conditional()
            .onUnix(() -> Arrays.asList(effectiveDistroDir.resolve("./bin/" + currentConfig.command).toString()))
            .onWindows(() -> Arrays.asList("cmd", "/c", effectiveDistroDir.resolve("bin\\" + currentConfig.command + ".bat").toString()))
            .supply();
        processBuilder.command(command);
        processBuilder.directory(workingDir.toFile());
        Map<String, String> environment = processBuilder.environment();
        // Don't inherit anything from the environment for as that would lack reproducibility
        environment.clear();
        environment.putAll(getHavenaskEnvironment());

        // don't buffer all in memory, make sure we don't block on the default pipes
        processBuilder.redirectError(ProcessBuilder.Redirect.appendTo(currentConfig.stderrFile.toFile()));
        processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(currentConfig.stdoutFile.toFile()));

        if (keystorePassword != null && keystorePassword.length() > 0) {
            try {
                Files.write(currentConfig.stdinFile, (keystorePassword + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);
                processBuilder.redirectInput(currentConfig.stdinFile.toFile());
            } catch (IOException e) {
                throw new TestClustersException("Failed to set the keystore password for " + this, e);
            }
        }
        LOGGER.info("Running `{}` in `{}` for {} env: {}", command, workingDir, this, environment);
        try {
            havenaskProcess = processBuilder.start();
        } catch (IOException e) {
            throw new TestClustersException("Failed to start " + currentConfig.command + " process for " + this, e);
        }
        reaper.registerPid(toString(), havenaskProcess.pid());
    }

    @Internal
    public Path getDistroDir() {
        return canUseSharedDistribution()
            ? getExtractedDistributionDir().toFile().listFiles()[0].toPath()
            : workingDir.resolve("distro").resolve(getVersion() + "-" + testDistribution);
    }

    @Override
    @Internal
    public String getHttpSocketURI() {
        return getHttpPortInternal().get(0);
    }

    @Override
    @Internal
    public String getTransportPortURI() {
        return getTransportPortInternal().get(0);
    }

    @Override
    @Internal
    public List<String> getAllHttpSocketURI() {
        waitForAllConditions();
        return getHttpPortInternal();
    }

    @Override
    @Internal
    public List<String> getAllTransportPortURI() {
        waitForAllConditions();
        return getTransportPortInternal();
    }

    @Internal
    public File getServerLog() {
        return confPathLogs.resolve(defaultConfig.get("cluster.name") + "_server.json").toFile();
    }

    @Internal
    public File getAuditLog() {
        return confPathLogs.resolve(defaultConfig.get("cluster.name") + "_audit.json").toFile();
    }

    @Override
    public synchronized void stop(boolean tailLogs) {
        logToProcessStdout("Stopping node");
        try {
            if (Files.exists(httpPortsFile)) {
                Files.delete(httpPortsFile);
            }
            if (Files.exists(transportPortFile)) {
                Files.delete(transportPortFile);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (havenaskProcess == null && tailLogs) {
            // This is a special case. If start() throws an exception the plugin will still call stop
            // Another exception here would eat the orriginal.
            return;
        }
        LOGGER.info("Stopping `{}`, tailLogs: {}", this, tailLogs);
        requireNonNull(havenaskProcess, "Can't stop `" + this + "` as it was not started or already stopped.");
        // Test clusters are not reused, don't spend time on a graceful shutdown
        stopHandle(havenaskProcess.toHandle(), true);
        reaper.unregister(toString());
        if (tailLogs) {
            logFileContents("Standard output of node", currentConfig.stdoutFile);
            logFileContents("Standard error of node", currentConfig.stderrFile);
        }
        havenaskProcess = null;
        // Clean up the ports file in case this is started again.
        try {
            if (Files.exists(httpPortsFile)) {
                Files.delete(httpPortsFile);
            }
            if (Files.exists(transportPortFile)) {
                Files.delete(transportPortFile);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setNameCustomization(Function<String, String> nameCustomizer) {
        this.nameCustomization = nameCustomizer;
    }

    private void stopHandle(ProcessHandle processHandle, boolean forcibly) {
        // No-op if the process has already exited by itself.
        if (processHandle.isAlive() == false) {
            LOGGER.info("Process was not running when we tried to terminate it.");
            return;
        }

        // Stop all children last - if the ML processes are killed before the ES JVM then
        // they'll be recorded as having failed and won't restart when the cluster restarts.
        // ES could actually be a child when there's some wrapper process like on Windows,
        // and in that case the ML processes will be grandchildren of the wrapper.
        List<ProcessHandle> children = processHandle.children().collect(Collectors.toList());
        try {
            logProcessInfo(
                "Terminating " + currentConfig.command + " process" + (forcibly ? " forcibly " : "gracefully") + ":",
                processHandle.info()
            );

            if (forcibly) {
                processHandle.destroyForcibly();
            } else {
                processHandle.destroy();
                waitForProcessToExit(processHandle);
                if (processHandle.isAlive() == false) {
                    return;
                }
                LOGGER.info(
                    "process did not terminate after {} {}, stopping it forcefully",
                    HAVENASK_DESTROY_TIMEOUT,
                    HAVENASK_DESTROY_TIMEOUT_UNIT
                );
                processHandle.destroyForcibly();
            }

            waitForProcessToExit(processHandle);
            if (processHandle.isAlive()) {
                throw new TestClustersException("Was not able to terminate " + currentConfig.command + " process for " + this);
            }
        } finally {
            children.forEach(each -> stopHandle(each, forcibly));
        }
    }

    private void logProcessInfo(String prefix, ProcessHandle.Info info) {
        LOGGER.info(
            prefix + " commandLine:`{}` command:`{}` args:`{}`",
            info.commandLine().orElse("-"),
            info.command().orElse("-"),
            Arrays.stream(info.arguments().orElse(new String[] {})).map(each -> "'" + each + "'").collect(Collectors.joining(" "))
        );
    }

    private void logFileContents(String description, Path from) {
        final Map<String, Integer> errorsAndWarnings = new LinkedHashMap<>();
        LinkedList<String> ring = new LinkedList<>();
        try (LineNumberReader reader = new LineNumberReader(Files.newBufferedReader(from))) {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                final String lineToAdd;
                if (ring.isEmpty()) {
                    lineToAdd = line;
                } else {
                    if (line.startsWith("[")) {
                        lineToAdd = line;
                        // check to see if the previous message (possibly combined from multiple lines) was an error or
                        // warning as we want to show all of them
                        String previousMessage = normalizeLogLine(ring.getLast());
                        if (MESSAGES_WE_DONT_CARE_ABOUT.stream().noneMatch(previousMessage::contains)
                            && (previousMessage.contains("ERROR") || previousMessage.contains("WARN"))) {
                            errorsAndWarnings.put(previousMessage, errorsAndWarnings.getOrDefault(previousMessage, 0) + 1);
                        }
                    } else {
                        // We combine multi line log messages to make sure we never break exceptions apart
                        lineToAdd = ring.removeLast() + "\n" + line;
                    }
                }
                ring.add(lineToAdd);
                if (ring.size() >= TAIL_LOG_MESSAGES_COUNT) {
                    ring.removeFirst();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to tail log " + this, e);
        }

        if (errorsAndWarnings.isEmpty() == false || ring.isEmpty() == false) {
            LOGGER.error("\n=== {} `{}` ===", description, this);
        }
        if (errorsAndWarnings.isEmpty() == false) {
            LOGGER.lifecycle("\n»    ↓ errors and warnings from " + from + " ↓");
            errorsAndWarnings.forEach((message, count) -> {
                LOGGER.lifecycle("» " + message.replace("\n", "\n»  "));
                if (count > 1) {
                    LOGGER.lifecycle("»   ↑ repeated " + count + " times ↑");
                }
            });
        }

        ring.removeIf(line -> MESSAGES_WE_DONT_CARE_ABOUT.stream().anyMatch(line::contains));

        if (ring.isEmpty() == false) {
            LOGGER.lifecycle("»   ↓ last " + TAIL_LOG_MESSAGES_COUNT + " non error or warning messages from " + from + " ↓");
            ring.forEach(message -> {
                if (errorsAndWarnings.containsKey(normalizeLogLine(message)) == false) {
                    LOGGER.lifecycle("» " + message.replace("\n", "\n»  "));
                }
            });
        }
    }

    private String normalizeLogLine(String line) {
        if (line.contains("ERROR")) {
            return line.substring(line.indexOf("ERROR"));
        }
        if (line.contains("WARN")) {
            return line.substring(line.indexOf("WARN"));
        }
        return line;
    }

    private void waitForProcessToExit(ProcessHandle processHandle) {
        try {
            processHandle.onExit().get(HAVENASK_DESTROY_TIMEOUT, HAVENASK_DESTROY_TIMEOUT_UNIT);
        } catch (InterruptedException e) {
            LOGGER.info("Interrupted while waiting for {} process", currentConfig.command, e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOGGER.info("Failure while waiting for process to exist", e);
        } catch (TimeoutException e) {
            LOGGER.info("Timed out waiting for process to exit", e);
        }
    }

    private void createWorkingDir() throws IOException {
        // Start configuration from scratch in case of a restart
        fileSystemOperations.delete(d -> d.delete(currentConfig.configFile.getParent()));
        Files.createDirectories(currentConfig.configFile.getParent());
        Files.createDirectories(confPathRepo);
        Files.createDirectories(confPathData);
        Files.createDirectories(confPathLogs);
        Files.createDirectories(tmpDir);
    }

    private void setupNodeDistribution(Path distroExtractDir) throws IOException {
        if (canUseSharedDistribution() == false) {
            logToProcessStdout("Configuring custom cluster specific distro directory: " + getDistroDir());
            if (Files.exists(getDistroDir()) == false) {
                try {
                    syncWithLinks(distroExtractDir, getDistroDir());
                } catch (LinkCreationException e) {
                    // Note does not work for network drives, e.g. Vagrant
                    LOGGER.info("Failed to create working dir using hard links. Falling back to copy", e);
                    // ensure we get a clean copy
                    FileUtils.deleteDirectory(getDistroDir().toFile());
                    syncWithCopy(distroExtractDir, getDistroDir());
                }
            }
        }
    }

    /**
     * Does the equivalent of `cp -lr` and `chmod -r a-w` to save space and improve speed.
     * We remove write permissions to make sure files are note mistakenly edited ( e.x. the config file ) and changes
     * reflected across all copies. Permissions are retained to be able to replace the links.
     *
     * @param sourceRoot      where to copy from
     * @param destinationRoot destination to link to
     */
    private void syncWithLinks(Path sourceRoot, Path destinationRoot) {
        sync(sourceRoot, destinationRoot, (Path d, Path s) -> {
            try {
                Files.createLink(d, s);
            } catch (IOException e) {
                // Note does not work for network drives, e.g. Vagrant
                throw new LinkCreationException("Failed to create hard link " + d + " pointing to " + s, e);
            }
        });
    }

    private void syncWithCopy(Path sourceRoot, Path destinationRoot) {
        sync(sourceRoot, destinationRoot, (Path d, Path s) -> {
            try {
                Files.copy(s, d);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to copy " + s + " to " + d, e);
            }
        });
    }

    private void sync(Path sourceRoot, Path destinationRoot, BiConsumer<Path, Path> syncMethod) {
        assert Files.exists(destinationRoot) == false;
        try (Stream<Path> stream = Files.walk(sourceRoot)) {
            stream.forEach(source -> {
                Path relativeDestination = sourceRoot.relativize(source);
                if (relativeDestination.getNameCount() <= 1) {
                    return;
                }
                // Throw away the first name as the archives have everything in a single top level folder we are not interested in
                relativeDestination = relativeDestination.subpath(1, relativeDestination.getNameCount());

                Path destination = destinationRoot.resolve(relativeDestination);
                if (Files.isDirectory(source)) {
                    try {
                        Files.createDirectories(destination);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Can't create directory " + destination.getParent(), e);
                    }
                } else {
                    try {
                        Files.createDirectories(destination.getParent());
                    } catch (IOException e) {
                        throw new UncheckedIOException("Can't create directory " + destination.getParent(), e);
                    }
                    syncMethod.accept(destination, source);

                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("Can't walk source " + sourceRoot, e);
        }
    }

    private void createConfiguration() {
        String nodeName = nameCustomization.apply(safeName(name));
        Map<String, String> baseConfig = new HashMap<>(defaultConfig);
        if (nodeName != null) {
            baseConfig.put("node.name", nodeName);
        }
        baseConfig.put("path.repo", confPathRepo.toAbsolutePath().toString());
        baseConfig.put("path.data", confPathData.toAbsolutePath().toString());
        baseConfig.put("path.logs", confPathLogs.toAbsolutePath().toString());
        baseConfig.put("path.shared_data", workingDir.resolve("sharedData").toString());
        baseConfig.put("node.attr.testattr", "test");
        baseConfig.put("node.portsfile", "true");
        baseConfig.put("http.port", httpPort);
        if (getVersion().onOrAfter(Version.fromString("6.7.0"))) {
            baseConfig.put("transport.port", transportPort);
        } else {
            baseConfig.put("transport.tcp.port", transportPort);
        }
        // Default the watermarks to absurdly low to prevent the tests from failing on nodes without enough disk space
        baseConfig.put("cluster.routing.allocation.disk.watermark.low", "1b");
        baseConfig.put("cluster.routing.allocation.disk.watermark.high", "1b");
        // increase script compilation limit since tests can rapid-fire script compilations
        if (getVersion().onOrAfter(Version.fromString("7.9.0"))) {
            baseConfig.put("script.disable_max_compilations_rate", "true");
        } else {
            baseConfig.put("script.max_compilations_rate", "2048/1m");
        }
        if (getVersion().onOrAfter("6.0.0")) {
            baseConfig.put("cluster.routing.allocation.disk.watermark.flood_stage", "1b");
        }
        // Temporarily disable the real memory usage circuit breaker. It depends on real memory usage which we have no full control
        // over and the REST client will not retry on circuit breaking exceptions yet (see #31986 for details). Once the REST client
        // can retry on circuit breaking exceptions, we can revert again to the default configuration.
        if (getVersion().onOrAfter("7.0.0")) {
            baseConfig.put("indices.breaker.total.use_real_memory", "false");
        }
        // Don't wait for state, just start up quickly. This will also allow new and old nodes in the BWC case to become the master
        baseConfig.put("discovery.initial_state_timeout", "0s");

        // TODO: Remove these once https://github.com/elastic/elasticsearch/issues/46091 is fixed
        if (getVersion().onOrAfter("1.0.0")) {
            baseConfig.put("logger.org.havenask.action.support.master", "DEBUG");
            baseConfig.put("logger.org.havenask.cluster.coordination", "DEBUG");
        } else {
            baseConfig.put("logger.org.elasticsearch.action.support.master", "DEBUG");
            baseConfig.put("logger.org.elasticsearch.cluster.coordination", "DEBUG");
        }

        HashSet<String> overriden = new HashSet<>(baseConfig.keySet());
        overriden.retainAll(settings.keySet());
        overriden.removeAll(OVERRIDABLE_SETTINGS);
        if (overriden.isEmpty() == false) {
            throw new IllegalArgumentException(
                "Testclusters does not allow the following settings to be changed:" + overriden + " for " + this
            );
        }
        // Make sure no duplicate config keys
        settings.keySet().stream().filter(OVERRIDABLE_SETTINGS::contains).forEach(baseConfig::remove);

        final Path configFileRoot = currentConfig.configFile.getParent();
        try {
            Files.write(
                currentConfig.configFile,
                Stream.concat(settings.entrySet().stream(), baseConfig.entrySet().stream())
                    .map(entry -> entry.getKey() + ": " + entry.getValue())
                    .collect(Collectors.joining("\n"))
                    .getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE
            );

            final List<Path> configFiles;
            try (Stream<Path> stream = Files.list(getDistroDir().resolve("config"))) {
                configFiles = stream.collect(Collectors.toList());
            }
            logToProcessStdout("Copying additional config files from distro " + configFiles);
            for (Path file : configFiles) {
                Path dest = currentConfig.configFile.getParent().resolve(file.getFileName());
                if (Files.exists(dest) == false) {
                    Files.copy(file, dest);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Could not write config file: " + currentConfig.configFile, e);
        }

        tweakJvmOptions(configFileRoot);
        LOGGER.info("Written config file:{} for {}", currentConfig.configFile, this);
    }

    private static void copyFolder(Path source, Path target) throws IOException {
        // 如果目标目录不存在，则创建
        if (!Files.exists(target)) {
            Files.createDirectories(target);
        }

        // 遍历源目录下的所有文件和子目录，并逐一进行复制
        Files.walk(source).forEach(f -> {
            try {
                Path targetFile = target.resolve(source.relativize(f));
                if (Files.isDirectory(f)) {
                    // 如果是目录，则创建对应的目录
                    Files.createDirectories(targetFile);
                } else {
                    // 如果是文件，则进行复制
                    Files.copy(f, targetFile);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }


    private void tweakJvmOptions(Path configFileRoot) {
        LOGGER.info("Tweak jvm options {}.", configFileRoot.resolve("jvm.options"));
        Path jvmOptions = configFileRoot.resolve("jvm.options");
        try {
            String content = new String(Files.readAllBytes(jvmOptions));
            Map<String, String> expansions = jvmOptionExpansions();
            for (String origin : expansions.keySet()) {
                if (!content.contains(origin)) {
                    throw new IOException("template property " + origin + " not found in template.");
                }
                content = content.replace(origin, expansions.get(origin));
            }
            Files.write(jvmOptions, content.getBytes());
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
    }

    private Map<String, String> jvmOptionExpansions() {
        Map<String, String> expansions = new HashMap<>();
        Version version = getVersion();
        String heapDumpOrigin = getVersion().onOrAfter("6.3.0") ? "-XX:HeapDumpPath=data" : "-XX:HeapDumpPath=/heap/dump/path";
        Path relativeLogPath = workingDir.relativize(confPathLogs);
        expansions.put(heapDumpOrigin, "-XX:HeapDumpPath=" + relativeLogPath.toString());
        if (version.onOrAfter("6.2.0")) {
            expansions.put("logs/gc.log", relativeLogPath.resolve("gc.log").toString());
        }
        if (getVersion().onOrAfter("7.0.0")) {
            expansions.put(
                "-XX:ErrorFile=logs/hs_err_pid%p.log",
                "-XX:ErrorFile=" + relativeLogPath.resolve("hs_err_pid%p.log").toString()
            );
        }
        return expansions;
    }

    private void checkFrozen() {
        if (configurationFrozen.get()) {
            throw new IllegalStateException("Configuration for " + this + " can not be altered, already locked");
        }
    }

    private List<String> getTransportPortInternal() {
        try {
            return readPortsFile(transportPortFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read transport ports file: " + transportPortFile + " for " + this, e);
        }
    }

    private List<String> getHttpPortInternal() {
        try {
            return readPortsFile(httpPortsFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read http ports file: " + httpPortsFile + " for " + this, e);
        }
    }

    private List<String> readPortsFile(Path file) throws IOException {
        try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
            return lines.map(String::trim).collect(Collectors.toList());
        }
    }

    private Path getExtractedDistributionDir() {
        return distributions.get(currentDistro).getExtracted().getSingleFile().toPath();
    }

    private List<File> getInstalledFileSet(Action<? super PatternFilterable> filter) {
        return Stream.concat(plugins.stream().map(Provider::get), modules.stream().map(Provider::get))
            .filter(File::exists)
            // TODO: We may be able to simplify this with Gradle 5.6
            // https://docs.gradle.org/nightly/release-notes.html#improved-handling-of-zip-archives-on-classpaths
            .map(zipFile -> archiveOperations.zipTree(zipFile).matching(filter))
            .flatMap(tree -> tree.getFiles().stream())
            .sorted(Comparator.comparing(File::getName))
            .collect(Collectors.toList());
    }

    @Classpath
    public List<File> getInstalledClasspath() {
        return getInstalledFileSet(filter -> filter.include("**/*.jar"));
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public List<File> getInstalledFiles() {
        return getInstalledFileSet(filter -> filter.exclude("**/*.jar"));
    }

    @Classpath
    public List<FileTree> getDistributionClasspath() {
        return getDistributionFiles(filter -> filter.include("**/*.jar"));
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public List<FileTree> getDistributionFiles() {
        return getDistributionFiles(filter -> filter.exclude("**/*.jar"));
    }

    private List<FileTree> getDistributionFiles(Action<PatternFilterable> patternFilter) {
        List<FileTree> files = new ArrayList<>();
        for (HavenaskDistribution distribution : distributions) {
            files.add(distribution.getExtracted().getAsFileTree().matching(patternFilter));
        }
        return files;
    }

    @Nested
    public List<?> getKeystoreSettings() {
        return keystoreSettings.getNormalizedCollection();
    }

    @Nested
    public List<?> getKeystoreFiles() {
        return keystoreFiles.getNormalizedCollection();
    }

    @Nested
    public List<?> getCliSetup() {
        return cliSetup.getFlatNormalizedCollection();
    }

    @Nested
    public List<?> getSettings() {
        return settings.getNormalizedCollection();
    }

    @Nested
    public List<?> getSystemProperties() {
        return systemProperties.getNormalizedCollection();
    }

    @Nested
    public List<?> getEnvironment() {
        return environment.getNormalizedCollection();
    }

    @Nested
    public List<?> getJvmArgs() {
        return jvmArgs.getNormalizedCollection();
    }

    @Nested
    public List<?> getExtraConfigFiles() {
        return extraConfigFiles.getNormalizedCollection();
    }

    @Override
    @Internal
    public boolean isProcessAlive() {
        requireNonNull(havenaskProcess, "Can't wait for `" + this + "` as it's not started. Does the task have `useCluster` ?");
        return havenaskProcess.isAlive();
    }

    void waitForAllConditions() {
        waitForConditions(waitConditions, System.currentTimeMillis(), NODE_UP_TIMEOUT_UNIT.toMillis(NODE_UP_TIMEOUT) +
        // Installing plugins at config time and loading them when nods start requires additional time we need to
        // account for
            ADDITIONAL_CONFIG_TIMEOUT_UNIT.toMillis(
                ADDITIONAL_CONFIG_TIMEOUT * (plugins.size() + keystoreFiles.size() + keystoreSettings.size() + credentials.size())
            ), TimeUnit.MILLISECONDS, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HavenaskNode that = (HavenaskNode) o;
        return Objects.equals(name, that.name) && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, path);
    }

    @Override
    public String toString() {
        return "node{" + path + ":" + name + "}";
    }

    @Input
    List<Map<String, String>> getCredentials() {
        return credentials;
    }

    private boolean checkPortsFilesExistWithDelay(TestClusterConfiguration node) {
        if (Files.exists(httpPortsFile) && Files.exists(transportPortFile)) {
            return true;
        }
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TestClustersException("Interrupted while waiting for ports files", e);
        }
        return Files.exists(httpPortsFile) && Files.exists(transportPortFile);
    }

    void setHttpPort(String httpPort) {
        this.httpPort = httpPort;
    }

    void setTransportPort(String transportPort) {
        this.transportPort = transportPort;
    }

    void setDataPath(Path dataPath) {
        this.confPathData = dataPath;
    }

    @Internal
    Path getHavenaskStdoutFile() {
        return currentConfig.stdoutFile;
    }

    @Internal
    Path getHavenaskStderrFile() {
        return currentConfig.stderrFile;
    }

    private static class FileEntry implements Named {
        private String name;
        private File file;

        FileEntry(String name, File file) {
            this.name = name;
            this.file = file;
        }

        @Input
        @Override
        public String getName() {
            return name;
        }

        @InputFile
        @PathSensitive(PathSensitivity.NONE)
        public File getFile() {
            return file;
        }
    }

    private static class CliEntry {
        private String executable;
        private CharSequence[] args;

        CliEntry(String executable, CharSequence[] args) {
            this.executable = executable;
            this.args = args;
        }

        @Input
        public String getExecutable() {
            return executable;
        }

        @Input
        public CharSequence[] getArgs() {
            return args;
        }
    }

    private static class LinkCreationException extends UncheckedIOException {
        LinkCreationException(String message, IOException cause) {
            super(message, cause);
        }
    }
}
