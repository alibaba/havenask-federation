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

package org.havenask.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.havenask.cli.UserException;
import org.havenask.common.CheckedConsumer;
import org.havenask.common.io.PathUtils;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.concurrent.ThreadContext;
import org.havenask.env.Environment;
import org.havenask.index.shard.ShardId;
import org.havenask.tasks.Task;
import org.havenask.test.HavenaskTestCase;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * This test confirms JSON log structure is properly formatted and can be parsed.
 * It has to be in a <code>org.havenask.common.logging</code> package to use <code>PrefixLogger</code>
 */
public class JsonLoggerTests extends HavenaskTestCase {

    private static final String LINE_SEPARATOR = System.lineSeparator();

    @BeforeClass
    public static void initNodeName() {
        JsonLogsTestSetup.init();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        LogConfigurator.registerErrorListener();
        setupLogging("json_layout");
    }

    @Override
    public void tearDown() throws Exception {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configurator.shutdown(context);
        super.tearDown();
    }

    public void testDeprecatedMessage() throws Exception {
        withThreadContext(threadContext -> {
            threadContext.putHeader(Task.X_OPAQUE_ID, "someId");
            final DeprecationLogger testLogger = DeprecationLogger.getLogger("test");
            testLogger.deprecate("someKey", "deprecated message1");

            final Path path = PathUtils.get(
                System.getProperty("havenask.logs.base_path"),
                System.getProperty("havenask.logs.cluster_name") + "_deprecated.json"
            );

            try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
                List<Map<String, String>> jsonLogs = stream.collect(Collectors.toList());

                assertThat(
                    jsonLogs,
                    contains(
                        allOf(
                            hasEntry("type", "deprecation"),
                            hasEntry("level", "DEPRECATION"),
                            hasEntry("component", "d.test"),
                            hasEntry("cluster.name", "havenask"),
                            hasEntry("node.name", "sample-name"),
                            hasEntry("message", "deprecated message1"),
                            hasEntry("x-opaque-id", "someId")
                        )
                    )
                );
            }

            assertWarnings("deprecated message1");
        });
    }


    public void testDeprecatedMessageWithoutXOpaqueId() throws IOException {
        final Logger testLogger = LogManager.getLogger("test");
        testLogger.info(new DeprecatedMessage("key", "someId", "deprecated message1"));
        testLogger.info(new DeprecatedMessage("key", "", "deprecated message2"));
        // This message will be filtered out by the RateLimitingFilter because an empty ID is the same as a null one.
        testLogger.info(new DeprecatedMessage("key", null, "deprecated message3"));
        testLogger.info("deprecated message4");

        final Path path = PathUtils.get(System.getProperty("havenask.logs.base_path"),
            System.getProperty("havenask.logs.cluster_name") + "_deprecated.json");
        try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
            List<Map<String, String>> jsonLogs = stream
                .collect(Collectors.toList());

            assertThat(jsonLogs, contains(
                allOf(
                    hasEntry("type", "deprecation"),
                    hasEntry("level", "INFO"),
                    hasEntry("component", "test"),
                    hasEntry("cluster.name", "havenask"),
                    hasEntry("node.name", "sample-name"),
                    hasEntry("message", "deprecated message1"),
                    hasEntry("x-opaque-id", "someId")),
                allOf(
                    hasEntry("type", "deprecation"),
                    hasEntry("level", "INFO"),
                    hasEntry("component", "test"),
                    hasEntry("cluster.name", "havenask"),
                    hasEntry("node.name", "sample-name"),
                    hasEntry("message", "deprecated message2"),
                    not(hasKey("x-opaque-id"))
                ),
                allOf(
                    hasEntry("type", "deprecation"),
                    hasEntry("level", "INFO"),
                    hasEntry("component", "test"),
                    hasEntry("cluster.name", "havenask"),
                    hasEntry("node.name", "sample-name"),
                    hasEntry("message", "deprecated message4"),
                    not(hasKey("x-opaque-id"))
                )
                )
            );
        }
    }

    public void testJsonLayout() throws IOException {
        final Logger testLogger = LogManager.getLogger("test");

        testLogger.error("This is an error message");
        testLogger.warn("This is a warning message");
        testLogger.info("This is an info message");
        testLogger.debug("This is a debug message");
        testLogger.trace("This is a trace message");
        final Path path = clusterLogsPath();
        try (Stream<JsonLogLine> stream = JsonLogsStream.from(path)) {
            List<JsonLogLine> jsonLogs = collectLines(stream);

            assertThat(jsonLogs, contains(
                logLine("file", Level.ERROR, "sample-name", "test", "This is an error message"),
                logLine("file", Level.WARN, "sample-name", "test", "This is a warning message"),
                logLine("file", Level.INFO, "sample-name", "test", "This is an info message"),
                logLine("file", Level.DEBUG, "sample-name", "test", "This is a debug message"),
                logLine("file", Level.TRACE, "sample-name", "test", "This is a trace message")
            ));
        }
    }

    public void testPrefixLoggerInJson() throws IOException {
        Logger shardIdLogger = Loggers.getLogger("shardIdLogger", ShardId.fromString("[indexName][123]"));
        shardIdLogger.info("This is an info message with a shardId");

        Logger prefixLogger = new PrefixLogger(LogManager.getLogger("prefixLogger"), "PREFIX");
        prefixLogger.info("This is an info message with a prefix");

        final Path path = clusterLogsPath();
        try (Stream<JsonLogLine> stream = JsonLogsStream.from(path)) {
            List<JsonLogLine> jsonLogs = collectLines(stream);
            assertThat(jsonLogs, contains(
                logLine("file", Level.INFO, "sample-name", "shardIdLogger",
                    "[indexName][123] This is an info message with a shardId"),
                logLine("file", Level.INFO, "sample-name", "prefixLogger", "PREFIX This is an info message with a prefix")
            ));
        }
    }

    public void testJsonInMessage() throws IOException {
        final Logger testLogger = LogManager.getLogger("test");
        String json = "{" + LINE_SEPARATOR +
            "  \"terms\" : {" + LINE_SEPARATOR +
            "    \"user\" : [" + LINE_SEPARATOR +
            "      \"u1\"," + LINE_SEPARATOR +
            "      \"u2\"," + LINE_SEPARATOR +
            "      \"u3\"" + LINE_SEPARATOR +
            "    ]," + LINE_SEPARATOR +
            "    \"boost\" : 1.0" + LINE_SEPARATOR +
            "  }" + LINE_SEPARATOR +
            "}";

        testLogger.info(json);

        final Path path = clusterLogsPath();
        try (Stream<JsonLogLine> stream = JsonLogsStream.from(path)) {
            List<JsonLogLine> jsonLogs = collectLines(stream);
            assertThat(jsonLogs, contains(
                logLine("file", Level.INFO, "sample-name", "test", json)
            ));
        }
    }

    public void testStacktrace() throws IOException {
        final Logger testLogger = LogManager.getLogger("test");
        testLogger.error("error message", new Exception("exception message", new RuntimeException("cause message")));

        final Path path = clusterLogsPath();
        try (Stream<JsonLogLine> stream = JsonLogsStream.from(path)) {
            List<JsonLogLine> jsonLogs = collectLines(stream);
            assertThat(jsonLogs, contains(
                allOf(
                    logLine("file", Level.ERROR, "sample-name", "test", "error message"),
                    stacktraceWith("java.lang.Exception: exception message"),
                    stacktraceWith("Caused by: java.lang.RuntimeException: cause message")
                )
            ));
        }
    }

    public void testJsonInStacktraceMessageIsSplitted() throws IOException {
        final Logger testLogger = LogManager.getLogger("test");

        String json = "{" + LINE_SEPARATOR +
            "  \"terms\" : {" + LINE_SEPARATOR +
            "    \"user\" : [" + LINE_SEPARATOR +
            "      \"u1\"," + LINE_SEPARATOR +
            "      \"u2\"," + LINE_SEPARATOR +
            "      \"u3\"" + LINE_SEPARATOR +
            "    ]," + LINE_SEPARATOR +
            "    \"boost\" : 1.0" + LINE_SEPARATOR +
            "  }" + LINE_SEPARATOR +
            "}";
        testLogger.error("error message " + json, new Exception(json));

        final Path path = clusterLogsPath();
        try (Stream<JsonLogLine> stream = JsonLogsStream.from(path)) {
            List<JsonLogLine> jsonLogs = collectLines(stream);

            assertThat(jsonLogs, contains(
                allOf(
                    //message field will have a single line with json escaped
                    logLine("file", Level.ERROR, "sample-name", "test", "error message " + json),

                    //stacktrace field will have each json line will in a separate array element
                    stacktraceWith(("java.lang.Exception: " + json).split(LINE_SEPARATOR))
                )
            ));
        }
    }


    public void testDuplicateLogMessages() throws Exception {
        final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger("test");

        // For the same key and X-Opaque-ID deprecation should be once
        withThreadContext(threadContext -> {
            threadContext.putHeader(Task.X_OPAQUE_ID, "ID1");
            deprecationLogger.deprecate("key", "message1");
            deprecationLogger.deprecate("key", "message2");
            assertWarnings("message1", "message2");

            final Path path = PathUtils.get(System.getProperty("havenask.logs.base_path"),
                System.getProperty("havenask.logs.cluster_name") + "_deprecated.json");
            try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
                List<Map<String, String>> jsonLogs = stream
                    .collect(Collectors.toList());

                assertThat(jsonLogs, contains(
                    allOf(
                        hasEntry("type", "deprecation"),
                        hasEntry("level", "DEPRECATION"),
                        hasEntry("component", "d.test"),
                        hasEntry("cluster.name", "havenask"),
                        hasEntry("node.name", "sample-name"),
                        hasEntry("message", "message1"),
                        hasEntry("x-opaque-id", "ID1"))
                    )
                );
            }
        });

        // For the same key and different X-Opaque-ID should be multiple times per key/x-opaque-id
        //continuing with message1-ID1 in logs already, adding a new deprecation log line with message2-ID2
        withThreadContext(threadContext -> {
            threadContext.putHeader(Task.X_OPAQUE_ID, "ID2");
            deprecationLogger.deprecate("key", "message1");
            deprecationLogger.deprecate("key", "message2");
            assertWarnings("message1", "message2");

            final Path path = PathUtils.get(
                System.getProperty("havenask.logs.base_path"),
                System.getProperty("havenask.logs.cluster_name") + "_deprecated.json"
            );
            try (Stream<Map<String, String>> stream = JsonLogsStream.mapStreamFrom(path)) {
                List<Map<String, String>> jsonLogs = stream.collect(Collectors.toList());

                assertThat(
                    jsonLogs,
                    contains(
                        allOf(
                            hasEntry("type", "deprecation"),
                            hasEntry("level", "DEPRECATION"),
                            hasEntry("component", "d.test"),
                            hasEntry("cluster.name", "havenask"),
                            hasEntry("node.name", "sample-name"),
                            hasEntry("message", "message1"),
                            hasEntry("x-opaque-id", "ID1")
                        ),
                        allOf(
                            hasEntry("type", "deprecation"),
                            hasEntry("level", "DEPRECATION"),
                            hasEntry("component", "d.test"),
                            hasEntry("cluster.name", "havenask"),
                            hasEntry("node.name", "sample-name"),
                            hasEntry("message", "message1"),
                            hasEntry("x-opaque-id", "ID2")
                        )
                    )
                );
            }
        });
    }

    private List<JsonLogLine> collectLines(Stream<JsonLogLine> stream) {
        return stream
            .skip(1)//skip the first line from super class
            .collect(Collectors.toList());
    }

    private Path clusterLogsPath() {
        return PathUtils.get(System.getProperty("havenask.logs.base_path"), System.getProperty("havenask.logs.cluster_name") + ".log");
    }

    private void setupLogging(final String config) throws IOException, UserException {
        setupLogging(config, Settings.EMPTY);
    }

    private void setupLogging(final String config, final Settings settings) throws IOException, UserException {
        assertFalse("Environment path.home variable should not be set", Environment.PATH_HOME_SETTING.exists(settings));
        final Path configDir = getDataPath(config);
        final Settings mergedSettings = Settings.builder()
                                                .put(settings)
                                                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                                                .build();
        // need to use custom config path so we can use a custom log4j2.properties file for the test
        final Environment environment = new Environment(mergedSettings, configDir);
        LogConfigurator.configure(environment);
    }

    private Matcher<JsonLogLine> logLine(String type, Level level, String nodeName, String component, String message) {
        return new FeatureMatcher<JsonLogLine, Boolean>(Matchers.is(true), "logLine", "logLine") {

            @Override
            protected Boolean featureValueOf(JsonLogLine actual) {
                return Objects.equals(actual.type(), type) &&
                    Objects.equals(actual.level(), level.toString()) &&
                    Objects.equals(actual.nodeName(), nodeName) &&
                    Objects.equals(actual.component(), component) &&
                    Objects.equals(actual.message(), message);
            }
        };
    }

    private Matcher<JsonLogLine> stacktraceWith(String... lines) {
        return new FeatureMatcher<JsonLogLine, List<String>>(Matchers.hasItems(lines),
            "stacktrace", "stacktrace") {

            @Override
            protected List<String> featureValueOf(JsonLogLine actual) {
                return actual.stacktrace();
            }
        };
    }

    private void withThreadContext(CheckedConsumer<ThreadContext, Exception> consumer) throws Exception {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            HeaderWarning.setThreadContext(threadContext);
            consumer.accept(threadContext);
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }
}
