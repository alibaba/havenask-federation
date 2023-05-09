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

package org.havenask.monitor.fs;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.mockfile.FilterFileChannel;
import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.havenask.cluster.coordination.DeterministicTaskQueue;
import org.havenask.common.io.PathUtils;
import org.havenask.common.io.PathUtilsForTesting;
import org.havenask.common.logging.Loggers;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.env.NodeEnvironment;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.MockLogAppender;
import org.havenask.test.junit.annotations.TestLogging;
import org.havenask.threadpool.TestThreadPool;
import org.havenask.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.OpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.havenask.monitor.StatusInfo.Status.HEALTHY;
import static org.havenask.monitor.StatusInfo.Status.UNHEALTHY;
import static org.havenask.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.is;

public class FsHealthServiceTests extends HavenaskTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;

    @Before
    public void createObjects() {
        Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
    }

    public void testSchedulesHealthCheckAtRefreshIntervals() throws Exception {
        long refreshInterval = randomLongBetween(1000, 12000);
        final Settings settings = Settings.builder().put(FsHealthService.REFRESH_INTERVAL_SETTING.getKey(), refreshInterval + "ms").build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, deterministicTaskQueue.getThreadPool(), env);
            final long startTimeMillis = deterministicTaskQueue.getCurrentTimeMillis();
            fsHealthService.doStart();
            assertFalse(deterministicTaskQueue.hasRunnableTasks());
            assertTrue(deterministicTaskQueue.hasDeferredTasks());
            int rescheduledCount = 0;
            for (int i = 1; i <= randomIntBetween(5, 10); i++) {
                if (deterministicTaskQueue.hasRunnableTasks()) {
                    deterministicTaskQueue.runRandomTask();
                } else {
                    assertThat(deterministicTaskQueue.getLatestDeferredExecutionTime(), is(refreshInterval * (rescheduledCount + 1)));
                    deterministicTaskQueue.advanceTime();
                    rescheduledCount++;
                }
                assertThat(deterministicTaskQueue.getCurrentTimeMillis() - startTimeMillis, is(refreshInterval * rescheduledCount));
            }

            fsHealthService.doStop();
            deterministicTaskQueue.runAllTasksInTimeOrder();

            assertFalse(deterministicTaskQueue.hasRunnableTasks());
            assertFalse(deterministicTaskQueue.hasDeferredTasks());
        }
    }

    public void testFailsHealthOnIOException() throws IOException {
        FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        FileSystemIOExceptionProvider disruptFileSystemProvider = new FileSystemIOExceptionProvider(fileSystem);
        fileSystem = disruptFileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem);
        final Settings settings = Settings.EMPTY;
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        TestThreadPool testThreadPool = new TestThreadPool(getClass().getName(), settings);
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(HEALTHY, fsHealthService.getHealth().getStatus());
            assertEquals("health check passed", fsHealthService.getHealth().getInfo());

            //disrupt file system
            disruptFileSystemProvider.restrictPathPrefix(""); // disrupt all paths
            disruptFileSystemProvider.injectIOException.set(true);
            fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(UNHEALTHY, fsHealthService.getHealth().getStatus());
            for (Path path : env.nodeDataPaths()) {
                assertTrue(fsHealthService.getHealth().getInfo().contains(path.toString()));
            }
            assertEquals(env.nodeDataPaths().length, disruptFileSystemProvider.getInjectedPathCount());
        } finally {
            disruptFileSystemProvider.injectIOException.set(false);
            PathUtilsForTesting.teardown();
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    @TestLogging(value = "org.havenask.monitor.fs:WARN", reason = "to ensure that we log on hung IO at WARN level")
    public void testLoggingOnHungIO() throws Exception {
        long slowLogThreshold = randomLongBetween(100, 200);
        final Settings settings = Settings.builder().put(FsHealthService.SLOW_PATH_LOGGING_THRESHOLD_SETTING.getKey(),
            slowLogThreshold + "ms").build();
        FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        TestThreadPool testThreadPool = new TestThreadPool(getClass().getName(), settings);
        FileSystemFsyncHungProvider disruptFileSystemProvider = new FileSystemFsyncHungProvider(fileSystem,
            randomLongBetween(slowLogThreshold + 1 , 400), testThreadPool);
        fileSystem = disruptFileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();

        Logger logger = LogManager.getLogger(FsHealthService.class);
        Loggers.addAppender(logger, mockAppender);
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            int counter = 0;
            for(Path path : env.nodeDataPaths()){
                mockAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "test" + ++counter,
                        FsHealthService.class.getCanonicalName(),
                        Level.WARN,
                        "health check of [" + path + "] took [*ms] which is above the warn threshold*"));
            }

            //disrupt file system
            disruptFileSystemProvider.injectIOException.set(true);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(env.nodeDataPaths().length, disruptFileSystemProvider.getInjectedPathCount());
            assertBusy(mockAppender::assertAllExpectationsMatched);
        } finally {
            Loggers.removeAppender(logger, mockAppender);
            mockAppender.stop();
            PathUtilsForTesting.teardown();
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    public void testFailsHealthOnSinglePathFsyncFailure() throws IOException {
        FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        FileSystemFsyncIOExceptionProvider disruptFsyncFileSystemProvider = new FileSystemFsyncIOExceptionProvider(fileSystem);
        fileSystem = disruptFsyncFileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem);
        final Settings settings = Settings.EMPTY;
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        TestThreadPool testThreadPool = new TestThreadPool(getClass().getName(), settings);
        try (NodeEnvironment env = newNodeEnvironment()) {
            Path[] paths = env.nodeDataPaths();
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(HEALTHY, fsHealthService.getHealth().getStatus());
            assertEquals("health check passed", fsHealthService.getHealth().getInfo());

            //disrupt file system fsync on single path
            disruptFsyncFileSystemProvider.injectIOException.set(true);
            String disruptedPath = randomFrom(paths).toString();
            disruptFsyncFileSystemProvider.restrictPathPrefix(disruptedPath);
            fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(UNHEALTHY, fsHealthService.getHealth().getStatus());
            assertThat(fsHealthService.getHealth().getInfo(), is("health check failed on [" + disruptedPath + "]"));
            assertEquals(1, disruptFsyncFileSystemProvider.getInjectedPathCount());
        } finally {
            disruptFsyncFileSystemProvider.injectIOException.set(false);
            PathUtilsForTesting.teardown();
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    public void testFailsHealthOnSinglePathWriteFailure() throws IOException {
        FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        FileSystemIOExceptionProvider disruptWritesFileSystemProvider = new FileSystemIOExceptionProvider(fileSystem);
        fileSystem = disruptWritesFileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem);
        final Settings settings = Settings.EMPTY;
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        TestThreadPool testThreadPool = new TestThreadPool(getClass().getName(), settings);
        try (NodeEnvironment env = newNodeEnvironment()) {
            Path[] paths = env.nodeDataPaths();
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(HEALTHY, fsHealthService.getHealth().getStatus());
            assertEquals("health check passed", fsHealthService.getHealth().getInfo());

            //disrupt file system writes on single path
            String disruptedPath = randomFrom(paths).toString();
            disruptWritesFileSystemProvider.restrictPathPrefix(disruptedPath);
            disruptWritesFileSystemProvider.injectIOException.set(true);
            fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(UNHEALTHY, fsHealthService.getHealth().getStatus());
            assertThat(fsHealthService.getHealth().getInfo(), is("health check failed on [" + disruptedPath + "]"));
            assertEquals(1, disruptWritesFileSystemProvider.getInjectedPathCount());
        } finally {
            disruptWritesFileSystemProvider.injectIOException.set(false);
            PathUtilsForTesting.teardown();
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    public void testFailsHealthOnUnexpectedLockFileSize() throws IOException {
        FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        final Settings settings = Settings.EMPTY;
        TestThreadPool testThreadPool = new TestThreadPool(getClass().getName(), settings);
        FileSystemUnexpectedLockFileSizeProvider unexpectedLockFileSizeFileSystemProvider = new FileSystemUnexpectedLockFileSizeProvider(
            fileSystem, 1, testThreadPool);
        fileSystem = unexpectedLockFileSizeFileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(HEALTHY, fsHealthService.getHealth().getStatus());
            assertEquals("health check passed", fsHealthService.getHealth().getInfo());

            // enabling unexpected file size injection
            unexpectedLockFileSizeFileSystemProvider.injectUnexpectedFileSize.set(true);

            fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(UNHEALTHY, fsHealthService.getHealth().getStatus());
            assertThat(fsHealthService.getHealth().getInfo(), is("health check failed due to broken node lock"));
            assertEquals(1, unexpectedLockFileSizeFileSystemProvider.getInjectedPathCount());
        } finally {
            unexpectedLockFileSizeFileSystemProvider.injectUnexpectedFileSize.set(false);
            PathUtilsForTesting.teardown();
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    private static class FileSystemIOExceptionProvider extends FilterFileSystemProvider {

        AtomicBoolean injectIOException = new AtomicBoolean();
        AtomicInteger injectedPaths = new AtomicInteger();

        private String pathPrefix;

        FileSystemIOExceptionProvider(FileSystem inner) {
            super("disrupt_fs_health://", inner);
        }

        public void restrictPathPrefix(String pathPrefix){
            this.pathPrefix = pathPrefix;
        }

        public int getInjectedPathCount(){
            return injectedPaths.get();
        }

        @Override
        public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
            if (injectIOException.get()){
                assert pathPrefix != null : "must set pathPrefix before starting disruptions";
                if (path.toString().startsWith(pathPrefix) && path.toString().
                    endsWith(FsHealthService.FsHealthMonitor.TEMP_FILE_NAME)) {
                    injectedPaths.incrementAndGet();
                    throw new IOException("fake IOException");
                }
            }
            return super.newOutputStream(path, options);
        }
    }

    private static class FileSystemFsyncIOExceptionProvider extends FilterFileSystemProvider {

        AtomicBoolean injectIOException = new AtomicBoolean();
        AtomicInteger injectedPaths = new AtomicInteger();

        private String pathPrefix = null;

        FileSystemFsyncIOExceptionProvider(FileSystem inner) {
            super("disrupt_fs_health://", inner);
        }

        public void restrictPathPrefix(String pathPrefix){
            this.pathPrefix = pathPrefix;
        }

        public int getInjectedPathCount(){
            return injectedPaths.get();
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {
                @Override
                public void force(boolean metaData) throws IOException {
                    if (injectIOException.get()) {
                        assert pathPrefix != null : "must set pathPrefix before starting disruptions";
                        if (path.toString().startsWith(pathPrefix) && path.toString().
                            endsWith(FsHealthService.FsHealthMonitor.TEMP_FILE_NAME)) {
                            injectedPaths.incrementAndGet();
                            throw new IOException("fake IOException");
                        }
                    }
                    super.force(metaData);
                }
            };
        }
    }

    private static class FileSystemFsyncHungProvider extends FilterFileSystemProvider {

        AtomicBoolean injectIOException = new AtomicBoolean();
        AtomicInteger injectedPaths = new AtomicInteger();

        private final long delay;
        private final ThreadPool threadPool;

        FileSystemFsyncHungProvider(FileSystem inner, long delay, ThreadPool threadPool) {
            super("disrupt_fs_health://", inner);
            this.delay = delay;
            this.threadPool = threadPool;
        }

        public int getInjectedPathCount(){
            return injectedPaths.get();
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {
                @Override
                public void force(boolean metaData) throws IOException {
                    if (injectIOException.get()) {
                        if (path.getFileName().toString().equals(FsHealthService.FsHealthMonitor.TEMP_FILE_NAME)) {
                            injectedPaths.incrementAndGet();
                            final long startTimeMillis = threadPool.relativeTimeInMillis();
                            do {
                                try {
                                    Thread.sleep(delay);
                                } catch (InterruptedException e) {
                                    throw new AssertionError(e);
                                }
                            } while (threadPool.relativeTimeInMillis() <= startTimeMillis + delay);
                        }
                    }
                    super.force(metaData);
                }
            };
        }
    }

    private static class FileSystemUnexpectedLockFileSizeProvider extends FilterFileSystemProvider {

        AtomicBoolean injectUnexpectedFileSize = new AtomicBoolean();
        AtomicInteger injectedPaths = new AtomicInteger();

        private final long size;
        private final ThreadPool threadPool;

        FileSystemUnexpectedLockFileSizeProvider(FileSystem inner, long size, ThreadPool threadPool) {
            super("disrupt_fs_health://", inner);
            this.size = size;
            this.threadPool = threadPool;
        }

        public int getInjectedPathCount(){
            return injectedPaths.get();
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {
                @Override
                public long size() throws IOException {
                    if (injectUnexpectedFileSize.get()) {
                        if (path.getFileName().toString().equals(NodeEnvironment.NODE_LOCK_FILENAME)) {
                            injectedPaths.incrementAndGet();
                            return size;
                        }
                    }
                    return super.size();
                }
            };
        }
    }
}
