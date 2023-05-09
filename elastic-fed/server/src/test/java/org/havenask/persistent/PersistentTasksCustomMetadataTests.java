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

package org.havenask.persistent;

import org.havenask.ResourceNotFoundException;
import org.havenask.Version;
import org.havenask.client.transport.TransportClient;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.Diff;
import org.havenask.cluster.NamedDiff;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.metadata.Metadata.Custom;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.common.ParseField;
import org.havenask.common.UUIDs;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.io.stream.NamedWriteableAwareStreamInput;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.NamedWriteableRegistry.Entry;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;
import org.havenask.persistent.PersistentTasksCustomMetadata.Assignment;
import org.havenask.persistent.PersistentTasksCustomMetadata.Builder;
import org.havenask.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.havenask.persistent.TestPersistentTasksPlugin.State;
import org.havenask.persistent.TestPersistentTasksPlugin.TestParams;
import org.havenask.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.havenask.test.AbstractDiffableSerializationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import static org.havenask.cluster.metadata.Metadata.CONTEXT_MODE_GATEWAY;
import static org.havenask.cluster.metadata.Metadata.CONTEXT_MODE_SNAPSHOT;
import static org.havenask.persistent.PersistentTasksExecutor.NO_NODE_FOUND;
import static org.havenask.test.VersionUtils.allReleasedVersions;
import static org.havenask.test.VersionUtils.compatibleFutureVersion;
import static org.havenask.test.VersionUtils.getFirstVersion;
import static org.havenask.test.VersionUtils.getPreviousVersion;
import static org.havenask.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class PersistentTasksCustomMetadataTests extends AbstractDiffableSerializationTestCase<Custom> {

    @Override
    protected PersistentTasksCustomMetadata createTestInstance() {
        int numberOfTasks = randomInt(10);
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();
        for (int i = 0; i < numberOfTasks; i++) {
            String taskId = UUIDs.base64UUID();
            tasks.addTask(taskId, TestPersistentTasksExecutor.NAME, new TestParams(randomAlphaOfLength(10)),
                    randomAssignment());
            if (randomBoolean()) {
                // From time to time update status
                tasks.updateTaskState(taskId, new State(randomAlphaOfLength(10)));
            }
        }
        return tasks.build();
    }

    @Override
    protected Writeable.Reader<Custom> instanceReader() {
        return PersistentTasksCustomMetadata::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Arrays.asList(
                new Entry(Metadata.Custom.class, PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata::new),
                new Entry(NamedDiff.class, PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata::readDiffFrom),
                new Entry(PersistentTaskParams.class, TestPersistentTasksExecutor.NAME, TestParams::new),
                new Entry(PersistentTaskState.class, TestPersistentTasksExecutor.NAME, State::new)
        ));
    }

    @Override
    protected Custom makeTestChanges(Custom testInstance) {
        Builder builder = PersistentTasksCustomMetadata.builder((PersistentTasksCustomMetadata) testInstance);
        switch (randomInt(3)) {
            case 0:
                addRandomTask(builder);
                break;
            case 1:
                if (builder.getCurrentTaskIds().isEmpty()) {
                    addRandomTask(builder);
                } else {
                    builder.reassignTask(pickRandomTask(builder), randomAssignment());
                }
                break;
            case 2:
                if (builder.getCurrentTaskIds().isEmpty()) {
                    addRandomTask(builder);
                } else {
                    builder.updateTaskState(pickRandomTask(builder), randomBoolean() ? new State(randomAlphaOfLength(10)) : null);
                }
                break;
            case 3:
                if (builder.getCurrentTaskIds().isEmpty()) {
                    addRandomTask(builder);
                } else {
                    builder.removeTask(pickRandomTask(builder));
                }
                break;
        }
        return builder.build();
    }

    @Override
    protected Writeable.Reader<Diff<Custom>> diffReader() {
        return PersistentTasksCustomMetadata::readDiffFrom;
    }

    @Override
    protected PersistentTasksCustomMetadata doParseInstance(XContentParser parser) {
        return PersistentTasksCustomMetadata.fromXContent(parser);
    }

    private String addRandomTask(Builder builder) {
        String taskId = UUIDs.base64UUID();
        builder.addTask(taskId, TestPersistentTasksExecutor.NAME, new TestParams(randomAlphaOfLength(10)), randomAssignment());
        return taskId;
    }

    private String pickRandomTask(PersistentTasksCustomMetadata.Builder testInstance) {
        return randomFrom(new ArrayList<>(testInstance.getCurrentTaskIds()));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(Arrays.asList(
                new NamedXContentRegistry.Entry(PersistentTaskParams.class,
                    new ParseField(TestPersistentTasksExecutor.NAME), TestParams::fromXContent),
                new NamedXContentRegistry.Entry(PersistentTaskState.class,
                    new ParseField(TestPersistentTasksExecutor.NAME), State::fromXContent)
        ));
    }

    @SuppressWarnings("unchecked")
    public void testSerializationContext() throws Exception {
        PersistentTasksCustomMetadata testInstance = createTestInstance();
        for (int i = 0; i < randomInt(10); i++) {
            testInstance = (PersistentTasksCustomMetadata) makeTestChanges(testInstance);
        }

        ToXContent.MapParams params = new ToXContent.MapParams(
                Collections.singletonMap(Metadata.CONTEXT_MODE_PARAM, randomFrom(CONTEXT_MODE_SNAPSHOT, CONTEXT_MODE_GATEWAY)));

        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference shuffled = toShuffledXContent(testInstance, xContentType, params, false);

        PersistentTasksCustomMetadata newInstance;
        try (XContentParser parser = createParser(XContentFactory.xContent(xContentType), shuffled)) {
            newInstance = doParseInstance(parser);
        }
        assertNotSame(newInstance, testInstance);

        assertEquals(testInstance.tasks().size(), newInstance.tasks().size());
        for (PersistentTask<?> testTask : testInstance.tasks()) {
            PersistentTask<TestParams> newTask = (PersistentTask<TestParams>) newInstance.getTask(testTask.getId());
            assertNotNull(newTask);

            // Things that should be serialized
            assertEquals(testTask.getTaskName(), newTask.getTaskName());
            assertEquals(testTask.getId(), newTask.getId());
            assertEquals(testTask.getState(), newTask.getState());
            assertEquals(testTask.getParams(), newTask.getParams());

            // Things that shouldn't be serialized
            assertEquals(0, newTask.getAllocationId());
            assertNull(newTask.getExecutorNode());
        }
    }

    public void testBuilder() {
        PersistentTasksCustomMetadata persistentTasks = null;
        String lastKnownTask = "";
        for (int i = 0; i < randomIntBetween(10, 100); i++) {
            final Builder builder;
            if (randomBoolean()) {
                builder = PersistentTasksCustomMetadata.builder();
            } else {
                builder = PersistentTasksCustomMetadata.builder(persistentTasks);
            }
            boolean changed = false;
            for (int j = 0; j < randomIntBetween(1, 10); j++) {
                switch (randomInt(3)) {
                    case 0:
                        lastKnownTask = addRandomTask(builder);
                        changed = true;
                        break;
                    case 1:
                        if (builder.hasTask(lastKnownTask)) {
                            changed = true;
                            builder.reassignTask(lastKnownTask, randomAssignment());
                        } else {
                            String fLastKnownTask = lastKnownTask;
                            expectThrows(ResourceNotFoundException.class, () -> builder.reassignTask(fLastKnownTask, randomAssignment()));
                        }
                        break;
                    case 2:
                        if (builder.hasTask(lastKnownTask)) {
                            changed = true;
                            builder.updateTaskState(lastKnownTask, randomBoolean() ? new State(randomAlphaOfLength(10)) : null);
                        } else {
                            String fLastKnownTask = lastKnownTask;
                            expectThrows(ResourceNotFoundException.class, () -> builder.updateTaskState(fLastKnownTask, null));
                        }
                        break;
                    case 3:
                        if (builder.hasTask(lastKnownTask)) {
                            changed = true;
                            builder.removeTask(lastKnownTask);
                        } else {
                            String fLastKnownTask = lastKnownTask;
                            expectThrows(ResourceNotFoundException.class, () -> builder.removeTask(fLastKnownTask));
                        }
                        break;
                }
            }
            assertEquals(changed, builder.isChanged());
            persistentTasks = builder.build();
        }
    }

    public void testMinVersionSerialization() throws IOException {
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();

        Version minVersion = allReleasedVersions().stream().filter(Version::isRelease).findFirst().orElseThrow(NoSuchElementException::new);
        final Version streamVersion = randomVersionBetween(random(), minVersion, getPreviousVersion(Version.CURRENT));
        tasks.addTask("test_compatible_version", TestPersistentTasksExecutor.NAME,
            new TestParams(null, randomVersionBetween(random(), minVersion, streamVersion),
                randomBoolean() ? Optional.empty() : Optional.of("test")),
            randomAssignment());
        tasks.addTask("test_incompatible_version", TestPersistentTasksExecutor.NAME,
            new TestParams(null, randomVersionBetween(random(), compatibleFutureVersion(streamVersion), Version.CURRENT),
                randomBoolean() ? Optional.empty() : Optional.of("test")),
            randomAssignment());
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(streamVersion);
        Set<String> features = new HashSet<>();
        final boolean transportClient = randomBoolean();
        if (transportClient) {
            features.add(TransportClient.TRANSPORT_CLIENT_FEATURE);
        }
        // if a transport client, then it must have the feature otherwise we add the feature randomly
        if (transportClient || randomBoolean()) {
            features.add("test");
        }
        out.setFeatures(features);
        tasks.build().writeTo(out);

        final StreamInput input = out.bytes().streamInput();
        input.setVersion(streamVersion);
        PersistentTasksCustomMetadata read =
            new PersistentTasksCustomMetadata(new NamedWriteableAwareStreamInput(input, getNamedWriteableRegistry()));

        assertThat(read.taskMap().keySet(), equalTo(Collections.singleton("test_compatible_version")));
    }

    public void testFeatureSerialization() throws IOException {
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();

        Version minVersion = getFirstVersion();
        tasks.addTask("test_compatible", TestPersistentTasksExecutor.NAME,
            new TestParams(null, randomVersionBetween(random(), minVersion, Version.CURRENT),
                randomBoolean() ? Optional.empty() : Optional.of("existing")),
            randomAssignment());
        tasks.addTask("test_incompatible", TestPersistentTasksExecutor.NAME,
            new TestParams(null, randomVersionBetween(random(), minVersion, Version.CURRENT), Optional.of("non_existing")),
            randomAssignment());
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.CURRENT);
        Set<String> features = new HashSet<>();
        features.add("existing");
        features.add(TransportClient.TRANSPORT_CLIENT_FEATURE);
        out.setFeatures(features);
        tasks.build().writeTo(out);

        PersistentTasksCustomMetadata read = new PersistentTasksCustomMetadata(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), getNamedWriteableRegistry()));
        assertThat(read.taskMap().keySet(), equalTo(Collections.singleton("test_compatible")));
    }

    public void testDisassociateDeadNodes_givenNoPersistentTasks() {
        ClusterState originalState = ClusterState.builder(new ClusterName("persistent-tasks-tests")).build();
        ClusterState returnedState = PersistentTasksCustomMetadata.disassociateDeadNodes(originalState);
        assertThat(originalState, sameInstance(returnedState));
    }

    public void testDisassociateDeadNodes_givenAssignedPersistentTask() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT))
                .localNodeId("node1")
                .masterNodeId("node1")
                .build();

        String taskName = "test/task";
        PersistentTasksCustomMetadata.Builder tasksBuilder =  PersistentTasksCustomMetadata.builder()
                .addTask("task-id", taskName, emptyTaskParams(taskName),
                        new PersistentTasksCustomMetadata.Assignment("node1", "test assignment"));

        ClusterState originalState = ClusterState.builder(new ClusterName("persistent-tasks-tests"))
                .nodes(nodes)
                .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
                .build();
        ClusterState returnedState = PersistentTasksCustomMetadata.disassociateDeadNodes(originalState);
        assertThat(originalState, sameInstance(returnedState));

        PersistentTasksCustomMetadata originalTasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(originalState);
        PersistentTasksCustomMetadata returnedTasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(returnedState);
        assertEquals(originalTasks, returnedTasks);
    }

    public void testDisassociateDeadNodes() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT))
                .localNodeId("node1")
                .masterNodeId("node1")
                .build();

        String taskName = "test/task";
        PersistentTasksCustomMetadata.Builder tasksBuilder =  PersistentTasksCustomMetadata.builder()
                .addTask("assigned-task", taskName, emptyTaskParams(taskName),
                        new PersistentTasksCustomMetadata.Assignment("node1", "test assignment"))
                .addTask("task-on-deceased-node", taskName, emptyTaskParams(taskName),
                new PersistentTasksCustomMetadata.Assignment("left-the-cluster", "test assignment"));

        ClusterState originalState = ClusterState.builder(new ClusterName("persistent-tasks-tests"))
                .nodes(nodes)
                .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
                .build();
        ClusterState returnedState = PersistentTasksCustomMetadata.disassociateDeadNodes(originalState);
        assertThat(originalState, not(sameInstance(returnedState)));

        PersistentTasksCustomMetadata originalTasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(originalState);
        PersistentTasksCustomMetadata returnedTasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(returnedState);
        assertNotEquals(originalTasks, returnedTasks);

        assertEquals(originalTasks.getTask("assigned-task"), returnedTasks.getTask("assigned-task"));
        assertNotEquals(originalTasks.getTask("task-on-deceased-node"), returnedTasks.getTask("task-on-deceased-node"));
        assertEquals(PersistentTasksCustomMetadata.LOST_NODE_ASSIGNMENT, returnedTasks.getTask("task-on-deceased-node").getAssignment());
    }

    private PersistentTaskParams emptyTaskParams(String taskName) {
        return new PersistentTaskParams() {

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) {
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) {

            }

            @Override
            public String getWriteableName() {
                return taskName;
            }

            @Override
            public Version getMinimalSupportedVersion() {
                return Version.CURRENT;
            }
        };
    }

    private Assignment randomAssignment() {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return NO_NODE_FOUND;
            } else {
                return new Assignment(null, randomAlphaOfLength(10));
            }
        }
        return new Assignment(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }
}
