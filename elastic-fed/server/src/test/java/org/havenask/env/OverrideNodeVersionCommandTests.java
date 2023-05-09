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

package org.havenask.env;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.havenask.HavenaskException;
import org.havenask.Version;
import org.havenask.cli.MockTerminal;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.BigArrays;
import org.havenask.gateway.PersistedClusterStateService;
import org.havenask.test.HavenaskTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class OverrideNodeVersionCommandTests extends HavenaskTestCase {

    private Environment environment;
    private Path[] nodePaths;
    private String nodeId;
    private final OptionSet noOptions = new OptionParser().parse();

    @Before
    public void createNodePaths() throws IOException {
        final Settings settings = buildEnvSettings(Settings.EMPTY);
        environment = TestEnvironment.newEnvironment(settings);
        try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, environment)) {
            nodePaths = nodeEnvironment.nodeDataPaths();
            nodeId = nodeEnvironment.nodeId();

            try (PersistedClusterStateService.Writer writer = new PersistedClusterStateService(nodePaths, nodeId,
                xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L).createWriter()) {
                writer.writeFullStateAndCommit(1L, ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder()
                    .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build()).build())
                    .build());
            }
        }
    }

    @After
    public void checkClusterStateIntact() throws IOException {
        assertTrue(Metadata.SETTING_READ_ONLY_SETTING.get(new PersistedClusterStateService(nodePaths, nodeId,
            xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L)
            .loadBestOnDiskState().metadata.persistentSettings()));
    }

    public void testFailsOnEmptyPath() {
        final Path emptyPath = createTempDir();
        final MockTerminal mockTerminal = new MockTerminal();
        final HavenaskException havenaskException = expectThrows(HavenaskException.class, () ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, new Path[]{emptyPath}, 0, noOptions, environment));
        assertThat(havenaskException.getMessage(), equalTo(OverrideNodeVersionCommand.NO_METADATA_MESSAGE));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));
    }

    public void testFailsIfUnnecessary() throws IOException {
        final Version nodeVersion = Version.fromId(between(Version.CURRENT.minimumIndexCompatibilityVersion().id, Version.CURRENT.id));
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        final HavenaskException havenaskException = expectThrows(HavenaskException.class, () ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, 0, noOptions, environment));
        assertThat(havenaskException.getMessage(), allOf(
            containsString("compatible with current version"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString())));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));
    }

    public void testWarnsIfTooOld() throws Exception {
        final Version nodeVersion = NodeMetadataTests.tooOldVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput("n\n");
        final HavenaskException havenaskException = expectThrows(HavenaskException.class, () ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, 0, noOptions, environment));
        assertThat(havenaskException.getMessage(), equalTo("aborted by user"));
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("too old"),
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString())));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePaths);
        assertThat(nodeMetadata.nodeVersion(), equalTo(nodeVersion));
    }

    public void testWarnsIfTooNew() throws Exception {
        final Version nodeVersion = NodeMetadataTests.tooNewVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput(randomFrom("yy", "Yy", "n", "yes", "true", "N", "no"));
        final HavenaskException havenaskException = expectThrows(HavenaskException.class, () ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, 0, noOptions, environment));
        assertThat(havenaskException.getMessage(), equalTo("aborted by user"));
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString())));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePaths);
        assertThat(nodeMetadata.nodeVersion(), equalTo(nodeVersion));
    }

    public void testOverwritesIfTooOld() throws Exception {
        final Version nodeVersion = NodeMetadataTests.tooOldVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput(randomFrom("y", "Y"));
        new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, 0, noOptions, environment);
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("too old"),
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString()),
            containsString(OverrideNodeVersionCommand.SUCCESS_MESSAGE)));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePaths);
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.CURRENT));
    }

    public void testOverwritesIfTooNew() throws Exception {
        final Version nodeVersion = NodeMetadataTests.tooNewVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePaths);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput(randomFrom("y", "Y"));
        new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePaths, 0, noOptions, environment);
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString()),
            containsString(OverrideNodeVersionCommand.SUCCESS_MESSAGE)));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePaths);
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.CURRENT));
    }
}