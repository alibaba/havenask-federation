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

package org.havenask.cluster.coordination;

import joptsimple.OptionSet;
import org.havenask.HavenaskException;
import org.havenask.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.havenask.cli.MockTerminal;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.common.settings.Settings;
import org.havenask.discovery.DiscoverySettings;
import org.havenask.env.Environment;
import org.havenask.env.NodeEnvironment;
import org.havenask.env.TestEnvironment;
import org.havenask.gateway.GatewayMetaState;
import org.havenask.gateway.PersistedClusterStateService;
import org.havenask.indices.IndicesService;
import org.havenask.test.InternalTestCluster;
import org.havenask.test.HavenaskIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.havenask.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.havenask.gateway.DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING;
import static org.havenask.index.query.QueryBuilders.matchAllQuery;
import static org.havenask.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.havenask.test.NodeRoles.nonMasterNode;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertHitCount;

@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class UnsafeBootstrapAndDetachCommandIT extends HavenaskIntegTestCase {

    private MockTerminal executeCommand(HavenaskNodeCommand command, Environment environment, int nodeOrdinal,
                                        boolean abort, Boolean applyClusterReadOnlyBlock)
        throws Exception {
        final MockTerminal terminal = new MockTerminal();
        final OptionSet options;
        if (Objects.nonNull(applyClusterReadOnlyBlock)) {
            options = command.getParser().parse("-ordinal", Integer.toString(nodeOrdinal),
                "-apply-cluster-read-only-block", Boolean.toString(applyClusterReadOnlyBlock));
        } else {
            options = command.getParser().parse("-ordinal", Integer.toString(nodeOrdinal));
        }
        final String input;

        if (abort) {
            input = randomValueOtherThanMany(c -> c.equalsIgnoreCase("y"), () -> randomAlphaOfLength(1));
        } else {
            input = randomBoolean() ? "y" : "Y";
        }

        terminal.addTextInput(input);

        try {
            command.execute(terminal, options, environment);
        } finally {
            assertThat(terminal.getOutput(), containsString(HavenaskNodeCommand.STOP_WARNING_MSG));
        }

        return terminal;
    }

    private MockTerminal unsafeBootstrap(Environment environment, boolean abort, Boolean applyClusterReadOnlyBlock) throws Exception {
        final MockTerminal terminal = executeCommand(new UnsafeBootstrapMasterCommand(), environment, 0, abort, applyClusterReadOnlyBlock);
        assertThat(terminal.getOutput(), containsString(UnsafeBootstrapMasterCommand.CONFIRMATION_MSG));
        assertThat(terminal.getOutput(), containsString(UnsafeBootstrapMasterCommand.MASTER_NODE_BOOTSTRAPPED_MSG));
        return terminal;
    }

    private MockTerminal detachCluster(Environment environment, boolean abort) throws Exception {
        final MockTerminal terminal = executeCommand(new DetachClusterCommand(), environment, 0, abort, null);
        assertThat(terminal.getOutput(), containsString(DetachClusterCommand.CONFIRMATION_MSG));
        assertThat(terminal.getOutput(), containsString(DetachClusterCommand.NODE_DETACHED_MSG));
        return terminal;
    }

    private MockTerminal unsafeBootstrap(Environment environment) throws Exception {
        return unsafeBootstrap(environment, false, null);
    }

    private MockTerminal detachCluster(Environment environment) throws Exception {
        return detachCluster(environment, false);
    }

    private void expectThrows(ThrowingRunnable runnable, String message) {
        HavenaskException ex = expectThrows(HavenaskException.class, runnable);
        assertThat(ex.getMessage(), containsString(message));
    }

    private void ensureReadOnlyBlock(boolean expected_block_state, String node) {
        ClusterState state = internalCluster().client(node).admin().cluster().prepareState().setLocal(true)
            .execute().actionGet().getState();
        boolean current_block_state = state.metadata().persistentSettings().
            getAsBoolean(Metadata.SETTING_READ_ONLY_SETTING.getKey(), false);
        if(current_block_state != expected_block_state) {
            fail(String.format(Locale.ROOT, "Read only setting for the node %s don't match. Expected %s. Actual %s", node,
                expected_block_state, current_block_state));
        }
    }

    private void removeBlock() {
        if (isInternalCluster()) {
            Settings settings = Settings.builder().putNull(Metadata.SETTING_READ_ONLY_SETTING.getKey()).build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get());
        }
    }

    public void testBootstrapNotMasterEligible() {
        final Environment environment = TestEnvironment.newEnvironment(Settings.builder()
            .put(nonMasterNode(internalCluster().getDefaultSettings()))
            .build());
        expectThrows(() -> unsafeBootstrap(environment), UnsafeBootstrapMasterCommand.NOT_MASTER_NODE_MSG);
    }

    public void testBootstrapNoDataFolder() {
        final Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());
        expectThrows(() -> unsafeBootstrap(environment), HavenaskNodeCommand.NO_NODE_FOLDER_FOUND_MSG);
    }

    public void testDetachNoDataFolder() {
        final Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());
        expectThrows(() -> detachCluster(environment), HavenaskNodeCommand.NO_NODE_FOLDER_FOUND_MSG);
    }

    public void testBootstrapNodeLocked() throws IOException {
        Settings envSettings = buildEnvSettings(Settings.EMPTY);
        Environment environment = TestEnvironment.newEnvironment(envSettings);
        try (NodeEnvironment ignored = new NodeEnvironment(envSettings, environment)) {
            expectThrows(() -> unsafeBootstrap(environment), HavenaskNodeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);
        }
    }

    public void testDetachNodeLocked() throws IOException {
        Settings envSettings = buildEnvSettings(Settings.EMPTY);
        Environment environment = TestEnvironment.newEnvironment(envSettings);
        try (NodeEnvironment ignored = new NodeEnvironment(envSettings, environment)) {
            expectThrows(() -> detachCluster(environment), HavenaskNodeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);
        }
    }

    public void testBootstrapNoNodeMetadata() {
        Settings envSettings = buildEnvSettings(Settings.EMPTY);
        Environment environment = TestEnvironment.newEnvironment(envSettings);
        expectThrows(() -> unsafeBootstrap(environment), HavenaskNodeCommand.NO_NODE_FOLDER_FOUND_MSG);
    }

    public void testBootstrapNotBootstrappedCluster() throws Exception {
        String node = internalCluster().startNode(
            Settings.builder()
                .put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s") // to ensure quick node startup
                .build());
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().setLocal(true)
                .execute().actionGet().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));
        });

        Settings dataPathSettings = internalCluster().dataPathSettings(node);

        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build());
        expectThrows(() -> unsafeBootstrap(environment), UnsafeBootstrapMasterCommand.EMPTY_LAST_COMMITTED_VOTING_CONFIG_MSG);
    }

    public void testBootstrapNoClusterState() throws IOException {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        NodeEnvironment nodeEnvironment = internalCluster().getMasterNodeInstance(NodeEnvironment.class);
        internalCluster().stopRandomDataNode();
        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build());
        PersistedClusterStateService.deleteAll(nodeEnvironment.nodeDataPaths());

        expectThrows(() -> unsafeBootstrap(environment), HavenaskNodeCommand.NO_NODE_METADATA_FOUND_MSG);
    }

    public void testDetachNoClusterState() throws IOException {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        NodeEnvironment nodeEnvironment = internalCluster().getMasterNodeInstance(NodeEnvironment.class);
        internalCluster().stopRandomDataNode();
        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build());
        PersistedClusterStateService.deleteAll(nodeEnvironment.nodeDataPaths());

        expectThrows(() -> detachCluster(environment), HavenaskNodeCommand.NO_NODE_METADATA_FOUND_MSG);
    }

    public void testBootstrapAbortedByUser() throws IOException {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build());
        expectThrows(() -> unsafeBootstrap(environment, true, null), HavenaskNodeCommand.ABORTED_BY_USER_MSG);
    }

    public void testDetachAbortedByUser() throws IOException {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String node = internalCluster().startNode();
        Settings dataPathSettings = internalCluster().dataPathSettings(node);
        ensureStableCluster(1);
        internalCluster().stopRandomDataNode();

        Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build());
        expectThrows(() -> detachCluster(environment, true), HavenaskNodeCommand.ABORTED_BY_USER_MSG);
    }

    public void test3MasterNodes2Failed() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(2);
        List<String> masterNodes = new ArrayList<>();

        logger.info("--> start 1st master-eligible node");
        masterNodes.add(internalCluster().startMasterOnlyNode(Settings.builder()
            .put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s")
            .build())); // node ordinal 0

        logger.info("--> start one data-only node");
        String dataNode = internalCluster().startDataOnlyNode(Settings.builder()
            .put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s")
            .build()); // node ordinal 1

        logger.info("--> start 2nd and 3rd master-eligible nodes and bootstrap");
        masterNodes.addAll(internalCluster().startMasterOnlyNodes(2)); // node ordinals 2 and 3

        logger.info("--> wait for all nodes to join the cluster");
        ensureStableCluster(4);

        List<String> currentClusterNodes = new ArrayList<>(masterNodes);
        currentClusterNodes.add(dataNode);
        currentClusterNodes.forEach(node-> ensureReadOnlyBlock(false, node));

        logger.info("--> create index test");
        createIndex("test");
        ensureGreen("test");

        Settings master1DataPathSettings = internalCluster().dataPathSettings(masterNodes.get(0));
        Settings master2DataPathSettings = internalCluster().dataPathSettings(masterNodes.get(1));
        Settings master3DataPathSettings = internalCluster().dataPathSettings(masterNodes.get(2));
        Settings dataNodeDataPathSettings = internalCluster().dataPathSettings(dataNode);

        logger.info("--> stop 2nd and 3d master eligible node");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodes.get(1)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodes.get(2)));

        logger.info("--> ensure NO_MASTER_BLOCK on data-only node");
        assertBusy(() -> {
            ClusterState state = internalCluster().client(dataNode).admin().cluster().prepareState().setLocal(true)
                .execute().actionGet().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));
        });

        logger.info("--> try to unsafely bootstrap 1st master-eligible node, while node lock is held");
        Environment environmentMaster1 = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(master1DataPathSettings).build());
        expectThrows(() -> unsafeBootstrap(environmentMaster1), UnsafeBootstrapMasterCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG);

        logger.info("--> stop 1st master-eligible node and data-only node");
        NodeEnvironment nodeEnvironment = internalCluster().getMasterNodeInstance(NodeEnvironment.class);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodes.get(0)));
        assertBusy(() -> internalCluster().getInstance(GatewayMetaState.class, dataNode).allPendingAsyncStatesWritten());
        internalCluster().stopRandomDataNode();

        logger.info("--> unsafely-bootstrap 1st master-eligible node");
        MockTerminal terminal = unsafeBootstrap(environmentMaster1, false, true);
        Metadata metadata = HavenaskNodeCommand.createPersistedClusterStateService(Settings.EMPTY, nodeEnvironment.nodeDataPaths())
            .loadBestOnDiskState().metadata;
        assertThat(terminal.getOutput(), containsString(
            String.format(Locale.ROOT, UnsafeBootstrapMasterCommand.CLUSTER_STATE_TERM_VERSION_MSG_FORMAT,
                metadata.coordinationMetadata().term(), metadata.version())));

        logger.info("--> start 1st master-eligible node");
        String masterNode2 = internalCluster().startMasterOnlyNode(master1DataPathSettings);

        logger.info("--> detach-cluster on data-only node");
        Environment environmentData = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataNodeDataPathSettings).build());
        detachCluster(environmentData, false);

        logger.info("--> start data-only node");
        String dataNode2 = internalCluster().startDataOnlyNode(dataNodeDataPathSettings);

        logger.info("--> ensure there is no NO_MASTER_BLOCK and unsafe-bootstrap is reflected in cluster state");
        assertBusy(() -> {
            ClusterState state = internalCluster().client(dataNode2).admin().cluster().prepareState().setLocal(true)
                .execute().actionGet().getState();
            assertFalse(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));
            assertTrue(state.metadata().persistentSettings().getAsBoolean(UnsafeBootstrapMasterCommand.UNSAFE_BOOTSTRAP.getKey(), false));
        });

        List<String> bootstrappedNodes = new ArrayList<>();
        bootstrappedNodes.add(dataNode2);
        bootstrappedNodes.add(masterNode2);
        bootstrappedNodes.forEach(node-> ensureReadOnlyBlock(true, node));

        logger.info("--> ensure index test is green");
        ensureGreen("test");
        IndexMetadata indexMetadata = clusterService().state().metadata().index("test");
        assertThat(indexMetadata.getSettings().get(IndexMetadata.SETTING_HISTORY_UUID), notNullValue());

        logger.info("--> detach-cluster on 2nd and 3rd master-eligible nodes");
        Environment environmentMaster2 = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(master2DataPathSettings).build());
        detachCluster(environmentMaster2, false);
        Environment environmentMaster3 = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(master3DataPathSettings).build());
        detachCluster(environmentMaster3, false);

        logger.info("--> start 2nd and 3rd master-eligible nodes and ensure 4 nodes stable cluster");
        bootstrappedNodes.add(internalCluster().startMasterOnlyNode(master2DataPathSettings));
        bootstrappedNodes.add(internalCluster().startMasterOnlyNode(master3DataPathSettings));
        ensureStableCluster(4);
        bootstrappedNodes.forEach(node-> ensureReadOnlyBlock(true, node));
        removeBlock();
    }

    public void testAllMasterEligibleNodesFailedDanglingIndexImport() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);

        Settings settings = Settings.builder()
            .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true)
            .build();

        logger.info("--> start mixed data and master-eligible node and bootstrap cluster");
        String masterNode = internalCluster().startNode(settings); // node ordinal 0

        logger.info("--> start data-only node and ensure 2 nodes stable cluster");
        String dataNode = internalCluster().startDataOnlyNode(settings); // node ordinal 1
        ensureStableCluster(2);

        logger.info("--> index 1 doc and ensure index is green");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        ensureGreen("test");
        assertBusy(() -> internalCluster().getInstances(IndicesService.class).forEach(
            indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten())));

        logger.info("--> verify 1 doc in the index");
        assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 1L);
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));

        logger.info("--> stop data-only node and detach it from the old cluster");
        Settings dataNodeDataPathSettings = Settings.builder()
            .put(internalCluster().dataPathSettings(dataNode), true)
            .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true)
            .build();
        assertBusy(() -> internalCluster().getInstance(GatewayMetaState.class, dataNode).allPendingAsyncStatesWritten());
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataNode));
        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder()
                .put(internalCluster().getDefaultSettings())
                .put(dataNodeDataPathSettings)
                .put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true)
                .build());
        detachCluster(environment, false);

        logger.info("--> stop master-eligible node, clear its data and start it again - new cluster should form");
        internalCluster().restartNode(masterNode, new InternalTestCluster.RestartCallback(){
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });

        logger.info("--> start data-only only node and ensure 2 nodes stable cluster");
        internalCluster().startDataOnlyNode(dataNodeDataPathSettings);
        ensureStableCluster(2);

        logger.info("--> verify that the dangling index exists and has green status");
        assertBusy(() -> {
            assertThat(client().admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(true));
        });
        ensureGreen("test");

        logger.info("--> verify the doc is there");
        assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(true));
    }

    public void testNoInitialBootstrapAfterDetach() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String masterNode = internalCluster().startMasterOnlyNode();
        Settings masterNodeDataPathSettings = internalCluster().dataPathSettings(masterNode);
        internalCluster().stopCurrentMasterNode();

        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(masterNodeDataPathSettings).build());
        detachCluster(environment);

        String node = internalCluster().startMasterOnlyNode(Settings.builder()
            // give the cluster 2 seconds to elect the master (it should not)
            .put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "2s")
            .put(masterNodeDataPathSettings)
            .build());

        ClusterState state = internalCluster().client().admin().cluster().prepareState().setLocal(true)
            .execute().actionGet().getState();
        assertTrue(state.blocks().hasGlobalBlockWithId(NoMasterBlockService.NO_MASTER_BLOCK_ID));

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node));
    }

    public void testCanRunUnsafeBootstrapAfterErroneousDetachWithoutLoosingMetadata() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String masterNode = internalCluster().startMasterOnlyNode();
        Settings masterNodeDataPathSettings = internalCluster().dataPathSettings(masterNode);
        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest().persistentSettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "1234kb"));
        internalCluster().client().admin().cluster().updateSettings(req).get();

        ClusterState state = internalCluster().client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()),
            equalTo("1234kb"));

        ensureReadOnlyBlock(false, masterNode);

        internalCluster().stopCurrentMasterNode();

        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(masterNodeDataPathSettings).build());
        detachCluster(environment);
        unsafeBootstrap(environment); //read-only block will remain same as one before bootstrap, in this case it is false

        String masterNode2 = internalCluster().startMasterOnlyNode(masterNodeDataPathSettings);
        ensureGreen();
        ensureReadOnlyBlock(false, masterNode2);

        state = internalCluster().client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metadata().settings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()),
            equalTo("1234kb"));
    }

    public void testUnsafeBootstrapWithApplyClusterReadOnlyBlockAsFalse() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        String masterNode = internalCluster().startMasterOnlyNode();
        Settings masterNodeDataPathSettings = internalCluster().dataPathSettings(masterNode);
        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest().persistentSettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "1234kb"));
        internalCluster().client().admin().cluster().updateSettings(req).get();

        ClusterState state = internalCluster().client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metadata().persistentSettings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()),
            equalTo("1234kb"));

        ensureReadOnlyBlock(false, masterNode);

        internalCluster().stopCurrentMasterNode();

        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(masterNodeDataPathSettings).build());
        unsafeBootstrap(environment, false, false);

        String masterNode2 = internalCluster().startMasterOnlyNode(masterNodeDataPathSettings);
        ensureGreen();
        ensureReadOnlyBlock(false, masterNode2);

        state = internalCluster().client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metadata().settings().get(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()),
            equalTo("1234kb"));
    }
}
