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

package org.havenask.action.admin;

import org.havenask.action.admin.cluster.node.info.NodesInfoResponse;
import org.havenask.action.admin.cluster.node.info.NodesInfoAction;
import org.havenask.action.admin.cluster.node.info.NodeInfo;
import org.havenask.action.admin.cluster.node.stats.NodeStats;
import org.havenask.action.admin.cluster.node.stats.NodesStatsAction;
import org.havenask.action.admin.cluster.node.stats.NodesStatsResponse;
import org.havenask.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.havenask.action.admin.indices.recovery.RecoveryAction;
import org.havenask.action.admin.indices.recovery.RecoveryResponse;
import org.havenask.action.admin.indices.stats.IndicesStatsAction;
import org.havenask.action.admin.indices.stats.IndicesStatsResponse;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.TimeValue;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.transport.MockTransportService;
import org.havenask.test.transport.StubbableTransport;
import org.havenask.transport.ReceiveTimeoutTransportException;
import org.havenask.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.containsString;

@HavenaskIntegTestCase.ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
public class ClientTimeoutIT extends HavenaskIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testNodesInfoTimeout(){
        String masterNode = internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        String anotherDataNode = internalCluster().startDataOnlyNode();

        // Happy case
        NodesInfoResponse response = dataNodeClient().admin().cluster().prepareNodesInfo().get();
        assertThat(response.getNodes().size(), equalTo(3));

        //simulate timeout on bad node.
        simulateTimeoutAtTransport(dataNode, anotherDataNode, NodesInfoAction.NAME);

        // One bad data node
        response = dataNodeClient().admin().cluster().prepareNodesInfo().get();
        ArrayList<String> nodes = new ArrayList<String>();
        for(NodeInfo node : response.getNodes()) {
            nodes.add(node.getNode().getName());
        }
        assertThat(response.getNodes().size(), equalTo(2));
        assertThat(nodes.contains(masterNode), is(true));
    }

    public void testNodesStatsTimeout(){
        String masterNode = internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        String anotherDataNode = internalCluster().startDataOnlyNode();
        TimeValue timeout = TimeValue.timeValueMillis(1000);

        // Happy case
        NodesStatsResponse response1 = dataNodeClient().admin().cluster().prepareNodesStats().get();
        assertThat(response1.getNodes().size(), equalTo(3));

        // One bad data node
        simulateTimeoutAtTransport(dataNode, anotherDataNode, NodesStatsAction.NAME);

        NodesStatsResponse response = dataNodeClient().admin().cluster().prepareNodesStats().get();
        ArrayList<String> nodes = new ArrayList<String>();
        for(NodeStats node : response.getNodes()) {
            nodes.add(node.getNode().getName());
        }
        assertThat(response.getNodes().size(), equalTo(2));
        assertThat(nodes.contains(masterNode), is(true));
    }

    public void testListTasksTimeout(){
        String masterNode = internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        String anotherDataNode = internalCluster().startDataOnlyNode();
        TimeValue timeout = TimeValue.timeValueMillis(1000);

        // Happy case
        ListTasksResponse response1 = dataNodeClient().admin().cluster().prepareListTasks().get();
        assertThat(response1.getPerNodeTasks().keySet().size(), equalTo(3));

        // One bad data node
        simulateTimeoutAtTransport(dataNode, anotherDataNode, NodesStatsAction.NAME);

        ListTasksResponse response = dataNodeClient().admin().cluster().prepareListTasks().get();
        assertNull(response.getPerNodeTasks().get(anotherDataNode));
    }

    public void testRecoveriesWithTimeout(){
        internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        String anotherDataNode = internalCluster().startDataOnlyNode();

        int numShards = 4;
        assertAcked(prepareCreate("test-index", 0, Settings.builder().
                put("number_of_shards", numShards).put("routing.allocation.total_shards_per_node", 2).
                put("number_of_replicas", 0)));
        ensureGreen();
        final long numDocs = scaledRandomIntBetween(50, 100);
        for (int i = 0; i < numDocs; i++) {
            index("test-index", "doc", Integer.toString(i));
        }
        refresh("test-index");
        ensureSearchable("test-index");

        // Happy case
        RecoveryResponse recoveryResponse = dataNodeClient().admin().indices().prepareRecoveries().get();
        assertThat(recoveryResponse.getTotalShards(), equalTo(numShards));
        assertThat(recoveryResponse.getSuccessfulShards(), equalTo(numShards));

        //simulate timeout on bad node.
        simulateTimeoutAtTransport(dataNode, anotherDataNode, RecoveryAction.NAME);

        //verify response with bad node.
        recoveryResponse = dataNodeClient().admin().indices().prepareRecoveries().get();
        assertThat(recoveryResponse.getTotalShards(), equalTo(numShards));
        assertThat(recoveryResponse.getSuccessfulShards(), equalTo(numShards/2));
        assertThat(recoveryResponse.getFailedShards(), equalTo(numShards/2));
        assertThat(recoveryResponse.getShardFailures()[0].reason(), containsString("ReceiveTimeoutTransportException"));
    }

    public void testStatsWithTimeout(){
        internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        String anotherDataNode = internalCluster().startDataOnlyNode();

        int numShards = 4;
        logger.info("-->  creating index");
        assertAcked(prepareCreate("test-index", 0, Settings.builder().
                put("number_of_shards", numShards).put("routing.allocation.total_shards_per_node", 2).
                put("number_of_replicas", 0)));
        ensureGreen();
        final long numDocs = scaledRandomIntBetween(50, 100);
        for (int i = 0; i < numDocs; i++) {
            index("test-index", "doc", Integer.toString(i));
        }
        refresh("test-index");
        ensureSearchable("test-index");

        //happy case
        IndicesStatsResponse indicesStats = dataNodeClient().admin().indices().prepareStats().setDocs(true).get();
        assertThat(indicesStats.getTotalShards(), equalTo(numShards));
        assertThat(indicesStats.getSuccessfulShards(), equalTo(numShards));

        // simulate timeout on bad node.
        simulateTimeoutAtTransport(dataNode, anotherDataNode, IndicesStatsAction.NAME);

        // verify indices state response with bad node.
        indicesStats = dataNodeClient().admin().indices().prepareStats().setDocs(true).get();
        assertThat(indicesStats.getTotalShards(), equalTo(numShards));
        assertThat(indicesStats.getFailedShards(), equalTo(numShards/2));
        assertThat(indicesStats.getSuccessfulShards(), equalTo(numShards/2));
        assertThat(indicesStats.getTotal().getDocs().getCount(), lessThan(numDocs));
        assertThat(indicesStats.getShardFailures()[0].reason(), containsString("ReceiveTimeoutTransportException"));
    }

    private void simulateTimeoutAtTransport(String dataNode, String anotherDataNode, String transportActionName) {
        MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(TransportService.class,
                dataNode));
        StubbableTransport.SendRequestBehavior sendBehaviour = (connection, requestId, action, request, options) -> {
            if (action.startsWith(transportActionName)) {
                throw new ReceiveTimeoutTransportException(connection.getNode(), action, "simulate timeout");
            }
            connection.sendRequest(requestId, action, request, options);
        };
        mockTransportService.addSendBehavior(internalCluster().getInstance(TransportService.class, anotherDataNode), sendBehaviour);
        MockTransportService mockTransportServiceAnotherNode = ((MockTransportService) internalCluster().getInstance(TransportService.class,
                anotherDataNode));
        mockTransportServiceAnotherNode.addSendBehavior(internalCluster().getInstance(TransportService.class, dataNode), sendBehaviour);

    }
}
