/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine;

import java.io.IOException;

import org.havenask.cluster.ClusterChangedEvent;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateApplier;
import org.havenask.cluster.node.DiscoveryNode;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.component.AbstractLifecycleComponent;
import org.havenask.common.settings.Settings;

public class SearcherIndicesService extends AbstractLifecycleComponent implements ClusterStateApplier {

    private final ClusterService clusterService;
    private final Settings settings;

    public SearcherIndicesService(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.settings = clusterService.getSettings();
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (!lifecycle.started()) {
            return;
        }

        final ClusterState state = event.state();


    }

    @Override
    protected void doStart() {
        if (DiscoveryNode.isDataNode(settings)) {
            clusterService.addHighPriorityApplier(this);
        }
    }

    @Override
    protected void doStop() {
        if (DiscoveryNode.isDataNode(settings)) {
            clusterService.removeApplier(this);
        }
    }

    @Override
    protected void doClose() throws IOException {

    }
}
