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

package org.havenask.plugin.noop;

import org.havenask.plugin.noop.action.bulk.NoopBulkAction;
import org.havenask.plugin.noop.action.bulk.RestNoopBulkAction;
import org.havenask.plugin.noop.action.bulk.TransportNoopBulkAction;
import org.havenask.action.ActionRequest;
import org.havenask.action.ActionResponse;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.node.DiscoveryNodes;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.IndexScopedSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.settings.SettingsFilter;
import org.havenask.plugin.noop.action.search.NoopSearchAction;
import org.havenask.plugin.noop.action.search.RestNoopSearchAction;
import org.havenask.plugin.noop.action.search.TransportNoopSearchAction;
import org.havenask.plugins.ActionPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.rest.RestController;
import org.havenask.rest.RestHandler;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class NoopPlugin extends Plugin implements ActionPlugin {
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(NoopBulkAction.INSTANCE, TransportNoopBulkAction.class),
            new ActionHandler<>(NoopSearchAction.INSTANCE, TransportNoopSearchAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestNoopBulkAction(),
                new RestNoopSearchAction());
    }
}
