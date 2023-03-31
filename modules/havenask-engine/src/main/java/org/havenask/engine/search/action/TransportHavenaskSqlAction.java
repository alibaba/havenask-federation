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

package org.havenask.engine.search.action;

import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.indices.IndicesService;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool.Names;
import org.havenask.transport.TransportService;

public class TransportHavenaskSqlAction extends HandledTransportAction<HavenaskSqlRequest, HavenaskSqlResponse> {

    @Inject
    public TransportHavenaskSqlAction(
        ClusterService clusterService, TransportService transportService, IndicesService indicesService,
        ActionFilters actionFilters) {
        super(HavenaskSqlAction.NAME, transportService, actionFilters, HavenaskSqlRequest::new, Names.SEARCH);
    }

    @Override
    protected void doExecute(Task task, HavenaskSqlRequest request, ActionListener<HavenaskSqlResponse> listener) {
        // TODO Auto-generated method stub
    }
}
