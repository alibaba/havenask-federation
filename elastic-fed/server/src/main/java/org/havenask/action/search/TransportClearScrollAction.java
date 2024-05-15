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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.action.search;

import org.havenask.action.ActionListener;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.tasks.Task;
import org.havenask.transport.TransportService;

import java.util.Objects;

public class TransportClearScrollAction extends HandledTransportAction<ClearScrollRequest, ClearScrollResponse> {

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final SearchTransportService searchTransportService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    public static TransportClearScrollExecutor transportClearScrollExecutor;
    public interface TransportClearScrollExecutor {
        void apply(Task task,
                   ClearScrollRequest request,
                   final ActionListener<ClearScrollResponse> listener,
                   SearchTransportService searchTransportService);
    }

    @Inject
    public TransportClearScrollAction(TransportService transportService, ClusterService clusterService, ActionFilters actionFilters,
                                      SearchTransportService searchTransportService, NamedWriteableRegistry namedWriteableRegistry) {
        super(ClearScrollAction.NAME, transportService, actionFilters, ClearScrollRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.searchTransportService = searchTransportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    protected void doExecute(Task task, ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {
        if (Objects.nonNull(transportClearScrollExecutor)) {
            transportClearScrollExecutor.apply(task, request, listener, searchTransportService);
        } else {
            Runnable runnable = new ClearScrollController(
                    request, listener, clusterService.state().nodes(), logger, searchTransportService);
            runnable.run();
        }
    }
}
