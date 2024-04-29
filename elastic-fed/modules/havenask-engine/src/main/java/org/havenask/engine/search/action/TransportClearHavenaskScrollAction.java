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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.action.ActionListener;
import org.havenask.action.search.ClearScrollRequest;
import org.havenask.action.search.ClearScrollResponse;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.engine.HavenaskScrollService;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

public class TransportClearHavenaskScrollAction extends HandledTransportAction<ClearScrollRequest, ClearScrollResponse> {
    private static final Logger logger = LogManager.getLogger(TransportClearHavenaskScrollAction.class);
    private ClusterService clusterService;
    private HavenaskSearchTransportService havenaskSearchTransportService;
    private HavenaskScrollService havenaskScrollService;

    @Inject
    public TransportClearHavenaskScrollAction(
        ClusterService clusterService,
        TransportService transportService,
        HavenaskSearchTransportService havenaskSearchTransportService,
        HavenaskScrollService havenaskScrollService,
        ActionFilters actionFilters
    ) {
        super(ClearHavenaskScrollAction.NAME, transportService, actionFilters, ClearScrollRequest::new, ThreadPool.Names.SEARCH);
        this.clusterService = clusterService;
        this.havenaskSearchTransportService = havenaskSearchTransportService;
        HavenaskSearchTransportService.registerRequestHandler(transportService, havenaskScrollService);
        this.havenaskScrollService = havenaskScrollService;
    }

    @Override
    protected void doExecute(Task task, ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {
        try {
            Runnable runnable = new ClearHavenaskScrollController(
                request,
                listener,
                clusterService.state().nodes(),
                logger,
                havenaskSearchTransportService
            );
            runnable.run();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
