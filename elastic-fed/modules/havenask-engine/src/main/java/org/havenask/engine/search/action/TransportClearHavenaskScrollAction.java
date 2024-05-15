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
import org.apache.lucene.store.ByteArrayDataInput;
import org.havenask.action.ActionListener;
import org.havenask.action.search.ClearScrollController;
import org.havenask.action.search.ClearScrollRequest;
import org.havenask.action.search.ClearScrollResponse;
import org.havenask.action.search.SearchTransportService;
import org.havenask.action.search.TransportClearScrollAction;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.engine.HavenaskScrollService;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class TransportClearHavenaskScrollAction extends HandledTransportAction<ClearScrollRequest, ClearScrollResponse> {
    private static final Logger logger = LogManager.getLogger(TransportClearHavenaskScrollAction.class);
    private final ClusterService clusterService;
    private final HavenaskSearchTransportService havenaskSearchTransportService;

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
        TransportClearScrollAction.transportClearScrollExecutor = (
            task,
            request,
            listener,
            searchTransportService) -> TransportClearHavenaskScrollAction.executeHavenaskClearScroll(
                task,
                request,
                listener,
                searchTransportService,
                clusterService,
                transportService,
                havenaskSearchTransportService
            );
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

    public static void executeHavenaskClearScroll(
        Task task,
        ClearScrollRequest request,
        final ActionListener<ClearScrollResponse> listener,
        SearchTransportService searchTransportService,
        ClusterService clusterService,
        TransportService transportService,
        HavenaskSearchTransportService havenaskSearchTransportService
    ) {
        try {
            List<String> elasticScrollIds = new ArrayList<>();
            List<String> havenaskScrollIds = new ArrayList<>();
            separateElasticScrollIdAndHavenaskScrollId(request.getScrollIds(), elasticScrollIds, havenaskScrollIds);
            if (!elasticScrollIds.isEmpty()) {
                ClearScrollRequest havenaskClearScrollRequest = new ClearScrollRequest();
                havenaskClearScrollRequest.setScrollIds(havenaskScrollIds);
                ClearScrollRequest esClearScrollRequest = new ClearScrollRequest();
                esClearScrollRequest.setScrollIds(elasticScrollIds);

                ActionListener<ClearScrollResponse> havenaskSearchListener = ActionListener.wrap(response -> {
                    Runnable esRunnable = new ClearScrollController(
                        request,
                        listener,
                        clusterService.state().nodes(),
                        logger,
                        searchTransportService
                    );
                    esRunnable.run();
                }, e -> { listener.onFailure(e); });
                Runnable runnable = new ClearHavenaskScrollController(
                    havenaskClearScrollRequest,
                    havenaskSearchListener,
                    clusterService.state().nodes(),
                    logger,
                    havenaskSearchTransportService
                );
                runnable.run();
            } else {
                Runnable runnable = new ClearHavenaskScrollController(
                    request,
                    listener,
                    clusterService.state().nodes(),
                    logger,
                    havenaskSearchTransportService
                );
                runnable.run();
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static void separateElasticScrollIdAndHavenaskScrollId(
        List<String> scrollIds,
        List<String> elasticScrollIds,
        List<String> havenaskScrollIds
    ) {
        try {
            if (scrollIds.size() == 1 && "_all".equals(scrollIds.get(0))) {
                elasticScrollIds.add(scrollIds.get(0));
                havenaskScrollIds.add(scrollIds.get(0));
                return;
            }

            for (String scrollId : scrollIds) {
                byte[] bytes = Base64.getUrlDecoder().decode(scrollId);
                ByteArrayDataInput in = new ByteArrayDataInput(bytes);
                final String firstChunk = in.readString();
                if ("havenask_scroll_id".equals(firstChunk)) {
                    havenaskScrollIds.add(scrollId);
                } else {
                    elasticScrollIds.add(scrollId);
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse scroll id", e);
        }
    }

}
