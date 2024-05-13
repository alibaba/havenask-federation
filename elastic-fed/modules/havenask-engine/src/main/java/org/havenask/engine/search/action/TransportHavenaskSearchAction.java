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
import org.havenask.action.ingest.IngestActionForwarder;
import org.havenask.action.search.SearchRequest;
import org.havenask.action.search.SearchResponse;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.IndexAbstraction;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.engine.HavenaskScrollService;
import org.havenask.engine.NativeProcessControlService;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.http.QrsHttpClient;
import org.havenask.engine.search.dsl.DSLSession;
import org.havenask.engine.search.internal.HavenaskScrollContext;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.util.Objects;

public class TransportHavenaskSearchAction extends HandledTransportAction<SearchRequest, SearchResponse> {
    private static final Logger logger = LogManager.getLogger(TransportHavenaskSearchAction.class);
    private final ClusterService clusterService;
    private final IngestActionForwarder ingestForwarder;
    private final QrsClient qrsClient;
    private HavenaskScrollService havenaskScrollService;
    private final NamedXContentRegistry namedXContentRegistry;

    @Inject
    public TransportHavenaskSearchAction(
        ClusterService clusterService,
        TransportService transportService,
        NamedXContentRegistry namedXContentRegistry,
        NativeProcessControlService nativeProcessControlService,
        ActionFilters actionFilters,
        HavenaskScrollService havenaskScrollService
    ) {
        super(HavenaskSearchAction.NAME, transportService, actionFilters, SearchRequest::new, ThreadPool.Names.SEARCH);
        this.clusterService = clusterService;
        this.namedXContentRegistry = namedXContentRegistry;
        this.ingestForwarder = new IngestActionForwarder(transportService);
        clusterService.addStateApplier(this.ingestForwarder);
        this.qrsClient = new QrsHttpClient(nativeProcessControlService.getQrsHttpPort());
        this.havenaskScrollService = havenaskScrollService;
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        if (false == clusterService.localNode().isIngestNode()) {
            ingestForwarder.forwardIngestRequest(HavenaskSearchAction.INSTANCE, request, listener);
            return;
        }

        try {
            // TODO: 目前的逻辑只有单havenask索引的查询会走到这里，后续如果有多索引的查询，这里需要做相应的修改
            if (request.indices().length != 1) {
                throw new IllegalArgumentException("illegal index count! only support search single havenask index.");
            }
            String tableName = request.indices()[0];
            ClusterState clusterState = clusterService.state();
            IndexAbstraction indexAbstraction = clusterState.metadata().getIndicesLookup().get(tableName);
            if (indexAbstraction == null) {
                throw new IllegalArgumentException("illegal index name! index name not exist.");
            }

            IndexMetadata indexMetadata = indexAbstraction.getWriteIndex();
            DSLSession session = new DSLSession(
                qrsClient,
                indexMetadata,
                request,
                clusterService.localNode().getId(),
                namedXContentRegistry
            );
            SearchResponse searchResponse = session.execute();

            if (Objects.nonNull(request.scroll())) {
                HavenaskScrollContext havenaskScrollContext = new HavenaskScrollContext(
                    havenaskScrollService.getThreadPool(),
                    session,
                    request.scroll().keepAlive().getMillis()
                );
                havenaskScrollService.putScrollContext(havenaskScrollContext);
            }

            listener.onResponse(searchResponse);
        } catch (Exception e) {
            logger.info("Failed to execute havenask search, ", e);
            listener.onFailure(e);
        }
    }
}
