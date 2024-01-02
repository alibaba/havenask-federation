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

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.action.ActionListener;
import org.havenask.action.ingest.IngestActionForwarder;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.HandledTransportAction;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.engine.NativeProcessControlService;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.rpc.QrsSqlResponse;
import org.havenask.engine.rpc.http.QrsHttpClient;
import org.havenask.rest.RestStatus;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool.Names;
import org.havenask.transport.TransportService;

public class TransportHavenaskSqlAction extends HandledTransportAction<HavenaskSqlRequest, HavenaskSqlResponse> {
    private static final Logger logger = LogManager.getLogger(TransportHavenaskSqlAction.class);

    private ClusterService clusterService;
    private final IngestActionForwarder ingestForwarder;
    private QrsClient qrsClient;

    @Inject
    public TransportHavenaskSqlAction(
        ClusterService clusterService,
        TransportService transportService,
        NativeProcessControlService nativeProcessControlService,
        ActionFilters actionFilters
    ) {
        super(HavenaskSqlAction.NAME, transportService, actionFilters, HavenaskSqlRequest::new, Names.SEARCH);
        this.clusterService = clusterService;
        this.ingestForwarder = new IngestActionForwarder(transportService);
        clusterService.addStateApplier(this.ingestForwarder);
        this.qrsClient = new QrsHttpClient(nativeProcessControlService.getQrsHttpPort());
    }

    @Override
    protected void doExecute(Task task, HavenaskSqlRequest request, ActionListener<HavenaskSqlResponse> listener) {
        if (false == clusterService.localNode().isIngestNode()) {
            ingestForwarder.forwardIngestRequest(HavenaskSqlAction.INSTANCE, request, listener);
            return;
        }

        QrsSqlRequest qrsSqlRequest = new QrsSqlRequest(request.getSql(), request.getKvpair());
        try {
            QrsSqlResponse result = qrsClient.executeSql(qrsSqlRequest);
            listener.onResponse(new HavenaskSqlResponse(result.getResult(), result.getResultCode()));
        } catch (IOException e) {
            logger.warn("execute sql failed", e);
            listener.onResponse(new HavenaskSqlResponse("execute sql failed:" + e, RestStatus.INTERNAL_SERVER_ERROR.getStatus()));
        }
    }
}
