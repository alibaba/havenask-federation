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

import com.alibaba.fastjson.JSONObject;

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
import org.havenask.engine.rpc.SqlClientInfoResponse;
import org.havenask.engine.rpc.http.QrsHttpClient;
import org.havenask.engine.search.action.HavenaskSqlClientInfoAction.Request;
import org.havenask.engine.search.action.HavenaskSqlClientInfoAction.Response;
import org.havenask.rest.RestStatus;
import org.havenask.tasks.Task;
import org.havenask.threadpool.ThreadPool.Names;
import org.havenask.transport.TransportService;

public class TransportHavenaskSqlClientInfoAction extends HandledTransportAction<Request, Response> {
    private static final Logger logger = LogManager.getLogger(TransportHavenaskSqlClientInfoAction.class);

    private ClusterService clusterService;
    private final IngestActionForwarder ingestForwarder;
    private QrsClient qrsClient;

    @Inject
    public TransportHavenaskSqlClientInfoAction(
        ClusterService clusterService,
        TransportService transportService,
        NativeProcessControlService nativeProcessControlService,
        ActionFilters actionFilters
    ) {
        super(HavenaskSqlClientInfoAction.NAME, transportService, actionFilters, HavenaskSqlClientInfoAction.Request::new, Names.SEARCH);
        this.clusterService = clusterService;
        this.ingestForwarder = new IngestActionForwarder(transportService);
        this.qrsClient = new QrsHttpClient(nativeProcessControlService.getQrsHttpPort());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doExecute(Task task, HavenaskSqlClientInfoAction.Request request, ActionListener<Response> listener) {
        if (false == clusterService.localNode().isIngestNode()) {
            ingestForwarder.forwardIngestRequest(HavenaskSqlClientInfoAction.INSTANCE, request, listener);
            return;
        }

        try {
            SqlClientInfoResponse response = qrsClient.executeSqlClientInfo();
            if (response.getErrorCode() != 0) {
                listener.onResponse(new HavenaskSqlClientInfoAction.Response(response.getErrorMessage(), response.getErrorCode()));
                return;
            }

            JSONObject result = response.getResult();
            if (result.containsKey("default")) {
                JSONObject defaultJson = result.getJSONObject("default");
                if (defaultJson.containsKey("general")) {
                    JSONObject generalJson = defaultJson.getJSONObject("general");
                    if (generalJson.containsKey("tables")) {
                        JSONObject tablesJson = generalJson.getJSONObject("tables");
                        tablesJson.remove("in0");
                        tablesJson.remove("in0_summary_");
                    }
                }
            }
            listener.onResponse(new HavenaskSqlClientInfoAction.Response(result));
        } catch (IOException e) {
            logger.warn("execute sql client info api failed", e);
            listener.onResponse(
                new HavenaskSqlClientInfoAction.Response(
                    "execute sql client info api failed:" + e.getMessage(),
                    RestStatus.INTERNAL_SERVER_ERROR.getStatus()
                )
            );
        }
    }
}
