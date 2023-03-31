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

package org.havenask.engine.search.rest;

import java.io.IOException;
import java.util.List;

import org.havenask.client.node.NodeClient;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.engine.search.action.HavenaskSqlAction;
import org.havenask.engine.search.action.HavenaskSqlRequest;
import org.havenask.engine.search.action.HavenaskSqlResponse;
import org.havenask.rest.BaseRestHandler;
import org.havenask.rest.BytesRestResponse;
import org.havenask.rest.RestRequest;
import org.havenask.rest.RestRequest.Method;
import org.havenask.rest.RestResponse;
import org.havenask.rest.RestStatus;
import org.havenask.rest.action.RestBuilderListener;

public class RestHavenaskSqlAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "havenask_sql_action";
    }

    public List<Route> routes() {
        return List.of(
            new Route(Method.GET, "/_havenask/sql"),
            new Route(Method.POST, "/_havenask/sql")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String query = request.param("query");
        if (query == null) {
            throw new IllegalArgumentException("query is null");
        }

        String kvpair = request.param("kvpair");
        if (kvpair == null) {
            // 组装kvpair
            String trace = request.param("trace");
            String formatType = request.param("format");
            long timeout = request.paramAsLong("timeout", -1);
            boolean searchInfo = request.paramAsBoolean("searchInfo", false);
            boolean sqlPlan = request.paramAsBoolean("sqlPlan", false);
            boolean forbitMergeSearchInfo = request.paramAsBoolean("forbitMergeSearchInfo", false);
            boolean resultReadable = request.paramAsBoolean("resultReadable", false);
            int parallel = request.paramAsInt("parallel", 1);
            String parallelTables = request.param("parallelTables");
            String databaseName = request.param("databaseName");
            boolean lackResultEnable = request.paramAsBoolean("lackResultEnable", false);
            boolean optimizerDebug = request.paramAsBoolean("optimizerDebug", false);
            boolean sortLimitTogether = request.paramAsBoolean("sortLimitTogether", true);
            boolean forceLimit = request.paramAsBoolean("forceLimit", false);
            boolean joinConditionCheck = request.paramAsBoolean("joinConditionCheck", true);
            boolean forceJoinHask = request.paramAsBoolean("forceJoinHask", false);
            boolean planLevel = request.paramAsBoolean("planLevel", false);
            boolean cacheEnable = request.paramAsBoolean("cacheEnable", false);

            StringBuffer kvBuffer = new StringBuffer();
            if (trace != null) {
                kvBuffer.append("trace:").append(trace).append(";");
            }
            if (formatType != null) {
                kvBuffer.append("format:").append(formatType).append(";");
            }
            if (timeout > 0) {
                kvBuffer.append("timeout:").append(timeout).append(";");
            }
            if (searchInfo) {
                kvBuffer.append("searchInfo:").append(searchInfo).append(";");
            }
            if (sqlPlan) {
                kvBuffer.append("sqlPlan:").append(sqlPlan).append(";");
            }
            if (forbitMergeSearchInfo) {
                kvBuffer.append("forbitMergeSearchInfo:").append(forbitMergeSearchInfo).append(";");
            }
            if (resultReadable) {
                kvBuffer.append("resultReadable:").append(resultReadable).append(";");
            }
            if (parallel > 1) {
                kvBuffer.append("parallel:").append(parallel).append(";");
            }
            if (parallelTables != null) {
                kvBuffer.append("parallelTables:").append(parallelTables).append(";");
            }
            if (databaseName != null) {
                kvBuffer.append("databaseName:").append(databaseName).append(";");
            }
            if (lackResultEnable) {
                kvBuffer.append("lackResultEnable:").append(lackResultEnable).append(";");
            }
            if (optimizerDebug) {
                kvBuffer.append("optimizerDebug:").append(optimizerDebug).append(";");
            }
            if (false == sortLimitTogether) {
                kvBuffer.append("sortLimitTogether:").append(sortLimitTogether).append(";");
            }
            if (forceLimit) {
                kvBuffer.append("forceLimit:").append(forceLimit).append(";");
            }
            if (false == joinConditionCheck) {
                kvBuffer.append("joinConditionCheck:").append(joinConditionCheck).append(";");
            }
            if (forceJoinHask) {
                kvBuffer.append("forceJoinHask:").append(forceJoinHask).append(";");
            }
            if (planLevel) {
                kvBuffer.append("planLevel:").append(planLevel).append(";");
            }
            if (cacheEnable) {
                kvBuffer.append("cacheEnable:").append(cacheEnable).append(";");
            }

            kvpair = kvBuffer.toString();
            if (kvpair.length() > 0) {
                kvpair = kvpair.substring(0, kvpair.length() - 1);
            }
        }

        HavenaskSqlRequest havenaskSqlRequest = new HavenaskSqlRequest(query, kvpair);
        return channel -> client.execute(HavenaskSqlAction.INSTANCE, havenaskSqlRequest,
            new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(HavenaskSqlResponse response, XContentBuilder builder) {
                    return new BytesRestResponse(RestStatus.OK, response.getResult());
                }
            });
    }
}
