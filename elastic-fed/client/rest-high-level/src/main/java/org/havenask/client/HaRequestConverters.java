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

package org.havenask.client;

import org.apache.http.client.methods.HttpGet;
import org.havenask.client.ha.SqlClientInfoRequest;
import org.havenask.client.ha.SqlRequest;

public class HaRequestConverters {
    public static Request sql(SqlRequest sqlRequest) {
        String endpoint = "/_havenask/sql";
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        request.addParameter("query", sqlRequest.getSql());
        request.addParameter("format", "full_json");
        if (sqlRequest.getTrace() != null) {
            request.addParameter("trace", sqlRequest.getTrace());
        }

        if (sqlRequest.getTimeout() != null) {
            request.addParameter("timeout", String.valueOf(sqlRequest.getTimeout()));
        }

        if (sqlRequest.getSearchInfo() != null) {
            request.addParameter("searchInfo", String.valueOf(sqlRequest.getSearchInfo()));
        }

        if (sqlRequest.getSqlPlan() != null) {
            request.addParameter("sqlPlan", String.valueOf(sqlRequest.getSqlPlan()));
        }

        if (sqlRequest.getForbitMergeSearchInfo() != null) {
            request.addParameter("forbitMergeSearchInfo", String.valueOf(sqlRequest.getForbitMergeSearchInfo()));
        }

        if (sqlRequest.getResultReadable() != null) {
            request.addParameter("resultReadable", String.valueOf(sqlRequest.getResultReadable()));
        }

        if (sqlRequest.getParallel() != null) {
            request.addParameter("parallel", String.valueOf(sqlRequest.getParallel()));
        }

        if (sqlRequest.getParallelTables() != null) {
            request.addParameter("parallelTables", sqlRequest.getParallelTables());
        }

        if (sqlRequest.getLackResultEnable() != null) {
            request.addParameter("lackResultEnable", String.valueOf(sqlRequest.getLackResultEnable()));
        }

        if (sqlRequest.getOptimizerDebug() != null) {
            request.addParameter("optimizerDebug", String.valueOf(sqlRequest.getOptimizerDebug()));
        }

        if (sqlRequest.getSortLimitTogether() != null) {
            request.addParameter("sortLimitTogether", String.valueOf(sqlRequest.getSortLimitTogether()));
        }

        if (sqlRequest.getForceLimit() != null) {
            request.addParameter("forceLimit", String.valueOf(sqlRequest.getForceLimit()));
        }

        if (sqlRequest.getJoinConditionCheck() != null) {
            request.addParameter("joinConditionCheck", String.valueOf(sqlRequest.getJoinConditionCheck()));
        }

        if (sqlRequest.getForceJoinHask() != null) {
            request.addParameter("forceJoinHask", String.valueOf(sqlRequest.getForceJoinHask()));
        }

        if (sqlRequest.getPlanLevel() != null) {
            request.addParameter("planLevel", String.valueOf(sqlRequest.getPlanLevel()));
        }

        if (sqlRequest.getCacheEnable() != null) {
            request.addParameter("cacheEnable", String.valueOf(sqlRequest.getCacheEnable()));
        }

        return request;
    }

    public static Request sqlClientInfo(SqlClientInfoRequest sqlClientInfoRequest) {
        String endpoint = "/_havenask/sql_info";
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        return request;
    }
}
