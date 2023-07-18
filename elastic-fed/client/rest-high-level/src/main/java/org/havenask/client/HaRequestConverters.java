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

        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.putParam("sql", sqlRequest.getSql());
        if (sqlRequest.getTrace() != null) {
            parameters.putParam("trace", sqlRequest.getTrace());
        }

        if (sqlRequest.getTimeout() != null) {
            parameters.putParam("timeout", String.valueOf(sqlRequest.getTimeout()));
        }

        if (sqlRequest.getSearchInfo() != null) {
            parameters.putParam("searchInfo", String.valueOf(sqlRequest.getSearchInfo()));
        }

        if (sqlRequest.getSqlPlan() != null) {
            parameters.putParam("sqlPlan", String.valueOf(sqlRequest.getSqlPlan()));
        }

        if (sqlRequest.getForbitMergeSearchInfo() != null) {
            parameters.putParam("forbitMergeSearchInfo", String.valueOf(sqlRequest.getForbitMergeSearchInfo()));
        }

        if (sqlRequest.getResultReadable() != null) {
            parameters.putParam("resultReadable", String.valueOf(sqlRequest.getResultReadable()));
        }

        if (sqlRequest.getParallel() != null) {
            parameters.putParam("parallel", String.valueOf(sqlRequest.getParallel()));
        }

        if (sqlRequest.getParallelTables() != null) {
            parameters.putParam("parallelTables", sqlRequest.getParallelTables());
        }

        if (sqlRequest.getLackResultEnable() != null) {
            parameters.putParam("lackResultEnable", String.valueOf(sqlRequest.getLackResultEnable()));
        }

        if (sqlRequest.getOptimizerDebug() != null) {
            parameters.putParam("optimizerDebug", String.valueOf(sqlRequest.getOptimizerDebug()));
        }

        if (sqlRequest.getSortLimitTogether() != null) {
            parameters.putParam("sortLimitTogether", String.valueOf(sqlRequest.getSortLimitTogether()));
        }

        if (sqlRequest.getForceLimit() != null) {
            parameters.putParam("forceLimit", String.valueOf(sqlRequest.getForceLimit()));
        }

        if (sqlRequest.getJoinConditionCheck() != null) {
            parameters.putParam("joinConditionCheck", String.valueOf(sqlRequest.getJoinConditionCheck()));
        }

        if (sqlRequest.getForceJoinHask() != null) {
            parameters.putParam("forceJoinHask", String.valueOf(sqlRequest.getForceJoinHask()));
        }

        if (sqlRequest.getPlanLevel() != null) {
            parameters.putParam("planLevel", String.valueOf(sqlRequest.getPlanLevel()));
        }

        if (sqlRequest.getCacheEnable() != null) {
            parameters.putParam("cacheEnable", String.valueOf(sqlRequest.getCacheEnable()));
        }

        request.addParameters(parameters.asMap());
        return request;
    }

    public static Request sqlClientInfo(SqlClientInfoRequest sqlClientInfoRequest) {
        String endpoint = "/_havenask/sqlClientInfo";
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        return request;
    }
}
