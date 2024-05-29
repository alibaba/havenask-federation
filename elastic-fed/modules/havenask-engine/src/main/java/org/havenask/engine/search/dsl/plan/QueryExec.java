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

package org.havenask.engine.search.dsl.plan;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.client.ha.SqlResponse;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.rpc.QrsSqlResponse;
import org.havenask.engine.search.HavenaskSearchFetchProcessor;
import org.havenask.engine.search.dsl.DSLSession;
import org.havenask.engine.search.dsl.expression.QuerySQLExpression;
import org.havenask.search.SearchHits;

import java.sql.SQLException;
import java.util.Locale;

import static org.havenask.engine.search.rest.RestHavenaskSqlAction.SQL_DATABASE;

public class QueryExec implements Executable<SearchHits> {
    protected Logger logger = LogManager.getLogger(QueryExec.class);

    private final QuerySQLExpression querySQLExpression;

    public QueryExec(QuerySQLExpression querySQLExpression) {
        this.querySQLExpression = querySQLExpression;
    }

    @Override
    public SearchHits execute(DSLSession session) throws Exception {
        // exec query
        String sql = querySQLExpression.translate();
        logger.debug("query exec, session: {}, exec sql: {}", session.getSessionId(), sql);
        String kvpair = "format:full_json;timeout:10000;databaseName:" + SQL_DATABASE;

        QrsSqlResponse qrsSqlResponse = session.getClient().executeSql(new QrsSqlRequest(sql, kvpair));
        SqlResponse queryPhaseSqlResponse = SqlResponse.parse(qrsSqlResponse.getResult());
        if (queryPhaseSqlResponse.getErrorInfo().getErrorCode() != 0) {
            throw new SQLException(
                String.format(
                    Locale.ROOT,
                    "execute query phase sql failed after transfer dsl to sql. "
                        + "query phase sql: '%s', "
                        + "errorCode: %s, error: %s, message: %s ",
                    sql,
                    queryPhaseSqlResponse.getErrorInfo().getErrorCode(),
                    queryPhaseSqlResponse.getErrorInfo().getError(),
                    queryPhaseSqlResponse.getErrorInfo().getMessage()
                )
            );
        }

        HavenaskSearchFetchProcessor fetchProcessor = new HavenaskSearchFetchProcessor(session.getClient());
        return fetchProcessor.executeFetchHits(queryPhaseSqlResponse, session.getIndex(), session.getQuery(), session.isSourceEnabled());
    }
}
