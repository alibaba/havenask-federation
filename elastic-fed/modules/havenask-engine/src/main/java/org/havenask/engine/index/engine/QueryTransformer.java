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

package org.havenask.engine.index.engine;

import java.io.IOException;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.havenask.engine.index.query.ProximaQuery;

public class QueryTransformer {

    public static String toSql(String table, Query query) throws IOException {
        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("select _id from " + table);
        if (query instanceof ProximaQuery) {
            ProximaQuery proximaQuery = (ProximaQuery) query;
            sqlQuery.append(" where MATCHINDEX('" + proximaQuery.getField() + "', '");
            for (int i = 0; i < proximaQuery.getQueryVector().length; i++) {
                sqlQuery.append(proximaQuery.getQueryVector()[i]);
                if (i < proximaQuery.getQueryVector().length - 1) {
                    sqlQuery.append(",");
                }
            }
            sqlQuery.append("&n=" + proximaQuery.getTopN() + "')");
        } else if (query instanceof MatchAllDocsQuery) {
            // do nothing
        } else {
            // TODO reject unsupported DSL
            throw new IOException("unsupported DSL query:" + query);
        }

        return sqlQuery.toString();
    }
}
