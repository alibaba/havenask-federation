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

import org.havenask.engine.index.query.KnnSearchBuilder;
import org.havenask.engine.index.query.ProximaQueryBuilder;
import org.havenask.index.query.MatchAllQueryBuilder;
import org.havenask.index.query.MatchQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.TermQueryBuilder;
import org.havenask.search.SearchExtBuilder;
import org.havenask.search.builder.SearchSourceBuilder;

public class QueryTransformer {
    public static String toSql(String table, SearchSourceBuilder dsl) throws IOException {
        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("select _id from " + table);
        QueryBuilder queryBuilder = dsl.query();
        StringBuilder where = new StringBuilder();
        if (dsl.ext().size() > 0) {
            for (SearchExtBuilder ext : dsl.ext()) {
                if (ext instanceof KnnSearchBuilder) {
                    KnnSearchBuilder knnSearchBuilder = (KnnSearchBuilder) ext;
                    if (knnSearchBuilder.getFilterQueries().size() > 0 || knnSearchBuilder.getSimilarity() != null) {
                        throw new IOException("unsupported knn parameter: " + dsl);
                    }

                    where.append(" where MATCHINDEX('" + knnSearchBuilder.getField() + "', '");
                    for (int i = 0; i < knnSearchBuilder.getQueryVector().length; i++) {
                        where.append(knnSearchBuilder.getQueryVector()[i]);
                        if (i < knnSearchBuilder.getQueryVector().length - 1) {
                            where.append(",");
                        }
                    }
                    where.append("&n=" + knnSearchBuilder.k() + "')");
                    break;
                }
            }
        } else if (queryBuilder != null) {
            if (queryBuilder instanceof MatchAllQueryBuilder) {

            } else if (queryBuilder instanceof ProximaQueryBuilder) {
                ProximaQueryBuilder<?> proximaQueryBuilder = (ProximaQueryBuilder<?>) queryBuilder;
                where.append(" where MATCHINDEX('" + proximaQueryBuilder.getFieldName() + "', '");
                for (int i = 0; i < proximaQueryBuilder.getVector().length; i++) {
                    where.append(proximaQueryBuilder.getVector()[i]);
                    if (i < proximaQueryBuilder.getVector().length - 1) {
                        where.append(",");
                    }
                }
                where.append("&n=" + proximaQueryBuilder.getSize() + "')");
            } else if (queryBuilder instanceof TermQueryBuilder) {
                TermQueryBuilder termQueryBuilder = (TermQueryBuilder) queryBuilder;
                where.append(" where " + termQueryBuilder.fieldName() + "='" + termQueryBuilder.value() + "'");
            } else if (queryBuilder instanceof MatchQueryBuilder) {
                MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
                where.append(" where MATCHINDEX('" + matchQueryBuilder.fieldName() + "', '" + matchQueryBuilder.value() + "')");
            } else {
                // TODO reject unsupported DSL
                throw new IOException("unsupported DSL: " + dsl);
            }
        }
        sqlQuery.append(where);
        int size = 0;
        if (dsl.size() >= 0) {
            size += dsl.size();
            if (dsl.from() >= 0) {
                size += dsl.from();
            }
        }

        if (size > 0) {
            sqlQuery.append(" limit " + size);
        }
        return sqlQuery.toString();
    }
}
