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

import org.havenask.engine.index.query.ProximaQueryBuilder;
import org.havenask.index.query.MatchAllQueryBuilder;
import org.havenask.index.query.MatchQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.TermQueryBuilder;
import org.havenask.search.builder.KnnSearchBuilder;
import org.havenask.search.builder.SearchSourceBuilder;

public class QueryTransformer {
    public static String toSql(String table, SearchSourceBuilder dsl) throws IOException {
        StringBuilder sqlQuery = new StringBuilder();
        QueryBuilder queryBuilder = dsl.query();
        StringBuilder where = new StringBuilder();
        StringBuilder selectParams = new StringBuilder();
        StringBuilder orderBy = new StringBuilder();

        selectParams.append(" _id");

        if (dsl.knnSearch().size() > 0) {
            // TODO 目前不支持knnSearch同时查询多个向量,后续增加支持
            if (dsl.knnSearch().size() > 1) {
                throw new IOException("目前暂不支持同时查询多个向量field" + dsl);
            }
            where.append(" where ");
            boolean first = true;
            for (KnnSearchBuilder knnSearchBuilder : dsl.knnSearch()) {
                if (knnSearchBuilder.getFilterQueries().size() > 0 || knnSearchBuilder.getSimilarity() != null) {
                    throw new IOException("unsupported knn parameter: " + dsl);
                }

                if (false == first) {
                    where.append(" or ");
                }

                if (first) {
                    first = false;
                }
                // TODO 支持knnSearch同时查询多个向量后需要修改selectParams的写法
                selectParams.append(", vectorscore('").append(knnSearchBuilder.getField()).append("') as _score");

                where.append("MATCHINDEX('" + knnSearchBuilder.getField() + "', '");
                for (int i = 0; i < knnSearchBuilder.getQueryVector().length; i++) {
                    where.append(knnSearchBuilder.getQueryVector()[i]);
                    if (i < knnSearchBuilder.getQueryVector().length - 1) {
                        where.append(",");
                    }
                }
                where.append("&n=" + knnSearchBuilder.k() + "')");
            }
            orderBy.append(" order by _score desc");
        } else if (queryBuilder != null) {
            if (queryBuilder instanceof MatchAllQueryBuilder) {} else if (queryBuilder instanceof ProximaQueryBuilder) {
                ProximaQueryBuilder<?> proximaQueryBuilder = (ProximaQueryBuilder<?>) queryBuilder;
                selectParams.append(", vectorscore('").append(proximaQueryBuilder.getFieldName()).append("') as _score");
                where.append(" where MATCHINDEX('" + proximaQueryBuilder.getFieldName() + "', '");
                for (int i = 0; i < proximaQueryBuilder.getVector().length; i++) {
                    where.append(proximaQueryBuilder.getVector()[i]);
                    if (i < proximaQueryBuilder.getVector().length - 1) {
                        where.append(",");
                    }
                }
                where.append("&n=" + proximaQueryBuilder.getSize() + "')");
                orderBy.append(" order by _score desc");
            } else if (queryBuilder instanceof TermQueryBuilder) {
                TermQueryBuilder termQueryBuilder = (TermQueryBuilder) queryBuilder;
                where.append(" where " + termQueryBuilder.fieldName() + "='" + termQueryBuilder.value() + "'");
            } else if (queryBuilder instanceof MatchQueryBuilder) {
                MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) queryBuilder;
                where.append(" where MATCHINDEX('" + matchQueryBuilder.fieldName() + "', '" + matchQueryBuilder.value() + "')");
            } else {
                throw new IOException("unsupported DSL: " + dsl);
            }
        }
        sqlQuery.append("select").append(selectParams).append(" from ").append(table);
        sqlQuery.append(where).append(orderBy);
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
