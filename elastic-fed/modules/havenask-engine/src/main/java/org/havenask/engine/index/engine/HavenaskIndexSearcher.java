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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.apache.lucene.search.similarities.Similarity;
import org.havenask.common.Strings;
import org.havenask.common.lucene.search.TopDocsAndMaxScore;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.rpc.QrsSqlRequest;
import org.havenask.engine.rpc.QrsSqlResponse;
import org.havenask.index.shard.ShardId;
import org.havenask.search.DefaultSearchContext;
import org.havenask.search.DocValueFormat;
import org.havenask.search.internal.ContextIndexSearcher;
import org.havenask.search.query.QuerySearchResult;

import static org.havenask.engine.search.rest.RestHavenaskSqlAction.SQL_DATABASE;

public class HavenaskIndexSearcher extends ContextIndexSearcher {
    private final QrsClient qrsHttpClient;
    private final ShardId shardId;
    private final DefaultSearchContext searchContext;

    public HavenaskIndexSearcher(
        QrsClient qrsHttpClient,
        ShardId shardId,
        DefaultSearchContext searchContext,
        IndexReader reader,
        Similarity similarity,
        QueryCache queryCache,
        QueryCachingPolicy queryCachingPolicy,
        boolean wrapWithExitableDirectoryReader
    ) throws IOException {
        super(reader, similarity, queryCache, queryCachingPolicy, wrapWithExitableDirectoryReader);
        this.qrsHttpClient = qrsHttpClient;
        this.shardId = shardId;
        this.searchContext = searchContext;
    }

    @Override
    public void search(Query query, Collector collector) throws IOException {
        String sql = QueryTransformer.toSql(shardId.getIndexName(), query);
        String kvpair = "format:full_json;timeout:10000;databaseName:" + SQL_DATABASE;
        QrsSqlRequest request = new QrsSqlRequest(sql, kvpair);
        QrsSqlResponse response = qrsHttpClient.executeSql(request);
        if (false == Strings.isNullOrEmpty(response.getResult())) {
            buildQuerySearchResult(searchContext.queryResult(), response.getResult());
        }
        searchContext.skipQueryCollectors(true);
    }

    public static void buildQuerySearchResult(QuerySearchResult querySearchResult, String sqlResponse) {
        TopDocs topDocs = new TopDocs(new TotalHits(0, Relation.GREATER_THAN_OR_EQUAL_TO), new ScoreDoc[0]);
        TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(topDocs, 0);
        querySearchResult.topDocs(topDocsAndMaxScore, new DocValueFormat[] { DocValueFormat.RAW });
    }
}