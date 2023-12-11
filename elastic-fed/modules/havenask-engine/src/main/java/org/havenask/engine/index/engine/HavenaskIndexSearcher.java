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
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.havenask.client.Client;
import org.havenask.client.ha.SqlResponse;
import org.havenask.common.Strings;
import org.havenask.common.lucene.search.TopDocsAndMaxScore;
import org.havenask.engine.search.action.HavenaskSqlAction;
import org.havenask.engine.search.action.HavenaskSqlRequest;
import org.havenask.engine.search.action.HavenaskSqlResponse;
import org.havenask.engine.util.Utils;
import org.havenask.index.shard.ShardId;
import org.havenask.search.DefaultSearchContext;
import org.havenask.search.DocValueFormat;
import org.havenask.search.internal.ContextIndexSearcher;
import org.havenask.search.internal.ReaderContext;
import org.havenask.search.query.QuerySearchResult;

public class HavenaskIndexSearcher extends ContextIndexSearcher {
    private static final Logger logger = LogManager.getLogger(HavenaskIndexSearcher.class);
    public static final String IDS_CONTEXT = "havenask_ids";
    private static final int ID_POS = 0;
    private static final int SCORE_POS = 1;
    private final Client client;
    private final ShardId shardId;
    private final String tableName;
    private final DefaultSearchContext searchContext;

    public HavenaskIndexSearcher(
        Client client,
        ShardId shardId,
        DefaultSearchContext searchContext,
        IndexReader reader,
        Similarity similarity,
        QueryCache queryCache,
        QueryCachingPolicy queryCachingPolicy,
        boolean wrapWithExitableDirectoryReader
    ) throws IOException {
        super(reader, similarity, queryCache, queryCachingPolicy, wrapWithExitableDirectoryReader);
        this.client = client;
        this.shardId = shardId;
        this.tableName = Utils.getHavenaskTableName(shardId);
        this.searchContext = searchContext;
    }

    @Override
    public void search(Query query, Collector collector) throws IOException {
        String sql = QueryTransformer.toSql(tableName, searchContext.request().source(), searchContext.indexShard().mapperService());
        HavenaskSqlResponse response = client.execute(HavenaskSqlAction.INSTANCE, new HavenaskSqlRequest(sql, null)).actionGet();
        if (false == Strings.isNullOrEmpty(response.getResult())) {
            SqlResponse sqlResponse = SqlResponse.parse(response.getResult());
            if (logger.isDebugEnabled()) {
                logger.debug("sql: {}, sqlResponse took: {} ms", sql, sqlResponse.getTotalTime());
            }
            buildQuerySearchResult(searchContext.queryResult(), sqlResponse, searchContext.readerContext());
        }
        searchContext.skipQueryCollectors(true);
    }

    public static void buildQuerySearchResult(QuerySearchResult querySearchResult, SqlResponse sqlResponse, ReaderContext readerContext)
        throws IOException {
        ScoreDoc[] queryScoreDocs = new ScoreDoc[sqlResponse.getRowCount()];
        List<String> idList = new ArrayList<>(sqlResponse.getRowCount());
        float maxScore = 0;
        int sqlDataSize = sqlResponse.getRowCount() > 0 ? sqlResponse.getSqlResult().getData()[0].length : 0;
        if (sqlDataSize > 2) {
            throw new IOException("unknow sqlResponse:" + sqlResponse.getSqlResult().getData().toString());
        }
        for (int i = 0; i < sqlResponse.getRowCount(); i++) {
            float defaultScore = sqlResponse.getRowCount() - i;
            float curScore = sqlDataSize == 1 ? defaultScore : ((Double) sqlResponse.getSqlResult().getData()[i][SCORE_POS]).floatValue();

            queryScoreDocs[i] = new ScoreDoc(i, curScore);
            maxScore = maxScore > curScore ? maxScore : curScore;
            idList.add(String.valueOf(sqlResponse.getSqlResult().getData()[i][ID_POS]));
        }
        readerContext.putInContext(IDS_CONTEXT, idList);
        TopDocs topDocs = new TopDocs(new TotalHits(sqlResponse.getRowCount(), Relation.GREATER_THAN_OR_EQUAL_TO), queryScoreDocs);
        TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(topDocs, maxScore);
        querySearchResult.topDocs(topDocsAndMaxScore, new DocValueFormat[] { DocValueFormat.RAW });
    }
}
