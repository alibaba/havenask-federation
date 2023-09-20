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

package org.havenask.engine.search.fetch;

import org.havenask.index.shard.IndexShard;
import org.havenask.index.shard.ShardId;
import org.havenask.search.SearchHit;
import org.havenask.search.fetch.subphase.FetchSourceContext;
import org.havenask.engine.search.fetch.FetchSubPhase.HitContent;
import org.havenask.search.internal.SearchContext;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HavenaskFetchPhaseTests extends HavenaskTestCase {
    public void testHitExecute() throws IOException {
        String indexName = "test";
        String[][] includes = new String[][] { { "name" }, {}, { "key1", "length" } };
        String[][] excludes = new String[][] { {}, { "key1" }, {} };
        String[] resSourceStr = new String[] {
            "{\"name\":\"alice\"}",
            "{\"name\":\"alice\",\"length\":1}",
            "{\"key1\":\"doc1\",\"length\":1}" };

        for (int i = 0; i < includes.length; i++) {
            FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes[i], excludes[i]);

            SearchHit searchHit = new SearchHit(-1);
            String sourceStr = "{\n" + "  \"key1\" :\"doc1\",\n" + "  \"name\" :\"alice\",\n" + "  \"length\":1\n" + "}\n";
            Object source = sourceStr;
            HitContent hit = new HitContent(searchHit, source);

            // mock SearchContext
            SearchContext searchContext = mock(SearchContext.class);
            IndexShard indexShard = mock(IndexShard.class);
            ShardId shardId = mock(ShardId.class);
            when(indexShard.shardId()).thenReturn(shardId);
            when(shardId.getIndexName()).thenReturn(indexName);
            when(searchContext.indexShard()).thenReturn(indexShard);
            when(searchContext.fetchSourceContext()).thenReturn(fetchSourceContext);

            searchContext.fetchSourceContext(fetchSourceContext);
            FetchSubPhaseProcessor processor = new FetchSourcePhase().getProcessor(searchContext);
            processor.process(hit);

            SearchHit res = hit.getHit();
            assertEquals(resSourceStr[i], res.getSourceAsString());
        }
    }
}
