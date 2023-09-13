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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.havenask.engine.index.query.HnswQuery;
import org.havenask.test.HavenaskTestCase;

public class QueryTransformerTests extends HavenaskTestCase {
    public void testMatchAllDocsQuery() throws IOException {
        String sql = QueryTransformer.toSql("table", new MatchAllDocsQuery());
        assertEquals(sql, "select _id from table");
    }

    public void testProximaQuery() throws IOException {
        HnswQuery hnswQuery = new HnswQuery("field", new float[] { 1.0f, 2.0f }, 20, null, null, null);
        String sql = QueryTransformer.toSql("table", hnswQuery);
        assertEquals(sql, "select _id from table where MATCHINDEX('field', '1.0,2.0&n=20')");
    }

    public void testUnsupportedDSL() throws IOException {
        try {
            QueryTransformer.toSql("table", new TermQuery(new Term("field", "value")));
            fail();
        } catch (IOException e) {
            assertEquals(e.getMessage(), "unsupported DSL query:field:value");
        }
    }
}
