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

package org.havenask.engine.index.query;

import org.apache.lucene.search.Query;
import org.havenask.engine.HavenaskEnginePlugin;
import org.havenask.index.query.QueryShardContext;
import org.havenask.plugins.Plugin;
import org.havenask.test.AbstractQueryTestCase;
import org.havenask.test.TestGeoShapeFieldMapperPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class LinearQueryBuilderTests extends AbstractQueryTestCase<LinearQueryBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(HavenaskEnginePlugin.class, TestGeoShapeFieldMapperPlugin.class);
    }

    @Override
    protected LinearQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = "test";
        float[] vector = { 1.5f, 2.5f };
        int size = 10;
        return new LinearQueryBuilder(fieldName, vector, size);
    }

    @Override
    protected void doAssertLuceneQuery(LinearQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        return;
    }

    public void testFromJson() throws IOException {
        String json = "{\n"
            + "    \"linear\": {\n"
            + "      \"feature\": {\n"
            + "        \"vector\": [1.5, 2.5],\n"
            + "        \"size\": 10\n"
            + "      }\n"
            + "    }\n"
            + "}";

        LinearQueryBuilder parsed = (LinearQueryBuilder) parseQuery(json);
        assertEquals(json, "feature", parsed.getFieldName());
        assertEquals(10, parsed.getSize());
        float[] expectedVector = new float[] { 1.5f, 2.5f };
        assertTrue(Arrays.equals(expectedVector, parsed.getVector()));
        return;
    }
}
