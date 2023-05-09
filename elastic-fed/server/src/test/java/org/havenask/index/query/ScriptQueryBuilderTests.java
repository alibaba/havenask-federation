/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.index.query;

import org.apache.lucene.search.Query;
import org.havenask.HavenaskException;
import org.havenask.common.ParsingException;
import org.havenask.script.MockScriptEngine;
import org.havenask.script.Script;
import org.havenask.script.ScriptType;
import org.havenask.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptQueryBuilderTests extends AbstractQueryTestCase<ScriptQueryBuilder> {
    @Override
    protected ScriptQueryBuilder doCreateTestQueryBuilder() {
        String script = "1";
        Map<String, Object> params = Collections.emptyMap();
        return new ScriptQueryBuilder(new Script(ScriptType.INLINE, MockScriptEngine.NAME, script, params));
    }

    @Override
    protected boolean builderGeneratesCacheableQueries() {
        return false;
    }

    @Override
    protected void doAssertLuceneQuery(ScriptQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(ScriptQueryBuilder.ScriptQuery.class));
    }

    public void testIllegalConstructorArg() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new ScriptQueryBuilder((Script) null));
        assertEquals("script cannot be null", e.getMessage());
    }

    public void testFromJsonVerbose() throws IOException {
        String json =
            "{\n" +
                "  \"script\" : {\n" +
                "    \"script\" : {\n" +
                "      \"source\" : \"5\",\n" +
                "      \"lang\" : \"mockscript\"\n" +
                "    },\n" +
                "    \"boost\" : 1.0,\n" +
                "    \"_name\" : \"PcKdEyPOmR\"\n" +
                "  }\n" +
                "}";

        ScriptQueryBuilder parsed = (ScriptQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "mockscript", parsed.script().getLang());
    }

    public void testFromJson() throws IOException {
        String json =
            "{\n" +
                "  \"script\" : {\n" +
                "    \"script\" : \"5\"," +
                "    \"boost\" : 1.0,\n" +
                "    \"_name\" : \"PcKdEyPOmR\"\n" +
                "  }\n" +
                "}";

        ScriptQueryBuilder parsed = (ScriptQueryBuilder) parseQuery(json);
        assertEquals(json, "5", parsed.script().getIdOrCode());
    }

    public void testArrayOfScriptsException() {
        String json =
            "{\n" +
                "  \"script\" : {\n" +
                "    \"script\" : [ {\n" +
                "      \"source\" : \"5\",\n" +
                "      \"lang\" : \"mockscript\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"source\" : \"6\",\n" +
                "      \"lang\" : \"mockscript\"\n" +
                "    }\n ]" +
                "  }\n" +
                "}";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(e.getMessage(), containsString("does not support an array of scripts"));
    }

    @Override
    protected Set<String> getObjectsHoldingArbitraryContent() {
        //script_score.script.params can contain arbitrary parameters. no error is expected when
        //adding additional objects within the params object.
        return Collections.singleton(Script.PARAMS_PARSE_FIELD.getPreferredName());
    }

    /**
     * Check that this query is generally not cacheable
     */
    @Override
    public void testCacheability() throws IOException {
        ScriptQueryBuilder queryBuilder = createTestQueryBuilder();
        QueryShardContext context = createShardContext();
        QueryBuilder rewriteQuery = rewriteQuery(queryBuilder, new QueryShardContext(context));
        assertNotNull(rewriteQuery.toQuery(context));
        assertFalse("query should not be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }

    public void testDisallowExpensiveQueries() {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.allowExpensiveQueries()).thenReturn(false);

        ScriptQueryBuilder queryBuilder = doCreateTestQueryBuilder();
        HavenaskException e = expectThrows(HavenaskException.class,
                () -> queryBuilder.toQuery(queryShardContext));
        assertEquals("[script] queries cannot be executed when 'search.allow_expensive_queries' is set to false.",
                e.getMessage());
    }
}
