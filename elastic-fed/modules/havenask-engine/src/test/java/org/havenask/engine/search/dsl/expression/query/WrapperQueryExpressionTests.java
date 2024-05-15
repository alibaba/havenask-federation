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

package org.havenask.engine.search.dsl.expression.query;

import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.engine.search.dsl.expression.ExpressionContext;
import org.havenask.env.Environment;
import org.havenask.index.query.BoolQueryBuilder;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.TermQueryBuilder;
import org.havenask.index.query.WrapperQueryBuilder;
import org.havenask.search.SearchModule;
import org.havenask.test.AbstractQueryTestCase;
import org.havenask.test.HavenaskTestCase;

import static java.util.Collections.emptyList;

public class WrapperQueryExpressionTests extends HavenaskTestCase {

    private ExpressionContext context;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder()
            .put("node.name", AbstractQueryTestCase.class.toString())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .build();
        NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
            new SearchModule(settings, false, emptyList()).getNamedXContents()
        );
        context = new ExpressionContext(xContentRegistry, null, -1);
    }

    public void testWrapperQueryExpression() {
        WrapperQueryBuilder wrapperQueryBuilder = new WrapperQueryBuilder("{ \"term\" : { \"field\" : \"value\" } }");
        WrapperQueryExpression wrapperQueryExpression = new WrapperQueryExpression(wrapperQueryBuilder, context);
        QueryBuilder queryBuilder = wrapperQueryExpression.getQueryBuilder();
        assertNotNull(queryBuilder);
        assertTrue(queryBuilder instanceof TermQueryBuilder);
        assertEquals("term", queryBuilder.getName());
        assertEquals("field", ((TermQueryBuilder) queryBuilder).fieldName());
        assertEquals(wrapperQueryExpression.translate(), "`field` = 'value'");
    }

    // test wrapper bool query
    public void testWrapperBoolQueryExpression() {
        WrapperQueryBuilder wrapperQueryBuilder = new WrapperQueryBuilder(
            "{ \"bool\" : { \"must\" : { \"term\" : { \"field\" : \"value\" } } } }"
        );
        WrapperQueryExpression wrapperQueryExpression = new WrapperQueryExpression(wrapperQueryBuilder, context);
        QueryBuilder queryBuilder = wrapperQueryExpression.getQueryBuilder();
        assertNotNull(queryBuilder);
        assertTrue(queryBuilder instanceof BoolQueryBuilder);
        assertEquals(wrapperQueryExpression.translate(), "(`field` = 'value')");
    }

}
