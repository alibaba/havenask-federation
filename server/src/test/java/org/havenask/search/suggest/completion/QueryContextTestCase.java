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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.search.suggest.completion;

import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;


public abstract class QueryContextTestCase<QC extends ToXContent> extends HavenaskTestCase {
    private static final int NUMBER_OF_RUNS = 20;

    /**
     * create random model that is put under test
     */
    protected abstract QC createTestModel();

    /**
     * read the context
     */
    protected abstract QC fromXContent(XContentParser parser) throws IOException;

    public void testToXContext() throws IOException {
        for (int i = 0; i < NUMBER_OF_RUNS; i++) {
            QC toXContent = createTestModel();
            XContentBuilder builder = XContentFactory.jsonBuilder();
            toXContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            XContentParser parser = createParser(builder);
            parser.nextToken();
            QC fromXContext = fromXContent(parser);
            assertEquals(toXContent, fromXContext);
            assertEquals(toXContent.hashCode(), fromXContext.hashCode());
        }
    }
}
