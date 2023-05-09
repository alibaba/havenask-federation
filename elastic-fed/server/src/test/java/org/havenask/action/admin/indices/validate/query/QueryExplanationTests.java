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

package org.havenask.action.admin.indices.validate.query;

import org.havenask.common.io.stream.Writeable;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.test.AbstractSerializingTestCase;
import org.havenask.action.admin.indices.validate.query.QueryExplanation;

import java.io.IOException;

public class QueryExplanationTests extends AbstractSerializingTestCase<QueryExplanation> {

    static QueryExplanation createRandomQueryExplanation(boolean isValid) {
        String index = "index_" + randomInt(1000);
        int shard = randomInt(100);
        Boolean valid = isValid;
        String errorField = null;
        if (!valid) {
            errorField = randomAlphaOfLength(randomIntBetween(10, 100));
        }
        String explanation = randomAlphaOfLength(randomIntBetween(10, 100));
        return new QueryExplanation(index, shard, valid, explanation, errorField);
    }

    static QueryExplanation createRandomQueryExplanation() {
        return createRandomQueryExplanation(randomBoolean());
    }

    @Override
    protected QueryExplanation doParseInstance(XContentParser parser) throws IOException {
        return QueryExplanation.fromXContent(parser);
    }

    @Override
    protected QueryExplanation createTestInstance() {
        return createRandomQueryExplanation();
    }

    @Override
    protected Writeable.Reader<QueryExplanation> instanceReader() {
        return QueryExplanation::new;
    }
}
