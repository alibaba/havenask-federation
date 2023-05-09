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

package org.havenask.search.profile.query;

import org.havenask.common.bytes.BytesReference;
import org.havenask.common.xcontent.ToXContent;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentParserUtils;
import org.havenask.common.xcontent.XContentType;
import org.havenask.search.profile.ProfileResult;
import org.havenask.search.profile.ProfileResultTests;
import org.havenask.test.HavenaskTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.havenask.common.xcontent.XContentHelper.toXContent;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertToXContentEquivalent;

public class QueryProfileShardResultTests extends HavenaskTestCase {

    public static QueryProfileShardResult createTestItem() {
        int size = randomIntBetween(0, 5);
        List<ProfileResult> queryProfileResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            queryProfileResults.add(ProfileResultTests.createTestItem(1));
        }
        CollectorResult profileCollector = CollectorResultTests.createTestItem(2);
        long rewriteTime = randomNonNegativeLong();
        if (randomBoolean()) {
            rewriteTime = rewriteTime % 1000; // make sure to often test this with small values too
        }
        return new QueryProfileShardResult(queryProfileResults, rewriteTime, profileCollector);
    }

    public void testFromXContent() throws IOException {
        QueryProfileShardResult profileResult = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(profileResult, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        QueryProfileShardResult parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsed = QueryProfileShardResult.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

}
