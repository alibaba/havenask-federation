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

import org.havenask.HavenaskException;
import org.havenask.action.support.DefaultShardOperationFailedException;
import org.havenask.common.Strings;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.test.AbstractBroadcastResponseTestCase;
import org.havenask.action.admin.indices.validate.query.QueryExplanation;
import org.havenask.action.admin.indices.validate.query.ValidateQueryResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ValidateQueryResponseTests extends AbstractBroadcastResponseTestCase<ValidateQueryResponse> {

    private static ValidateQueryResponse createRandomValidateQueryResponse(
        int totalShards, int successfulShards, int failedShards, List<DefaultShardOperationFailedException> failures) {
        boolean valid = failedShards == 0;
        List<QueryExplanation> queryExplanations = new ArrayList<>(totalShards);
        for(DefaultShardOperationFailedException failure: failures) {
            queryExplanations.add(
                new QueryExplanation(
                    failure.index(), failure.shardId(), false, failure.reason(), null
                )
            );
        }
        return new ValidateQueryResponse(
            valid, queryExplanations, totalShards, successfulShards, failedShards, failures
        );
    }

    private static ValidateQueryResponse createRandomValidateQueryResponse() {
        int totalShards = randomIntBetween(1, 10);
        int successfulShards = randomIntBetween(0, totalShards);
        int failedShards = totalShards - successfulShards;
        boolean valid = failedShards == 0;
        List<QueryExplanation> queryExplanations = new ArrayList<>(totalShards);
        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>(failedShards);
        for (int i=0; i<successfulShards; i++) {
            QueryExplanation queryExplanation = QueryExplanationTests.createRandomQueryExplanation(true);
            queryExplanations.add(queryExplanation);
        }
        for (int i=0; i<failedShards; i++) {
            QueryExplanation queryExplanation = QueryExplanationTests.createRandomQueryExplanation(false);
            HavenaskException exc = new HavenaskException("some_error_" + randomInt());
            shardFailures.add(
                new DefaultShardOperationFailedException(
                    queryExplanation.getIndex(), queryExplanation.getShard(),
                    exc
                )
            );
            queryExplanations.add(queryExplanation);
        }
        Collections.shuffle(queryExplanations, random());
        return new ValidateQueryResponse(valid, queryExplanations, totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ValidateQueryResponse doParseInstance(XContentParser parser) throws IOException {
        return ValidateQueryResponse.fromXContent(parser);
    }

    @Override
    protected ValidateQueryResponse createTestInstance() {
        return createRandomValidateQueryResponse();
    }

    @Override
    protected void assertEqualInstances(ValidateQueryResponse response, ValidateQueryResponse parsedResponse) {
        super.assertEqualInstances(response, parsedResponse);
        Set<QueryExplanation> queryExplSet = new HashSet<>(response.getQueryExplanation());
        assertEquals(response.isValid(), parsedResponse.isValid());
        assertEquals(response.getQueryExplanation().size(), parsedResponse.getQueryExplanation().size());
        assertTrue(queryExplSet.containsAll(parsedResponse.getQueryExplanation()));
    }

    @Override
    protected ValidateQueryResponse createTestInstance(int totalShards, int successfulShards, int failedShards,
                                                       List<DefaultShardOperationFailedException> failures) {
        return createRandomValidateQueryResponse(totalShards, successfulShards, failedShards, failures);
    }

    @Override
    public void testToXContent() {
        ValidateQueryResponse response = createTestInstance(10, 10, 0, new ArrayList<>());
        String output = Strings.toString(response);
        assertEquals("{\"_shards\":{\"total\":10,\"successful\":10,\"failed\":0},\"valid\":true}", output);
    }
}
