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

package org.havenask.index.rankeval;

import org.havenask.action.search.SearchType;
import org.havenask.action.support.IndicesOptions;
import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.Writeable.Reader;
import org.havenask.common.util.ArrayUtils;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.test.AbstractWireSerializingTestCase;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RankEvalRequestTests extends AbstractWireSerializingTestCase<RankEvalRequest> {

    private static RankEvalPlugin rankEvalPlugin = new RankEvalPlugin();

    @AfterClass
    public static void releasePluginResources() throws IOException {
        rankEvalPlugin.close();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(rankEvalPlugin.getNamedXContent());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(rankEvalPlugin.getNamedWriteables());
    }

    @Override
    protected RankEvalRequest createTestInstance() {
        int numberOfIndices = randomInt(3);
        String[] indices = new String[numberOfIndices];
        for (int i=0; i < numberOfIndices; i++) {
            indices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        RankEvalRequest rankEvalRequest = new RankEvalRequest(RankEvalSpecTests.createTestItem(), indices);
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(
                randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
            randomBoolean());
        rankEvalRequest.indicesOptions(indicesOptions);
        rankEvalRequest.searchType(randomFrom(SearchType.DFS_QUERY_THEN_FETCH, SearchType.QUERY_THEN_FETCH));
        return rankEvalRequest;
    }

    @Override
    protected Reader<RankEvalRequest> instanceReader() {
        return RankEvalRequest::new;
    }

    @Override
    protected RankEvalRequest mutateInstance(RankEvalRequest instance) throws IOException {
        RankEvalRequest mutation = copyInstance(instance);
        List<Runnable> mutators = new ArrayList<>();
        mutators.add(() -> mutation.indices(ArrayUtils.concat(instance.indices(), new String[] { randomAlphaOfLength(10) })));
        mutators.add(() -> mutation.indicesOptions(randomValueOtherThan(instance.indicesOptions(),
                () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()))));
        mutators.add(() -> {
            if (instance.searchType() == SearchType.DFS_QUERY_THEN_FETCH) {
                mutation.searchType(SearchType.QUERY_THEN_FETCH);
            } else {
                mutation.searchType(SearchType.DFS_QUERY_THEN_FETCH);
            }
        });
        mutators.add(() -> mutation.setRankEvalSpec(RankEvalSpecTests.mutateTestItem(instance.getRankEvalSpec())));
        mutators.add(() -> mutation.setRankEvalSpec(RankEvalSpecTests.mutateTestItem(instance.getRankEvalSpec())));
        randomFrom(mutators).run();
        return mutation;
    }

}
