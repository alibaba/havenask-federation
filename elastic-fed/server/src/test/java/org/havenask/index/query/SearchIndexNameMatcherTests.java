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

import org.havenask.Version;
import org.havenask.cluster.ClusterName;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.AliasMetadata;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.concurrent.ThreadContext;
import org.havenask.test.HavenaskTestCase;
import org.junit.Before;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchIndexNameMatcherTests extends HavenaskTestCase {
    private SearchIndexNameMatcher matcher;
    private SearchIndexNameMatcher remoteMatcher;

    @Before
    public void setUpMatchers() {
        Metadata.Builder metadataBuilder = Metadata.builder()
            .put(indexBuilder("index1").putAlias(AliasMetadata.builder("alias")))
            .put(indexBuilder("index2").putAlias(AliasMetadata.builder("alias")))
            .put(indexBuilder("index3"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(metadataBuilder).build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);

        matcher = new SearchIndexNameMatcher("index1", "", clusterService,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)));
        remoteMatcher = new SearchIndexNameMatcher("index1", "cluster", clusterService,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)));
    }

    private static IndexMetadata.Builder indexBuilder(String index) {
        Settings.Builder settings = settings(Version.CURRENT).
                put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        return IndexMetadata.builder(index).settings(settings);
    }

    public void testLocalIndex() {
        assertTrue(matcher.test("index1"));
        assertTrue(matcher.test("ind*x1"));
        assertFalse(matcher.test("index2"));

        assertTrue(matcher.test("alias"));
        assertTrue(matcher.test("*lias"));

        assertFalse(matcher.test("cluster:index1"));
    }

    public void testRemoteIndex() {
        assertTrue(remoteMatcher.test("cluster:index1"));
        assertTrue(remoteMatcher.test("cluster:ind*x1"));
        assertTrue(remoteMatcher.test("*luster:ind*x1"));
        assertFalse(remoteMatcher.test("cluster:index2"));

        assertTrue(remoteMatcher.test("cluster:alias"));
        assertTrue(remoteMatcher.test("cluster:*lias"));

        assertFalse(remoteMatcher.test("index1"));
        assertFalse(remoteMatcher.test("alias"));

        assertFalse(remoteMatcher.test("*index1"));
        assertFalse(remoteMatcher.test("*alias"));
        assertFalse(remoteMatcher.test("cluster*"));
        assertFalse(remoteMatcher.test("cluster*index1"));
    }
}
