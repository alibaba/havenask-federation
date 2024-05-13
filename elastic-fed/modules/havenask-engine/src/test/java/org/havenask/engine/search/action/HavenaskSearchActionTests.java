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

package org.havenask.engine.search.action;

import static org.havenask.engine.search.action.TransportHavenaskSearchAction.isSearchHavenask;

import org.havenask.Version;
import org.havenask.action.search.SearchRequest;
import org.havenask.cluster.metadata.AliasMetadata;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.common.settings.Settings;
import org.havenask.test.HavenaskTestCase;

public class HavenaskSearchActionTests extends HavenaskTestCase {

    // test isSearchHavenask
    public void testIsSearchHavenask() {
        Metadata metadata = initMetadata();

        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("havenask-1");
            assertTrue(isSearchHavenask(metadata, searchRequest));
        }

        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("havenask-*");
            assertFalse(isSearchHavenask(metadata, searchRequest));
        }

        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("havenask-1", "havenask-2");
            assertFalse(isSearchHavenask(metadata, searchRequest));
        }

        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("havenask-3");
            assertFalse(isSearchHavenask(metadata, searchRequest));
        }

        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("havenask-alias-1");
            assertTrue(isSearchHavenask(metadata, searchRequest));
        }

        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("havenask-alias-2");
            assertFalse(isSearchHavenask(metadata, searchRequest));
        }

        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("havenask-alias-3");
            assertFalse(isSearchHavenask(metadata, searchRequest));
        }

        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("havenask-not-found");
            assertFalse(isSearchHavenask(metadata, searchRequest));
        }
    }

    private Metadata initMetadata() {
        IndexMetadata.Builder index1 = IndexMetadata.builder("havenask-1")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put("index.engine", "havenask"))
            .numberOfShards(1)
            .numberOfReplicas(0);
        IndexMetadata.Builder index2 = IndexMetadata.builder("havenask-2")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put("index.engine", "havenask"))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("havenask-alias-1"));
        IndexMetadata.Builder index3 = IndexMetadata.builder("havenask-3")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("havenask-alias-2"));
        IndexMetadata.Builder index4 = IndexMetadata.builder("havenask-4")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("havenask-alias-2"));
        IndexMetadata.Builder index5 = IndexMetadata.builder("havenask-5")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("havenask-alias-3"));
        return Metadata.builder().put(index1).put(index2).put(index3).put(index4).put(index5).build();
    }
}
