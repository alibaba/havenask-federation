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

package org.havenask.index.mapper;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.UUIDs;
import org.havenask.common.settings.Settings;
import org.havenask.index.IndexSettings;
import org.havenask.index.query.QueryShardContext;
import org.havenask.test.HavenaskTestCase;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Collections;

public class IdFieldTypeTests extends HavenaskTestCase {

    public void testRangeQuery() {
        MappedFieldType ft = new IdFieldMapper.IdFieldType(() -> false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals("Field [_id] of type [_id] does not support range queries", e.getMessage());
    }

    public void testTermsQuery() {
        QueryShardContext context = Mockito.mock(QueryShardContext.class);
        Settings indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(IndexMetadata.INDEX_UUID_NA_VALUE).settings(indexSettings).build();
        IndexSettings mockSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        Mockito.when(context.getIndexSettings()).thenReturn(mockSettings);
        Mockito.when(context.indexVersionCreated()).thenReturn(indexSettings.getAsVersion(IndexMetadata.SETTING_VERSION_CREATED, null));

        MapperService mapperService = Mockito.mock(MapperService.class);
        Collection<String> types = Collections.emptySet();
        Mockito.when(context.queryTypes()).thenReturn(types);
        Mockito.when(context.getMapperService()).thenReturn(mapperService);

        MappedFieldType ft = new IdFieldMapper.IdFieldType(() -> false);
        Query query = ft.termQuery("id", context);
        assertEquals(new TermInSetQuery("_id", Uid.encodeId("id")), query);

        types = Collections.singleton("type");
        Mockito.when(context.queryTypes()).thenReturn(types);
        query = ft.termQuery("id", context);
        assertEquals(new TermInSetQuery("_id", Uid.encodeId("id")), query);
    }

    public void testIsAggregatable() {
        MappedFieldType ft = new IdFieldMapper.IdFieldType(() -> false);
        assertFalse(ft.isAggregatable());

        ft = new IdFieldMapper.IdFieldType(() -> true);
        assertTrue(ft.isAggregatable());
    }
}
