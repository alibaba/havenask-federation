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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.common.util.BigArrays;
import org.havenask.index.IndexSettings;
import org.havenask.index.query.QueryShardContext;
import org.havenask.test.HavenaskTestCase;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FieldNamesFieldTypeTests extends HavenaskTestCase {

    public void testTermQuery() {

        FieldNamesFieldMapper.FieldNamesFieldType fieldNamesFieldType = new FieldNamesFieldMapper.FieldNamesFieldType(true);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field_name");

        Settings settings = settings(Version.CURRENT).build();
        IndexSettings indexSettings = new IndexSettings(
                new IndexMetadata.Builder("foo").settings(settings).numberOfShards(1).numberOfReplicas(0).build(), settings);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType("_field_names")).thenReturn(fieldNamesFieldType);
        when(mapperService.fieldType("field_name")).thenReturn(fieldType);
        when(mapperService.simpleMatchToFullName("field_name")).thenReturn(Collections.singleton("field_name"));

        QueryShardContext queryShardContext = new QueryShardContext(0,
                indexSettings, BigArrays.NON_RECYCLING_INSTANCE, null, null, mapperService,
                null, null, null, null, null, null, () -> 0L, null, null, () -> true, null);
                Query termQuery = fieldNamesFieldType.termQuery("field_name", queryShardContext);
        assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.CONTENT_TYPE, "field_name")), termQuery);
        assertWarnings("terms query on the _field_names field is deprecated and will be removed, use exists query instead");

        FieldNamesFieldMapper.FieldNamesFieldType unsearchable = new FieldNamesFieldMapper.FieldNamesFieldType(false);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> unsearchable.termQuery("field_name", null));
        assertEquals("Cannot run [exists] queries if the [_field_names] field is disabled", e.getMessage());
    }
}
