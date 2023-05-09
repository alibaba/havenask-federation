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

package org.havenask.index.query.functionscore;

import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.settings.Settings;
import org.havenask.index.IndexSettings;
import org.havenask.index.mapper.MappedFieldType;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.NumberFieldMapper;
import org.havenask.index.mapper.NumberFieldMapper.NumberType;
import org.havenask.index.query.QueryShardContext;
import org.havenask.script.Script;
import org.havenask.test.HavenaskTestCase;
import org.mockito.Mockito;

public class ScoreFunctionBuilderTests extends HavenaskTestCase {

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new RandomScoreFunctionBuilder().seed(null));
        expectThrows(IllegalArgumentException.class, () -> new ScriptScoreFunctionBuilder((Script) null));
        expectThrows(IllegalArgumentException.class, () -> new FieldValueFactorFunctionBuilder((String) null));
        expectThrows(IllegalArgumentException.class, () -> new FieldValueFactorFunctionBuilder("").modifier(null));
        expectThrows(IllegalArgumentException.class, () -> new GaussDecayFunctionBuilder(null, "", "", ""));
        expectThrows(IllegalArgumentException.class, () -> new GaussDecayFunctionBuilder("", "", null, ""));
        expectThrows(IllegalArgumentException.class, () -> new GaussDecayFunctionBuilder("", "", null, "", randomDouble()));
        expectThrows(IllegalArgumentException.class, () -> new LinearDecayFunctionBuilder(null, "", "", ""));
        expectThrows(IllegalArgumentException.class, () -> new LinearDecayFunctionBuilder("", "", null, ""));
        expectThrows(IllegalArgumentException.class, () -> new LinearDecayFunctionBuilder("", "", null, "", randomDouble()));
        expectThrows(IllegalArgumentException.class, () -> new ExponentialDecayFunctionBuilder(null, "", "", ""));
        expectThrows(IllegalArgumentException.class, () -> new ExponentialDecayFunctionBuilder("", "", null, ""));
        expectThrows(IllegalArgumentException.class, () -> new ExponentialDecayFunctionBuilder("", "", null, "", randomDouble()));
    }

    public void testRandomScoreFunctionWithSeed() throws Exception {
        RandomScoreFunctionBuilder builder = new RandomScoreFunctionBuilder();
        builder.seed(42);
        QueryShardContext context = Mockito.mock(QueryShardContext.class);
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
        IndexSettings settings = new IndexSettings(IndexMetadata.builder("index").settings(indexSettings).build(), Settings.EMPTY);
        Mockito.when(context.index()).thenReturn(settings.getIndex());
        Mockito.when(context.getShardId()).thenReturn(0);
        Mockito.when(context.getIndexSettings()).thenReturn(settings);
        MapperService mapperService = Mockito.mock(MapperService.class);
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("foo", NumberType.LONG);
        Mockito.when(mapperService.fieldType(Mockito.anyString())).thenReturn(ft);
        Mockito.when(context.getMapperService()).thenReturn(mapperService);
        builder.toFunction(context);
        assertWarnings("Havenask requires that a [field] parameter is provided when a [seed] is set");
    }
}
