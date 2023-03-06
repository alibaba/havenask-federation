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

import org.apache.lucene.index.IndexableField;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.IndexService;
import org.havenask.index.mapper.ParseContext.Document;
import org.havenask.test.HavenaskSingleNodeTestCase;

import static org.havenask.test.StreamsUtils.copyToBytesFromClasspath;
import static org.havenask.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.equalTo;

public class PathMatchDynamicTemplateTests extends HavenaskSingleNodeTestCase {
    public void testSimple() throws Exception {
        String mapping = copyToStringFromClasspath("/org/havenask/index/mapper/dynamictemplate/pathmatch/test-mapping.json");
        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setType("person").setSource(mapping, XContentType.JSON).get();

        MapperService mapperService = index.mapperService();

        byte[] json = copyToBytesFromClasspath("/org/havenask/index/mapper/dynamictemplate/pathmatch/test-data.json");
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(
            new SourceToParse("test", "person", "1", new BytesArray(json), XContentType.JSON));
        client().admin().indices().preparePutMapping("test").setType("person")
            .setSource(parsedDoc.dynamicMappingsUpdate().toString(), XContentType.JSON).get();
        Document doc = parsedDoc.rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.stringValue(), equalTo("top_level"));
        assertThat(f.fieldType().stored(), equalTo(false));

        assertThat(mapperService.fieldType("name").isStored(), equalTo(false));

        f = doc.getField("obj1.name");
        assertThat(f.name(), equalTo("obj1.name"));
        assertThat(f.fieldType().stored(), equalTo(true));

        assertThat(mapperService.fieldType("obj1.name").isStored(), equalTo(true));

        f = doc.getField("obj1.obj2.name");
        assertThat(f.name(), equalTo("obj1.obj2.name"));
        assertThat(f.fieldType().stored(), equalTo(false));

        assertThat(mapperService.fieldType("obj1.obj2.name").isStored(), equalTo(false));

        // verify more complex path_match expressions
        assertNotNull(mapperService.fieldType("obj3.obj4.prop1").getTextSearchInfo());
    }
}
