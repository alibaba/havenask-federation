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

package org.havenask.index.mapper.size;

import java.util.Collection;

import org.havenask.common.Strings;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentFactory;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.IndexService;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.mapper.DocumentMapper;
import org.havenask.index.mapper.ParsedDocument;
import org.havenask.index.mapper.SourceToParse;
import org.havenask.plugin.mapper.MapperSizePlugin;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskSingleNodeTestCase;
import org.havenask.test.InternalSettingsPlugin;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.apache.lucene.index.IndexableField;

public class SizeMappingTests extends HavenaskSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperSizePlugin.class, InternalSettingsPlugin.class);
    }

    public void testSizeEnabled() throws Exception {
        IndexService service = createIndex("test", Settings.EMPTY, "type", "_size", "enabled=true");
        DocumentMapper docMapper = service.mapperService().documentMapper("type");

        BytesReference source = BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject());
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "type", "1", source, XContentType.JSON));

        boolean stored = false;
        boolean points = false;
        for (IndexableField field : doc.rootDoc().getFields("_size")) {
            stored |= field.fieldType().stored();
            points |= field.fieldType().pointIndexDimensionCount() > 0;
        }
        assertTrue(stored);
        assertTrue(points);
    }

    public void testSizeDisabled() throws Exception {
        IndexService service = createIndex("test", Settings.EMPTY, "type", "_size", "enabled=false");
        DocumentMapper docMapper = service.mapperService().documentMapper("type");

        BytesReference source = BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject());
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "type", "1", source, XContentType.JSON));

        assertThat(doc.rootDoc().getField("_size"), nullValue());
    }

    public void testSizeNotSet() throws Exception {
        IndexService service = createIndex("test", Settings.EMPTY, "type");
        DocumentMapper docMapper = service.mapperService().documentMapper("type");

        BytesReference source = BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject());
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "type", "1", source, XContentType.JSON));

        assertThat(doc.rootDoc().getField("_size"), nullValue());
    }

    public void testThatDisablingWorksWhenMerging() throws Exception {
        IndexService service = createIndex("test", Settings.EMPTY, "type", "_size", "enabled=true");
        DocumentMapper docMapper = service.mapperService().documentMapper("type");
        assertThat(docMapper.metadataMapper(SizeFieldMapper.class).enabled(), is(true));

        String disabledMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_size").field("enabled", false).endObject()
            .endObject().endObject());
        docMapper = service.mapperService().merge("type", new CompressedXContent(disabledMapping),
            MapperService.MergeReason.MAPPING_UPDATE);

        assertThat(docMapper.metadataMapper(SizeFieldMapper.class).enabled(), is(false));
    }

}
