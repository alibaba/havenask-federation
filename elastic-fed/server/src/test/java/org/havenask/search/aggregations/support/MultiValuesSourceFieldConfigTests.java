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

package org.havenask.search.aggregations.support;

import org.havenask.common.io.stream.NamedWriteableRegistry;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.QueryBuilders;
import org.havenask.script.Script;
import org.havenask.search.SearchModule;
import org.havenask.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class MultiValuesSourceFieldConfigTests extends AbstractSerializingTestCase<MultiValuesSourceFieldConfig> {

    @Override
    protected MultiValuesSourceFieldConfig doParseInstance(XContentParser parser) throws IOException {
        return MultiValuesSourceFieldConfig.PARSER.apply(true, true, true).apply(parser, null).build();
    }

    @Override
    protected MultiValuesSourceFieldConfig createTestInstance() {
        String field = randomAlphaOfLength(10);
        Object missing = randomBoolean() ? randomAlphaOfLength(10) : null;
        ZoneId timeZone = randomBoolean() ? randomZone() : null;
        QueryBuilder filter = randomBoolean() ? QueryBuilders.termQuery(randomAlphaOfLength(10), randomAlphaOfLength(10)) : null;
        return new MultiValuesSourceFieldConfig.Builder()
            .setFieldName(field).setMissing(missing).setScript(null).setTimeZone(timeZone).setFilter(filter).build();
    }

    @Override
    protected Writeable.Reader<MultiValuesSourceFieldConfig> instanceReader() {
        return MultiValuesSourceFieldConfig::new;
    }

    public void testMissingFieldScript() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MultiValuesSourceFieldConfig.Builder().build());
        assertThat(e.getMessage(), equalTo("[field] and [script] cannot both be null.  Please specify one or the other."));
    }

    public void testBothFieldScript() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new MultiValuesSourceFieldConfig.Builder().setFieldName("foo").setScript(new Script("foo")).build());
        assertThat(e.getMessage(), equalTo("[field] and [script] cannot both be configured.  Please specify one or the other."));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedXContents());
    }
}
