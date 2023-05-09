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

package org.havenask.indices.template;

import org.havenask.action.admin.indices.alias.Alias;
import org.havenask.action.admin.indices.template.get.GetIndexTemplatesResponse;

import org.havenask.common.xcontent.XContentFactory;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.HavenaskIntegTestCase.ClusterScope;

import java.io.IOException;
import java.util.Collections;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertBlocked;
import static org.hamcrest.Matchers.hasSize;

@ClusterScope(scope = HavenaskIntegTestCase.Scope.TEST)
public class IndexTemplateBlocksIT extends HavenaskIntegTestCase {
    public void testIndexTemplatesWithBlocks() throws IOException {
        // creates a simple index template
        client().admin().indices().preparePutTemplate("template_blocks")
                .setPatterns(Collections.singletonList("te*"))
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "text").field("store", true).endObject()
                        .startObject("field2").field("type", "keyword").field("store", true).endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        try {
            setClusterReadOnly(true);

            GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates("template_blocks").execute().actionGet();
            assertThat(response.getIndexTemplates(), hasSize(1));

            assertBlocked(client().admin().indices().preparePutTemplate("template_blocks_2")
                    .setPatterns(Collections.singletonList("block*"))
                    .setOrder(0)
                    .addAlias(new Alias("alias_1")));

            assertBlocked(client().admin().indices().prepareDeleteTemplate("template_blocks"));

        } finally {
            setClusterReadOnly(false);
        }
    }
}
