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

package org.havenask.client.documentation;

import org.havenask.action.admin.indices.mapping.get.GetMappingsResponse;
import org.havenask.client.Client;
import org.havenask.cluster.metadata.MappingMetadata;
import org.havenask.common.collect.ImmutableOpenMap;
import org.havenask.common.xcontent.XContentType;
import org.havenask.test.HavenaskIntegTestCase;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.instanceOf;

/**
 * This class is used to generate the Java indices administration documentation.
 * You need to wrap your code between two tags like:
 * // tag::example[]
 * // end::example[]
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags
 * with ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{client-tests}/IndicesDocumentationIT.java[your-example-tag-here]
 * --------------------------------------------------
 */
public class IndicesDocumentationIT extends HavenaskIntegTestCase {

    /**
     * This test method is used to generate the Put Mapping Java Indices API documentation
     * at "docs/java-api/admin/indices/put-mapping.asciidoc" so the documentation gets tested
     * so that it compiles and runs without throwing errors at runtime.
     */
     public void testPutMappingDocumentation() throws Exception {
        Client client = client();

        // tag::index-with-mapping
        client.admin().indices().prepareCreate("twitter")    // <1>
                .addMapping("_doc", "message", "type=text") // <2>
                .get();
        // end::index-with-mapping
        GetMappingsResponse getMappingsResponse = client.admin().indices().prepareGetMappings("twitter").get();
        assertEquals(1, getMappingsResponse.getMappings().size());
        ImmutableOpenMap<String, MappingMetadata> indexMapping = getMappingsResponse.getMappings().get("twitter");
        assertThat(indexMapping.get("_doc"), instanceOf(MappingMetadata.class));

        // we need to delete in order to create a fresh new index with another type
        client.admin().indices().prepareDelete("twitter").get();
        client.admin().indices().prepareCreate("twitter").get();

        // tag::putMapping-request-source
        client.admin().indices().preparePutMapping("twitter")   // <1>
        .setType("_doc")
        .setSource("{\n" +
                "  \"properties\": {\n" +
                "    \"name\": {\n" +                           // <2>
                "      \"type\": \"text\"\n" +
                "    }\n" +
                "  }\n" +
                "}", XContentType.JSON)
        .get();

        // You can also provide the type in the source document
        client.admin().indices().preparePutMapping("twitter")
        .setType("_doc")
        .setSource("{\n" +
                "    \"_doc\":{\n" +                            // <3>
                "        \"properties\": {\n" +
                "            \"name\": {\n" +
                "                \"type\": \"text\"\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}", XContentType.JSON)
        .get();
        // end::putMapping-request-source
        getMappingsResponse = client.admin().indices().prepareGetMappings("twitter").get();
        assertEquals(1, getMappingsResponse.getMappings().size());
        indexMapping = getMappingsResponse.getMappings().get("twitter");
        assertEquals(singletonMap("properties", singletonMap("name", singletonMap("type", "text"))),
                indexMapping.get("_doc").getSourceAsMap());
    }

}
