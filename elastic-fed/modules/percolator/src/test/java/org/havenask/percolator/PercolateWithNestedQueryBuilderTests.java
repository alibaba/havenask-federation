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

package org.havenask.percolator;

import org.havenask.action.admin.indices.mapping.put.PutMappingRequest;
import org.havenask.common.Strings;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.compress.CompressedXContent;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.mapper.MapperService;
import org.havenask.index.query.QueryBuilder;
import org.havenask.index.query.QueryShardContext;

import java.io.IOException;

public class PercolateWithNestedQueryBuilderTests extends PercolateQueryBuilderTests {

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        super.initializeAdditionalMappings(mapperService);
        mapperService.merge("_doc", new CompressedXContent(Strings.toString(PutMappingRequest.buildFromSimplifiedDef(
                "_doc", "some_nested_object", "type=nested"))), MapperService.MergeReason.MAPPING_UPDATE);
    }

    public void testDetectsNestedDocuments() throws IOException {
        QueryShardContext shardContext = createShardContext();

        PercolateQueryBuilder builder = new PercolateQueryBuilder(queryField,
                new BytesArray("{ \"foo\": \"bar\" }"), XContentType.JSON);
        QueryBuilder rewrittenBuilder = rewriteAndFetch(builder, shardContext);
        PercolateQuery query = (PercolateQuery) rewrittenBuilder.toQuery(shardContext);
        assertFalse(query.excludesNestedDocs());

        builder = new PercolateQueryBuilder(queryField,
                new BytesArray("{ \"foo\": \"bar\", \"some_nested_object\": [ { \"baz\": 42 } ] }"), XContentType.JSON);
        rewrittenBuilder = rewriteAndFetch(builder, shardContext);
        query = (PercolateQuery) rewrittenBuilder.toQuery(shardContext);
        assertTrue(query.excludesNestedDocs());
    }
}
