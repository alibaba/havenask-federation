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

package org.havenask.document;

import org.havenask.action.admin.indices.alias.Alias;

import static org.havenask.client.Requests.createIndexRequest;

public class AliasedIndexDocumentActionsIT extends DocumentActionsIT {

    @Override
    protected void createIndex() {
        logger.info("Creating index [test1] with alias [test]");
        try {
            client().admin().indices().prepareDelete("test1").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        logger.info("--> creating index test");
        client().admin().indices().create(createIndexRequest("test1")
                .mapping("type1", "name", "type=keyword,store=true")
                .alias(new Alias("test"))).actionGet();
    }

    @Override
    protected String getConcreteIndexName() {
        return "test1";
    }
}
