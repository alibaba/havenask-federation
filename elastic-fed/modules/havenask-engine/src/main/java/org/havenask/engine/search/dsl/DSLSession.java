/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.engine.search.dsl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.action.search.SearchResponse;
import org.havenask.common.UUIDs;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.search.dsl.plan.DSLExec;
import org.havenask.search.builder.SearchSourceBuilder;

import java.io.IOException;

public class DSLSession {
    protected Logger logger = LogManager.getLogger(DSLSession.class);

    private final QrsClient client;
    private final String index;
    private final long startTime;
    private final String sessionId;
    private SearchSourceBuilder query;

    public DSLSession(QrsClient client, String index) {
        this.client = client;
        this.index = index;
        this.startTime = System.currentTimeMillis();
        this.sessionId = UUIDs.randomBase64UUID();
    }

    public QrsClient getClient() {
        return client;
    }

    public String getIndex() {
        return index;
    }

    public SearchSourceBuilder getQuery() {
        return query;
    }

    public long getTook() {
        return System.currentTimeMillis() - startTime;
    }

    public SearchResponse execute(SearchSourceBuilder query) throws IOException {
        this.query = query;
        DSLExec exec = new DSLExec(query);
        SearchResponse searchResponse = exec.execute(this);
        logger.debug("DSLSession [{}] executed in [{}] ms", sessionId, System.currentTimeMillis() - startTime);
        return searchResponse;
    }
}
