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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.havenask.action.search.SearchRequest;
import org.havenask.action.search.SearchResponse;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.UUIDs;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.engine.rpc.QrsClient;
import org.havenask.engine.search.dsl.expression.ExpressionContext;
import org.havenask.engine.search.dsl.plan.DSLExec;
import org.havenask.engine.search.internal.HavenaskScroll;
import org.havenask.search.builder.SearchSourceBuilder;

import java.util.Map;
import java.util.Objects;

public class DSLSession {
    protected Logger logger = LogManager.getLogger(DSLSession.class);

    private final QrsClient client;
    private final IndexMetadata indexMetadata;
    private final NamedXContentRegistry namedXContentRegistry;
    private long startTime;
    private final String sessionId;
    private SearchSourceBuilder query;
    private HavenaskScroll havenaskScroll;
    private final boolean sourceEnabled;

    public DSLSession(
        QrsClient client,
        IndexMetadata indexMetadata,
        SearchRequest searchRequest,
        String nodeId,
        NamedXContentRegistry namedXContentRegistry
    ) {
        this.client = client;
        this.indexMetadata = indexMetadata;
        this.startTime = System.currentTimeMillis();
        this.sessionId = UUIDs.randomBase64UUID();
        this.query = searchRequest.source();
        if (Objects.nonNull(searchRequest.scroll())) {
            this.havenaskScroll = new HavenaskScroll(nodeId, searchRequest.scroll());
        }
        this.namedXContentRegistry = namedXContentRegistry;
        this.sourceEnabled = setSourceEnabled();
    }

    private boolean setSourceEnabled() {
        Map<String, Object> indexMapping = indexMetadata.mapping() != null ? indexMetadata.mapping().getSourceAsMap() : null;
        Boolean sourceEnabled = true;
        if (indexMapping != null && indexMapping.containsKey("_source")) {
            Object sourceValue = indexMapping.get("_source");
            if (sourceValue instanceof Map) {
                @SuppressWarnings("unchecked")
                Object sourceEnabledValue = ((Map<String, Object>) sourceValue).get("enabled");
                if (sourceEnabledValue instanceof Boolean) {
                    sourceEnabled = (Boolean) sourceEnabledValue;
                }
            }
        }
        return sourceEnabled;
    }

    public boolean isSourceEnabled() {
        return sourceEnabled;
    }

    public QrsClient getClient() {
        return client;
    }

    public String getIndex() {
        return indexMetadata.getIndex().getName();
    }

    public IndexMetadata getIndexMetadata() {
        return indexMetadata;
    }

    public SearchSourceBuilder getQuery() {
        return query;
    }

    public NamedXContentRegistry getNamedXContentRegistry() {
        return namedXContentRegistry;
    }

    public long getTook() {
        return System.currentTimeMillis() - startTime;
    }

    public String getSessionId() {
        return sessionId;
    }

    public SearchResponse execute() throws Exception {
        try {
            startTime = System.currentTimeMillis();
            ExpressionContext context = new ExpressionContext(namedXContentRegistry, havenaskScroll, indexMetadata.getNumberOfShards());
            DSLExec exec = new DSLExec(query, context);
            SearchResponse searchResponse = exec.execute(this);
            logger.debug("DSLSession [{}] executed in [{}] ms", sessionId, System.currentTimeMillis() - startTime);
            return searchResponse;
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(() -> new ParameterizedMessage("DSLSession [{}] executed fail, dsl:{}", sessionId, query.toString()), e);
            }
            throw e;
        }
    }
}
