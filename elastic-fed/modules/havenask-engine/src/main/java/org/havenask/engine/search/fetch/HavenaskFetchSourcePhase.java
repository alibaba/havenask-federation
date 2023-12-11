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

package org.havenask.engine.search.fetch;

import org.havenask.HavenaskException;
import org.havenask.HavenaskParseException;
import org.havenask.common.bytes.BytesArray;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.collect.Tuple;
import org.havenask.common.io.stream.BytesStreamOutput;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentHelper;
import org.havenask.common.xcontent.XContentType;
import org.havenask.search.builder.SearchSourceBuilder;
import org.havenask.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Map;

public class HavenaskFetchSourcePhase implements HavenaskFetchSubPhase {
    @Override
    public HavenaskFetchSubPhaseProcessor getProcessor(String indexName, SearchSourceBuilder searchSourceBuilder) throws IOException {
        FetchSourceContext fetchSourceContext = searchSourceBuilder.fetchSource();
        if (fetchSourceContext == null || fetchSourceContext.fetchSource() == false) {
            return null;
        }
        assert fetchSourceContext.fetchSource();
        return new HavenaskFetchSubPhaseProcessor() {
            @Override
            public void process(HitContent hitContent) {
                hitExecute(indexName, fetchSourceContext, hitContent);
            }
        };
    }

    private void hitExecute(String indexName, FetchSourceContext fetchSourceContext, HitContent hitContent) {
        BytesReference sourceAsBytes = new BytesArray((String) hitContent.source);
        SourceContent sourceContent = loadSource(sourceAsBytes);

        // If source is disabled in the mapping, then attempt to return early.
        if (sourceContent.getSourceAsMap() == null && sourceContent.getSourceAsBytes() == null) {
            if (containsFilters(fetchSourceContext)) {
                throw new IllegalArgumentException(
                    "unable to fetch fields from _source field: _source is disabled in the mappings " + "for index [" + indexName + "]"
                );
            }
            return;
        }

        // filter the source and add it to the hit.
        Object value = fetchSourceContext.getFilter().apply(sourceContent.getSourceAsMap());
        try {
            final int initialCapacity = Math.min(1024, sourceAsBytes.length());
            BytesStreamOutput streamOutput = new BytesStreamOutput(initialCapacity);
            XContentBuilder builder = new XContentBuilder(sourceContent.getSourceContentType().xContent(), streamOutput);
            if (value != null) {
                builder.value(value);
            } else {
                // This happens if the source filtering could not find the specified in the _source.
                // Just doing `builder.value(null)` is valid, but the xcontent validation can't detect what format
                // it is. In certain cases, for example response serialization we fail if no xcontent type can't be
                // detected. So instead we just return an empty top level object. Also this is in inline with what was
                // being return in this situation in 5.x and earlier.
                builder.startObject();
                builder.endObject();
            }
            hitContent.hit.sourceRef(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new HavenaskException("Error filtering source", e);
        }
    }

    private SourceContent loadSource(BytesReference sourceAsBytes) {
        Tuple<XContentType, Map<String, Object>> tuple = sourceAsMapAndType(sourceAsBytes);
        XContentType sourceContentType = tuple.v1();
        Map<String, Object> source = tuple.v2();
        return new SourceContent(sourceAsBytes, source, sourceContentType);
    }

    public class SourceContent {
        private BytesReference sourceAsBytes;
        private Map<String, Object> sourceAsMap;
        private XContentType sourceContentType;

        public SourceContent(BytesReference sourceAsBytes, Map<String, Object> source, XContentType sourceContentType) {
            this.sourceAsBytes = sourceAsBytes;
            this.sourceAsMap = source;
            this.sourceContentType = sourceContentType;
        }

        public BytesReference getSourceAsBytes() {
            return sourceAsBytes;
        }

        public Map<String, Object> getSourceAsMap() {
            return sourceAsMap;
        }

        public XContentType getSourceContentType() {
            return sourceContentType;
        }
    }

    private static boolean containsFilters(FetchSourceContext context) {
        return context.includes().length != 0 || context.excludes().length != 0;
    }

    private static Tuple<XContentType, Map<String, Object>> sourceAsMapAndType(BytesReference source) throws HavenaskParseException {
        return XContentHelper.convertToMap(source, false);
    }
}
