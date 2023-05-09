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

package org.havenask.action.index;

import org.havenask.action.DocWriteResponse;
import org.havenask.common.Strings;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.index.shard.ShardId;
import org.havenask.rest.RestStatus;

import java.io.IOException;

import static org.havenask.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A response of an index operation,
 *
 * @see IndexRequest
 * @see org.havenask.client.Client#index(IndexRequest)
 */
public class IndexResponse extends DocWriteResponse {

    public IndexResponse(ShardId shardId, StreamInput in) throws IOException {
        super(shardId, in);
    }

    public IndexResponse(StreamInput in) throws IOException {
        super(in);
    }

    public IndexResponse(ShardId shardId, String type, String id, long seqNo, long primaryTerm, long version, boolean created) {
        this(shardId, type, id, seqNo, primaryTerm, version, created ? Result.CREATED : Result.UPDATED);
    }

    private IndexResponse(ShardId shardId, String type, String id, long seqNo, long primaryTerm, long version, Result result) {
        super(shardId, type, id, seqNo, primaryTerm, version, assertCreatedOrUpdated(result));
    }

    private static Result assertCreatedOrUpdated(Result result) {
        assert result == Result.CREATED || result == Result.UPDATED;
        return result;
    }

    @Override
    public RestStatus status() {
        return result == Result.CREATED ? RestStatus.CREATED : super.status();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("IndexResponse[");
        builder.append("index=").append(getIndex());
        builder.append(",type=").append(getType());
        builder.append(",id=").append(getId());
        builder.append(",version=").append(getVersion());
        builder.append(",result=").append(getResult().getLowercase());
        builder.append(",seqNo=").append(getSeqNo());
        builder.append(",primaryTerm=").append(getPrimaryTerm());
        builder.append(",shards=").append(Strings.toString(getShardInfo()));
        return builder.append("]").toString();
    }

    public static IndexResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        Builder context = new Builder();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parseXContentFields(parser, context);
        }
        return context.build();
    }

    /**
     * Parse the current token and update the parsing context appropriately.
     */
    public static void parseXContentFields(XContentParser parser, Builder context) throws IOException {
        DocWriteResponse.parseInnerToXContent(parser, context);
    }

    /**
     * Builder class for {@link IndexResponse}. This builder is usually used during xcontent parsing to
     * temporarily store the parsed values, then the {@link Builder#build()} method is called to
     * instantiate the {@link IndexResponse}.
     */
    public static class Builder extends DocWriteResponse.Builder {
        @Override
        public IndexResponse build() {
            IndexResponse indexResponse = new IndexResponse(shardId, type, id, seqNo, primaryTerm, version, result);
            indexResponse.setForcedRefresh(forcedRefresh);
            if (shardInfo != null) {
                indexResponse.setShardInfo(shardInfo);
            }
            return indexResponse;
        }
    }
}
