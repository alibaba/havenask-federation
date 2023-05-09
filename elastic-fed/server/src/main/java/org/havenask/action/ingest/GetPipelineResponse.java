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

package org.havenask.action.ingest;

import org.havenask.action.ActionResponse;
import org.havenask.common.Strings;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.StatusToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentParser.Token;
import org.havenask.ingest.PipelineConfiguration;
import org.havenask.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.havenask.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class GetPipelineResponse extends ActionResponse implements StatusToXContentObject {

    private List<PipelineConfiguration> pipelines;

    public GetPipelineResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        pipelines = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            pipelines.add(PipelineConfiguration.readFrom(in));
        }
    }

    public GetPipelineResponse(List<PipelineConfiguration> pipelines) {
        this.pipelines = pipelines;
    }

    /**
     * Get the list of pipelines that were a part of this response.
     * The pipeline id can be obtained using getId on the PipelineConfiguration object.
     * @return A list of {@link PipelineConfiguration} objects.
     */
    public List<PipelineConfiguration> pipelines() {
        return Collections.unmodifiableList(pipelines);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(pipelines.size());
        for (PipelineConfiguration pipeline : pipelines) {
            pipeline.writeTo(out);
        }
    }

    public boolean isFound() {
        return !pipelines.isEmpty();
    }

    @Override
    public RestStatus status() {
        return isFound() ? RestStatus.OK : RestStatus.NOT_FOUND;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (PipelineConfiguration pipeline : pipelines) {
            builder.field(pipeline.getId(), pipeline.getConfigAsMap());
        }
        builder.endObject();
        return builder;
    }

    /**
     *
     * @param parser the parser for the XContent that contains the serialized GetPipelineResponse.
     * @return an instance of GetPipelineResponse read from the parser
     * @throws IOException If the parsing fails
     */
    public static GetPipelineResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        List<PipelineConfiguration> pipelines = new ArrayList<>();
        while(parser.nextToken().equals(Token.FIELD_NAME)) {
            String pipelineId = parser.currentName();
            parser.nextToken();
            try (XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent())) {
                contentBuilder.generator().copyCurrentStructure(parser);
                PipelineConfiguration pipeline =
                    new PipelineConfiguration(pipelineId, BytesReference.bytes(contentBuilder), contentBuilder.contentType());
                pipelines.add(pipeline);
            }
        }
        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(), parser);
        return new GetPipelineResponse(pipelines);
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        } else if (other instanceof GetPipelineResponse){
            GetPipelineResponse otherResponse = (GetPipelineResponse)other;
            if (pipelines == null) {
                return otherResponse.pipelines == null;
            } else {
                // We need a map here because order does not matter for equality
                Map<String, PipelineConfiguration> otherPipelineMap = new HashMap<>();
                for (PipelineConfiguration pipeline: otherResponse.pipelines) {
                    otherPipelineMap.put(pipeline.getId(), pipeline);
                }
                for (PipelineConfiguration pipeline: pipelines) {
                    PipelineConfiguration otherPipeline = otherPipelineMap.get(pipeline.getId());
                    if (!pipeline.equals(otherPipeline)) {
                        return false;
                    }
                }
                return true;
            }
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (PipelineConfiguration pipeline: pipelines) {
            // We only take the sum here to ensure that the order does not matter.
            result += (pipeline == null ? 0 : pipeline.hashCode());
        }
        return result;
    }

}
