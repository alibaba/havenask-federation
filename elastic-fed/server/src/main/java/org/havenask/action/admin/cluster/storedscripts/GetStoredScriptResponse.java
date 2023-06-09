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

package org.havenask.action.admin.cluster.storedscripts;

import org.havenask.LegacyESVersion;
import org.havenask.action.ActionResponse;
import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.ConstructingObjectParser;
import org.havenask.common.xcontent.ObjectParser;
import org.havenask.common.xcontent.StatusToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.rest.RestStatus;
import org.havenask.script.StoredScriptSource;

import java.io.IOException;
import java.util.Objects;

import static org.havenask.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.havenask.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class GetStoredScriptResponse extends ActionResponse implements StatusToXContentObject {

    public static final ParseField _ID_PARSE_FIELD = new ParseField("_id");
    public static final ParseField FOUND_PARSE_FIELD = new ParseField("found");
    public static final ParseField SCRIPT = new ParseField("script");

    private static final ConstructingObjectParser<GetStoredScriptResponse, String> PARSER =
        new ConstructingObjectParser<>("GetStoredScriptResponse",
            true,
            (a, c) -> {
                String id = (String) a[0];
                boolean found = (Boolean)a[1];
                StoredScriptSource scriptSource = (StoredScriptSource)a[2];
                return found ? new GetStoredScriptResponse(id, scriptSource) : new GetStoredScriptResponse(id, null);
            });

    static {
        PARSER.declareField(constructorArg(), (p, c) -> p.text(),
            _ID_PARSE_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(constructorArg(), (p, c) -> p.booleanValue(),
            FOUND_PARSE_FIELD, ObjectParser.ValueType.BOOLEAN);
        PARSER.declareField(optionalConstructorArg(), (p, c) -> StoredScriptSource.fromXContent(p, true),
            SCRIPT, ObjectParser.ValueType.OBJECT);
    }

    private String id;
    private StoredScriptSource source;

    public GetStoredScriptResponse(StreamInput in) throws IOException {
        super(in);

        if (in.readBoolean()) {
            source = new StoredScriptSource(in);
        } else {
            source = null;
        }

        if (in.getVersion().onOrAfter(LegacyESVersion.V_6_4_0)) {
            id = in.readString();
        }
    }

    GetStoredScriptResponse(String id, StoredScriptSource source) {
        this.id = id;
        this.source = source;
    }

    public String getId() {
        return id;
    }

    /**
     * @return if a stored script and if not found <code>null</code>
     */
    public StoredScriptSource getSource() {
        return source;
    }

    @Override
    public RestStatus status() {
        return source != null ? RestStatus.OK : RestStatus.NOT_FOUND;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(_ID_PARSE_FIELD.getPreferredName(), id);
        builder.field(FOUND_PARSE_FIELD.getPreferredName(), source != null);
        if (source != null) {
            builder.field(StoredScriptSource.SCRIPT_PARSE_FIELD.getPreferredName());
            source.toXContent(builder, params);
        }

        builder.endObject();
        return builder;
    }

    public static GetStoredScriptResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (source == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            source.writeTo(out);
        }
        if (out.getVersion().onOrAfter(LegacyESVersion.V_6_4_0)) {
            out.writeString(id);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetStoredScriptResponse that = (GetStoredScriptResponse) o;
        return Objects.equals(id, that.id) &&
            Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, source);
    }
}
