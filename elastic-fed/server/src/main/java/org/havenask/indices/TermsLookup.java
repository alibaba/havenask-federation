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

package org.havenask.indices;

import org.havenask.LegacyESVersion;
import org.havenask.common.Nullable;
import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.io.stream.Writeable;
import org.havenask.common.xcontent.ConstructingObjectParser;
import org.havenask.common.xcontent.ToXContentFragment;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.index.query.TermsQueryBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.havenask.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.havenask.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Encapsulates the parameters needed to fetch terms.
 */
public class TermsLookup implements Writeable, ToXContentFragment {

    private final String index;
    private @Nullable String type;
    private final String id;
    private final String path;
    private String routing;


    public TermsLookup(String index, String id, String path) {
        this(index, null, id, path);
    }

    /**
     * @deprecated Types are in the process of being removed, use {@link TermsLookup(String, String, String)} instead.
     */
    @Deprecated
    public TermsLookup(String index, String type, String id, String path) {
        if (id == null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying the id.");
        }
        if (path == null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying the path.");
        }
        if (index == null) {
            throw new IllegalArgumentException("[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying the index.");
        }
        this.index = index;
        this.type = type;
        this.id = id;
        this.path = path;
    }

    /**
     * Read from a stream.
     */
    public TermsLookup(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_0_0)) {
            type = in.readOptionalString();
        } else {
            // Before 7.0, the type parameter was always non-null and serialized as a (non-optional) string.
            type = in.readString();
        }
        id = in.readString();
        path = in.readString();
        if (in.getVersion().onOrAfter(LegacyESVersion.V_6_0_0_beta1)) {
            index = in.readString();
        } else {
            index = in.readOptionalString();
            if (index == null) {
                throw new IllegalStateException("index must not be null in a terms lookup");
            }
        }
        routing = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_0_0)) {
            out.writeOptionalString(type);
        } else {
            if (type == null) {
                throw new IllegalArgumentException("Typeless [terms] lookup queries are not supported if any " +
                    "node is running a version before 7.0.");

            }
            out.writeString(type);
        }
        out.writeString(id);
        out.writeString(path);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_6_0_0_beta1)) {
            out.writeString(index);
        } else {
            out.writeOptionalString(index);
        }
        out.writeOptionalString(routing);
    }

    public String index() {
        return index;
    }

    /**
     * @deprecated Types are in the process of being removed.
     */
    @Deprecated
    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    public String path() {
        return path;
    }

    public String routing() {
        return routing;
    }

    public TermsLookup routing(String routing) {
        this.routing = routing;
        return this;
    }

    private static final ConstructingObjectParser<TermsLookup, Void> PARSER = new ConstructingObjectParser<>("terms_lookup",
        args -> {
            String index = (String) args[0];
            String type = (String) args[1];
            String id = (String) args[2];
            String path = (String) args[3];
            return new TermsLookup(index, type, id, path);
        });
    static {
        PARSER.declareString(constructorArg(), new ParseField("index"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("type").withAllDeprecated());
        PARSER.declareString(constructorArg(), new ParseField("id"));
        PARSER.declareString(constructorArg(), new ParseField("path"));
        PARSER.declareString(TermsLookup::routing, new ParseField("routing"));
    }

    public static TermsLookup parseTermsLookup(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public String toString() {
        if (type == null) {
            return index + "/" + id + "/" + path;
        } else {
            return index + "/" + type + "/" + id + "/" + path;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("index", index);
        if (type != null) {
            builder.field("type", type);
        }
        builder.field("id", id);
        builder.field("path", path);
        if (routing != null) {
            builder.field("routing", routing);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, type, id, path, routing);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TermsLookup other = (TermsLookup) obj;
        return Objects.equals(index, other.index) &&
                Objects.equals(type, other.type) &&
                Objects.equals(id, other.id) &&
                Objects.equals(path, other.path) &&
                Objects.equals(routing, other.routing);
    }
}
