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

package org.havenask.cluster.metadata;

import org.havenask.cluster.AbstractDiffable;
import org.havenask.cluster.Diff;
import org.havenask.common.Nullable;
import org.havenask.common.ParseField;
import org.havenask.common.Strings;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.ConstructingObjectParser;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * A component template is a re-usable {@link Template} as well as metadata about the template. Each
 * component template is expected to be valid on its own. For example, if a component template
 * contains a field "foo", it's expected to contain all the necessary settings/mappings/etc for the
 * "foo" field. These component templates make up the individual pieces composing an index template.
 */
public class ComponentTemplate extends AbstractDiffable<ComponentTemplate> implements ToXContentObject {
    private static final ParseField TEMPLATE = new ParseField("template");
    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField METADATA = new ParseField("_meta");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ComponentTemplate, Void> PARSER =
        new ConstructingObjectParser<>("component_template", false,
            a -> new ComponentTemplate((Template) a[0], (Long) a[1], (Map<String, Object>) a[2]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), Template.PARSER, TEMPLATE);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), VERSION);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), METADATA);
    }

    private final Template template;
    @Nullable
    private final Long version;
    @Nullable
    private final Map<String, Object> metadata;

    static Diff<ComponentTemplate> readComponentTemplateDiffFrom(StreamInput in) throws IOException {
        return AbstractDiffable.readDiffFrom(ComponentTemplate::new, in);
    }

    public static ComponentTemplate parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ComponentTemplate(Template template, @Nullable Long version, @Nullable Map<String, Object> metadata) {
        this.template = template;
        this.version = version;
        this.metadata = metadata;
    }

    public ComponentTemplate(StreamInput in) throws IOException {
        this.template = new Template(in);
        this.version = in.readOptionalVLong();
        if (in.readBoolean()) {
            this.metadata = in.readMap();
        } else {
            this.metadata = null;
        }
    }

    public Template template() {
        return template;
    }

    @Nullable
    public Long version() {
        return version;
    }

    @Nullable
    public Map<String, Object> metadata() {
        return metadata;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.template.writeTo(out);
        out.writeOptionalVLong(this.version);
        if (this.metadata == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(this.metadata);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(template, version, metadata);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ComponentTemplate other = (ComponentTemplate) obj;
        return Objects.equals(template, other.template) &&
            Objects.equals(version, other.version) &&
            Objects.equals(metadata, other.metadata);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TEMPLATE.getPreferredName(), this.template);
        if (this.version != null) {
            builder.field(VERSION.getPreferredName(), this.version);
        }
        if (this.metadata != null) {
            builder.field(METADATA.getPreferredName(), this.metadata);
        }
        builder.endObject();
        return builder;
    }
}
