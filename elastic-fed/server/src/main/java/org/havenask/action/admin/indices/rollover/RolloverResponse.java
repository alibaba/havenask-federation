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

package org.havenask.action.admin.indices.rollover;

import org.havenask.LegacyESVersion;
import org.havenask.action.support.master.ShardsAcknowledgedResponse;
import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.ConstructingObjectParser;
import org.havenask.common.xcontent.ObjectParser;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.havenask.common.xcontent.ConstructingObjectParser.constructorArg;


/**
 * Response object for {@link RolloverRequest} API
 *
 * Note: there is a new class with the same name for the Java HLRC that uses a typeless format.
 * Any changes done to this class should also go to that client class.
 */
public final class RolloverResponse extends ShardsAcknowledgedResponse implements ToXContentObject {

    private static final ParseField NEW_INDEX = new ParseField("new_index");
    private static final ParseField OLD_INDEX = new ParseField("old_index");
    private static final ParseField DRY_RUN = new ParseField("dry_run");
    private static final ParseField ROLLED_OVER = new ParseField("rolled_over");
    private static final ParseField CONDITIONS = new ParseField("conditions");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RolloverResponse, Void> PARSER = new ConstructingObjectParser<>("rollover",
            true, args -> new RolloverResponse((String) args[0], (String) args[1], (Map<String,Boolean>) args[2],
            (Boolean)args[3], (Boolean)args[4], (Boolean) args[5], (Boolean) args[6]));

    static {
        PARSER.declareField(constructorArg(), (parser, context) -> parser.text(), OLD_INDEX, ObjectParser.ValueType.STRING);
        PARSER.declareField(constructorArg(), (parser, context) -> parser.text(), NEW_INDEX, ObjectParser.ValueType.STRING);
        PARSER.declareObject(constructorArg(), (parser, context) -> parser.map(), CONDITIONS);
        PARSER.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), DRY_RUN, ObjectParser.ValueType.BOOLEAN);
        PARSER.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), ROLLED_OVER, ObjectParser.ValueType.BOOLEAN);
        declareAcknowledgedAndShardsAcknowledgedFields(PARSER);
    }

    private final String oldIndex;
    private final String newIndex;
    private final Map<String, Boolean> conditionStatus;
    private final boolean dryRun;
    private final boolean rolledOver;
    // Needs to be duplicated, because shardsAcknowledged gets (de)serailized as last field whereas
    // in other subclasses of ShardsAcknowledgedResponse this field (de)serailized as first field.
    private final boolean shardsAcknowledged;

    RolloverResponse(StreamInput in) throws IOException {
        super(in, false, in.getVersion().onOrAfter(LegacyESVersion.V_6_4_0));
        if (in.getVersion().onOrAfter(LegacyESVersion.V_6_4_0)) {
            oldIndex = in.readString();
            newIndex = in.readString();
            int conditionSize = in.readVInt();
            conditionStatus = new HashMap<>(conditionSize);
            for (int i = 0; i < conditionSize; i++) {
                conditionStatus.put(in.readString(), in.readBoolean());
            }
            dryRun = in.readBoolean();
            rolledOver = in.readBoolean();
            shardsAcknowledged = in.readBoolean();
        } else {
            oldIndex = in.readString();
            newIndex = in.readString();
            int conditionSize = in.readVInt();
            conditionStatus = new HashMap<>(conditionSize);
            for (int i = 0; i < conditionSize; i++) {
                conditionStatus.put(in.readString(), in.readBoolean());
            }
            dryRun = in.readBoolean();
            rolledOver = in.readBoolean();
            acknowledged = in.readBoolean();
            shardsAcknowledged = in.readBoolean();
        }
    }

    public RolloverResponse(String oldIndex, String newIndex, Map<String, Boolean> conditionResults,
                            boolean dryRun, boolean rolledOver, boolean acknowledged, boolean shardsAcknowledged) {
        super(acknowledged, shardsAcknowledged);
        this.oldIndex = oldIndex;
        this.newIndex = newIndex;
        this.dryRun = dryRun;
        this.rolledOver = rolledOver;
        this.conditionStatus = conditionResults;
        this.shardsAcknowledged = shardsAcknowledged;
    }

    /**
     * Returns the name of the index that the request alias was pointing to
     */
    public String getOldIndex() {
        return oldIndex;
    }

    /**
     * Returns the name of the index that the request alias currently points to
     */
    public String getNewIndex() {
        return newIndex;
    }

    /**
     * Returns the statuses of all the request conditions
     */
    public Map<String, Boolean> getConditionStatus() {
        return conditionStatus;
    }

    /**
     * Returns if the rollover execution was skipped even when conditions were met
     */
    public boolean isDryRun() {
        return dryRun;
    }

    /**
     * Returns true if the rollover was not simulated and the conditions were met
     */
    public boolean isRolledOver() {
        return rolledOver;
    }

    @Override
    public boolean isShardsAcknowledged() {
        return shardsAcknowledged;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(LegacyESVersion.V_6_4_0)) {
            super.writeTo(out);
            out.writeString(oldIndex);
            out.writeString(newIndex);
            out.writeVInt(conditionStatus.size());
            for (Map.Entry<String, Boolean> entry : conditionStatus.entrySet()) {
                out.writeString(entry.getKey());
                out.writeBoolean(entry.getValue());
            }
            out.writeBoolean(dryRun);
            out.writeBoolean(rolledOver);
            out.writeBoolean(shardsAcknowledged);
        } else {
            out.writeString(oldIndex);
            out.writeString(newIndex);
            out.writeVInt(conditionStatus.size());
            for (Map.Entry<String, Boolean> entry : conditionStatus.entrySet()) {
                out.writeString(entry.getKey());
                out.writeBoolean(entry.getValue());
            }
            out.writeBoolean(dryRun);
            out.writeBoolean(rolledOver);
            out.writeBoolean(acknowledged);
            writeShardsAcknowledged(out);
        }
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        super.addCustomFields(builder, params);
        builder.field(OLD_INDEX.getPreferredName(), oldIndex);
        builder.field(NEW_INDEX.getPreferredName(), newIndex);
        builder.field(ROLLED_OVER.getPreferredName(), rolledOver);
        builder.field(DRY_RUN.getPreferredName(), dryRun);
        builder.startObject(CONDITIONS.getPreferredName());
        for (Map.Entry<String, Boolean> entry : conditionStatus.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
    }

    public static RolloverResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            RolloverResponse that = (RolloverResponse) o;
            return dryRun == that.dryRun &&
                    rolledOver == that.rolledOver &&
                    Objects.equals(oldIndex, that.oldIndex) &&
                    Objects.equals(newIndex, that.newIndex) &&
                    Objects.equals(conditionStatus, that.conditionStatus);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), oldIndex, newIndex, conditionStatus, dryRun, rolledOver);
    }
}
