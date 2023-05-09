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

package org.havenask.action.admin.cluster.settings;

import org.havenask.LegacyESVersion;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.ConstructingObjectParser;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.havenask.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A response for a cluster update settings action.
 */
public class ClusterUpdateSettingsResponse extends AcknowledgedResponse {

    private static final ParseField PERSISTENT = new ParseField("persistent");
    private static final ParseField TRANSIENT = new ParseField("transient");

    private static final ConstructingObjectParser<ClusterUpdateSettingsResponse, Void> PARSER = new ConstructingObjectParser<>(
            "cluster_update_settings_response", true, args -> {
                return new ClusterUpdateSettingsResponse((boolean) args[0], (Settings) args[1], (Settings) args[2]);
            });
    static {
        declareAcknowledgedField(PARSER);
        PARSER.declareObject(constructorArg(), (p, c) -> Settings.fromXContent(p), TRANSIENT);
        PARSER.declareObject(constructorArg(), (p, c) -> Settings.fromXContent(p), PERSISTENT);
    }

    final Settings transientSettings;
    final Settings persistentSettings;

    ClusterUpdateSettingsResponse(StreamInput in) throws IOException {
        super(in, in.getVersion().onOrAfter(LegacyESVersion.V_6_4_0));
        if (in.getVersion().onOrAfter(LegacyESVersion.V_6_4_0)) {
            transientSettings = Settings.readSettingsFromStream(in);
            persistentSettings = Settings.readSettingsFromStream(in);
        } else {
            transientSettings = Settings.readSettingsFromStream(in);
            persistentSettings = Settings.readSettingsFromStream(in);
            acknowledged = in.readBoolean();
        }
    }

    ClusterUpdateSettingsResponse(boolean acknowledged, Settings transientSettings, Settings persistentSettings) {
        super(acknowledged);
        this.persistentSettings = persistentSettings;
        this.transientSettings = transientSettings;
    }

    public Settings getTransientSettings() {
        return transientSettings;
    }

    public Settings getPersistentSettings() {
        return persistentSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(LegacyESVersion.V_6_4_0)) {
            super.writeTo(out);
            Settings.writeSettingsToStream(transientSettings, out);
            Settings.writeSettingsToStream(persistentSettings, out);
        } else {
            Settings.writeSettingsToStream(transientSettings, out);
            Settings.writeSettingsToStream(persistentSettings, out);
            out.writeBoolean(acknowledged);
        }
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PERSISTENT.getPreferredName());
        persistentSettings.toXContent(builder, params);
        builder.endObject();
        builder.startObject(TRANSIENT.getPreferredName());
        transientSettings.toXContent(builder, params);
        builder.endObject();
    }

    public static ClusterUpdateSettingsResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            ClusterUpdateSettingsResponse that = (ClusterUpdateSettingsResponse) o;
            return Objects.equals(transientSettings, that.transientSettings) &&
                    Objects.equals(persistentSettings, that.persistentSettings);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), transientSettings, persistentSettings);
    }
}
