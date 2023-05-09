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

package org.havenask.action.main;

import org.havenask.Build;
import org.havenask.LegacyESVersion;
import org.havenask.Version;
import org.havenask.action.ActionResponse;
import org.havenask.cluster.ClusterName;
import org.havenask.common.ParseField;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.xcontent.ObjectParser;
import org.havenask.common.xcontent.ToXContentObject;
import org.havenask.common.xcontent.XContentBuilder;
import org.havenask.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class MainResponse extends ActionResponse implements ToXContentObject {

    private String nodeName;
    private Version version;
    private ClusterName clusterName;
    private String clusterUuid;
    private Build build;
    private String versionNumber;
    public static final String TAGLINE = "The Havenask Federation Project";

    MainResponse() {}

    MainResponse(StreamInput in) throws IOException {
        super(in);
        nodeName = in.readString();
        version = Version.readVersion(in);
        clusterName = new ClusterName(in);
        clusterUuid = in.readString();
        build = Build.readBuild(in);
        if (in.getVersion().before(LegacyESVersion.V_7_0_0)) {
            in.readBoolean();
        }
        versionNumber = build.getQualifiedVersion();
    }

    public MainResponse(String nodeName, Version version, ClusterName clusterName, String clusterUuid, Build build) {
        this.nodeName = nodeName;
        this.version = version;
        this.clusterName = clusterName;
        this.clusterUuid = clusterUuid;
        this.build = build;
        this.versionNumber = build.getQualifiedVersion();
    }

    public MainResponse(String nodeName, Version version, ClusterName clusterName, String clusterUuid, Build build,
                        String versionNumber) {
        this.nodeName = nodeName;
        this.version = version;
        this.clusterName = clusterName;
        this.clusterUuid = clusterUuid;
        this.build = build;
        this.versionNumber = versionNumber;
    }

    public String getNodeName() {
        return nodeName;
    }

    public Version getVersion() {
        return version;
    }


    public ClusterName getClusterName() {
        return clusterName;
    }

    public String getClusterUuid() {
        return clusterUuid;
    }

    public Build getBuild() {
        return build;
    }

    public String getVersionNumber() {
        return versionNumber;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeName);
        if (out.getVersion().before(Version.V_1_0_0)) {
            Version.writeVersion(LegacyESVersion.V_7_10_2, out);
        } else {
            Version.writeVersion(version, out);
        }
        clusterName.writeTo(out);
        out.writeString(clusterUuid);
        Build.writeBuild(build, out);
        if (out.getVersion().before(LegacyESVersion.V_7_0_0)) {
            out.writeBoolean(true);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", nodeName);
        builder.field("cluster_name", clusterName.value());
        builder.field("cluster_uuid", clusterUuid);
        builder.startObject("version");
        if (isCompatibilityModeDisabled()) {
            builder.field("distribution", build.getDistribution());
        }
            builder.field("number", versionNumber)
            .field("build_type", build.type().displayName())
            .field("build_hash", build.hash())
            .field("build_date", build.date())
            .field("build_snapshot", build.isSnapshot())
            .field("lucene_version", version.luceneVersion.toString())
            .field("minimum_wire_compatibility_version", version.minimumCompatibilityVersion().toString())
            .field("minimum_index_compatibility_version", version.minimumIndexCompatibilityVersion().toString())
            .endObject();
        builder.field("tagline", TAGLINE);
        builder.endObject();
        return builder;
    }

    private boolean isCompatibilityModeDisabled() {
        // if we are not in compatibility mode (spoofing versionNumber), then
        // build.getQualifiedVersion is always used.
        return build.getQualifiedVersion().equals(versionNumber);
    }

    private static final ObjectParser<MainResponse, Void> PARSER = new ObjectParser<>(MainResponse.class.getName(), true,
            MainResponse::new);

    static {
        PARSER.declareString((response, value) -> response.nodeName = value, new ParseField("name"));
        PARSER.declareString((response, value) -> response.clusterName = new ClusterName(value), new ParseField("cluster_name"));
        PARSER.declareString((response, value) -> response.clusterUuid = value, new ParseField("cluster_uuid"));
        PARSER.declareString((response, value) -> {}, new ParseField("tagline"));
        PARSER.declareObject((response, value) -> {
            final String buildType = (String) value.get("build_type");
            response.build =
                    new Build(
                            /*
                             * Be lenient when reading on the wire, the enumeration values from other versions might be different than what
                             * we know.
                             */
                            buildType == null ? Build.Type.UNKNOWN : Build.Type.fromDisplayName(buildType, false),
                            (String) value.get("build_hash"),
                            (String) value.get("build_date"),
                            (boolean) value.get("build_snapshot"),
                            (String) value.get("number"),
                            (String) value.get("distribution")
                    );
            response.version = Version.fromString(
                ((String) value.get("number"))
                    .replace("-SNAPSHOT", "")
                    .replaceFirst("-(alpha\\d+|beta\\d+|rc\\d+)", "")
            );
            response.versionNumber = response.version.toString();
        }, (parser, context) -> parser.map(), new ParseField("version"));
    }

    public static MainResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MainResponse other = (MainResponse) o;
        return Objects.equals(nodeName, other.nodeName) &&
                Objects.equals(version, other.version) &&
                Objects.equals(clusterUuid, other.clusterUuid) &&
                Objects.equals(build, other.build) &&
                Objects.equals(clusterName, other.clusterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeName, version, clusterUuid, build, clusterName);
    }

    @Override
    public String toString() {
        return "MainResponse{" +
            "nodeName='" + nodeName + '\'' +
            ", version=" + version +
            ", clusterName=" + clusterName +
            ", clusterUuid='" + clusterUuid + '\'' +
            ", build=" + build +
            '}';
    }
}
