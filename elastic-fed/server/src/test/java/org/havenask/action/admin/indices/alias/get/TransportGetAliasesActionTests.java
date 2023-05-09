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

package org.havenask.action.admin.indices.alias.get;

import org.havenask.Version;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.AliasMetadata;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.common.collect.ImmutableOpenMap;
import org.havenask.indices.SystemIndexDescriptor;
import org.havenask.indices.SystemIndices;
import org.havenask.test.HavenaskTestCase;
import org.havenask.action.admin.indices.alias.get.GetAliasesRequest;
import org.havenask.action.admin.indices.alias.get.TransportGetAliasesAction;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TransportGetAliasesActionTests extends HavenaskTestCase {
    private final SystemIndices EMPTY_SYSTEM_INDICES = new SystemIndices(Collections.emptyMap());

    public void testPostProcess() {
        GetAliasesRequest request = new GetAliasesRequest();
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut("b", Collections.singletonList(new AliasMetadata.Builder("y").build()))
            .build();
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, new String[]{"a", "b", "c"}, aliases, ClusterState.EMPTY_STATE, false,
                EMPTY_SYSTEM_INDICES);
        assertThat(result.size(), equalTo(3));
        assertThat(result.get("a").size(), equalTo(0));
        assertThat(result.get("b").size(), equalTo(1));
        assertThat(result.get("c").size(), equalTo(0));

        request = new GetAliasesRequest();
        request.replaceAliases("y", "z");
        aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut("b", Collections.singletonList(new AliasMetadata.Builder("y").build()))
            .build();
        result = TransportGetAliasesAction.postProcess(request, new String[]{"a", "b", "c"}, aliases, ClusterState.EMPTY_STATE, false,
            EMPTY_SYSTEM_INDICES);
        assertThat(result.size(), equalTo(3));
        assertThat(result.get("a").size(), equalTo(0));
        assertThat(result.get("b").size(), equalTo(1));
        assertThat(result.get("c").size(), equalTo(0));

        request = new GetAliasesRequest("y", "z");
        aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut("b", Collections.singletonList(new AliasMetadata.Builder("y").build()))
            .build();
        result = TransportGetAliasesAction.postProcess(request, new String[]{"a", "b", "c"}, aliases, ClusterState.EMPTY_STATE, false,
            EMPTY_SYSTEM_INDICES);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("b").size(), equalTo(1));
    }

    public void testDeprecationWarningEmittedForTotalWildcard() {
        ClusterState state = systemIndexTestClusterState();

        GetAliasesRequest request = new GetAliasesRequest();
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut(".b", Collections.singletonList(new AliasMetadata.Builder(".y").build()))
            .fPut("c", Collections.singletonList(new AliasMetadata.Builder("d").build()))
            .build();
        final String[] concreteIndices = {"a", ".b", "c"};
        assertEquals(state.metadata().findAliases(request, concreteIndices), aliases);
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, concreteIndices, aliases, state, false, EMPTY_SYSTEM_INDICES);
        assertThat(result.size(), equalTo(3));
        assertThat(result.get("a").size(), equalTo(0));
        assertThat(result.get(".b").size(), equalTo(1));
        assertThat(result.get("c").size(), equalTo(1));
        assertWarnings("this request accesses system indices: [.b], but in a future major version, direct access to system " +
            "indices will be prevented by default");
    }

    public void testDeprecationWarningEmittedWhenSystemIndexIsRequested() {
        ClusterState state = systemIndexTestClusterState();

        GetAliasesRequest request = new GetAliasesRequest();
        request.indices(".b");
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut(".b", Collections.singletonList(new AliasMetadata.Builder(".y").build()))
            .build();
        final String[] concreteIndices = {".b"};
        assertEquals(state.metadata().findAliases(request, concreteIndices), aliases);
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, concreteIndices, aliases, state, false, EMPTY_SYSTEM_INDICES);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(".b").size(), equalTo(1));
        assertWarnings("this request accesses system indices: [.b], but in a future major version, direct access to system " +
            "indices will be prevented by default");
    }

    public void testDeprecationWarningEmittedWhenSystemIndexIsRequestedByAlias() {
        ClusterState state = systemIndexTestClusterState();

        GetAliasesRequest request = new GetAliasesRequest(".y");
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut(".b", Collections.singletonList(new AliasMetadata.Builder(".y").build()))
            .build();
        final String[] concreteIndices = {"a", ".b", "c"};
        assertEquals(state.metadata().findAliases(request, concreteIndices), aliases);
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, concreteIndices, aliases, state, false, EMPTY_SYSTEM_INDICES);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(".b").size(), equalTo(1));
        assertWarnings("this request accesses system indices: [.b], but in a future major version, direct access to system " +
            "indices will be prevented by default");
    }

    public void testDeprecationWarningNotEmittedWhenSystemAccessAllowed() {
        ClusterState state = systemIndexTestClusterState();

        GetAliasesRequest request = new GetAliasesRequest(".y");
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut(".b", Collections.singletonList(new AliasMetadata.Builder(".y").build()))
            .build();
        final String[] concreteIndices = {"a", ".b", "c"};
        assertEquals(state.metadata().findAliases(request, concreteIndices), aliases);
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, concreteIndices, aliases, state, true, EMPTY_SYSTEM_INDICES);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(".b").size(), equalTo(1));
    }

    /**
     * Ensures that deprecation warnings are not emitted when
     */
    public void testDeprecationWarningNotEmittedWhenOnlyNonsystemIndexRequested() {
        ClusterState state = systemIndexTestClusterState();

        GetAliasesRequest request = new GetAliasesRequest();
        request.indices("c");
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .fPut("c", Collections.singletonList(new AliasMetadata.Builder("d").build()))
            .build();
        final String[] concreteIndices = {"c"};
        assertEquals(state.metadata().findAliases(request, concreteIndices), aliases);
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, concreteIndices, aliases, state, false, EMPTY_SYSTEM_INDICES);
        assertThat(result.size(), equalTo(1));
        assertThat(result.get("c").size(), equalTo(1));
    }

    public void testDeprecationWarningEmittedWhenRequestingNonExistingAliasInSystemPattern() {
        ClusterState state = systemIndexTestClusterState();
        SystemIndices systemIndices = new SystemIndices(Collections.singletonMap(this.getTestName(),
            Collections.singletonList(new SystemIndexDescriptor(".y", "an index that doesn't exist"))));

        GetAliasesRequest request = new GetAliasesRequest(".y");
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.<String, List<AliasMetadata>>builder()
            .build();
        final String[] concreteIndices = {};
        assertEquals(state.metadata().findAliases(request, concreteIndices), aliases);
        ImmutableOpenMap<String, List<AliasMetadata>> result =
            TransportGetAliasesAction.postProcess(request, concreteIndices, aliases, state, false, systemIndices);
        assertThat(result.size(), equalTo(0));
        assertWarnings("this request accesses aliases with names reserved for system indices: [.y], but in a future major version, direct" +
            "access to system indices and their aliases will not be allowed");
    }

    public ClusterState systemIndexTestClusterState() {
        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder("a").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .put(IndexMetadata.builder(".b").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0)
                    .system(true).putAlias(AliasMetadata.builder(".y")))
                .put(IndexMetadata.builder("c").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0)
                    .putAlias(AliasMetadata.builder("d")))
                .build())
            .build();
    }


}
