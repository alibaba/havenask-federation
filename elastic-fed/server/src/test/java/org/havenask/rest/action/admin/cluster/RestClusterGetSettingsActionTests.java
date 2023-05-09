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

package org.havenask.rest.action.admin.cluster;

import org.havenask.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.common.settings.ClusterSettings;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.common.settings.SettingsFilter;
import org.havenask.test.HavenaskTestCase;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class RestClusterGetSettingsActionTests extends HavenaskTestCase {

    public void testFilterPersistentSettings() {
        runTestFilterSettingsTest(Metadata.Builder::persistentSettings, ClusterGetSettingsResponse::getPersistentSettings);
    }

    public void testFilterTransientSettings() {
        runTestFilterSettingsTest(Metadata.Builder::transientSettings, ClusterGetSettingsResponse::getTransientSettings);
    }

    private void runTestFilterSettingsTest(
            final BiConsumer<Metadata.Builder, Settings> md, final Function<ClusterGetSettingsResponse, Settings> s) {
        final Metadata.Builder mdBuilder = new Metadata.Builder();
        final Settings settings = Settings.builder().put("foo.filtered", "bar").put("foo.non_filtered", "baz").build();
        md.accept(mdBuilder, settings);
        final ClusterState.Builder builder = new ClusterState.Builder(ClusterState.EMPTY_STATE).metadata(mdBuilder);
        final SettingsFilter filter = new SettingsFilter(Collections.singleton("foo.filtered"));
        final Setting.Property[] properties = {Setting.Property.Dynamic, Setting.Property.Filtered, Setting.Property.NodeScope};
        final Set<Setting<?>> settingsSet = Stream.concat(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
                Stream.concat(
                        Stream.of(Setting.simpleString("foo.filtered", properties)),
                        Stream.of(Setting.simpleString("foo.non_filtered", properties))))
                .collect(Collectors.toSet());
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingsSet);
        final ClusterGetSettingsResponse response =
                RestClusterGetSettingsAction.response(builder.build(), randomBoolean(), filter, clusterSettings, Settings.EMPTY);
        assertFalse(s.apply(response).hasValue("foo.filtered"));
        assertTrue(s.apply(response).hasValue("foo.non_filtered"));
    }

}
