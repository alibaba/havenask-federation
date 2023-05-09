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

package org.havenask.index.mapper;

import org.havenask.common.xcontent.XContentType;
import org.havenask.plugins.Plugin;
import org.havenask.test.HavenaskSingleNodeTestCase;

import java.util.Collection;

import static org.havenask.test.StreamsUtils.copyToBytesFromClasspath;

/**
 * Rudimentary tests that the templates used by Logstash and Beats
 * prior to their 5.x releases work for newly created indices
 */
public class BWCTemplateTests extends HavenaskSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperExtrasPlugin.class);
    }

    public void testBeatsTemplatesBWC() throws Exception {
        byte[] metricBeat = copyToBytesFromClasspath("/org/havenask/index/mapper/metricbeat-6.0.template.json");
        byte[] packetBeat = copyToBytesFromClasspath("/org/havenask/index/mapper/packetbeat-6.0.template.json");
        byte[] fileBeat = copyToBytesFromClasspath("/org/havenask/index/mapper/filebeat-6.0.template.json");
        client().admin().indices().preparePutTemplate("metricbeat").setSource(metricBeat, XContentType.JSON).get();
        client().admin().indices().preparePutTemplate("packetbeat").setSource(packetBeat, XContentType.JSON).get();
        client().admin().indices().preparePutTemplate("filebeat").setSource(fileBeat, XContentType.JSON).get();

        client().prepareIndex("metricbeat-foo", "doc", "1").setSource("message", "foo").get();
        client().prepareIndex("packetbeat-foo", "doc", "1").setSource("message", "foo").get();
        client().prepareIndex("filebeat-foo", "doc", "1").setSource("message", "foo").get();
    }
}
