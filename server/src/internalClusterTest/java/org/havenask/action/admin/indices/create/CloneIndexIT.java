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

package org.havenask.action.admin.indices.create;

import org.havenask.Version;
import org.havenask.action.admin.indices.settings.get.GetSettingsResponse;
import org.havenask.action.admin.indices.shrink.ResizeType;
import org.havenask.action.admin.indices.stats.IndicesStatsResponse;
import org.havenask.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.XContentType;
import org.havenask.index.query.TermsQueryBuilder;
import org.havenask.index.seqno.SeqNoStats;
import org.havenask.test.HavenaskIntegTestCase;
import org.havenask.test.VersionUtils;

import static org.havenask.test.hamcrest.HavenaskAssertions.assertAcked;
import static org.havenask.test.hamcrest.HavenaskAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class CloneIndexIT extends HavenaskIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testCreateCloneIndex() {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        int numPrimaryShards = randomIntBetween(1, 5);
        prepareCreate("source").setSettings(Settings.builder().put(indexSettings())
            .put("number_of_shards", numPrimaryShards)
            .put("index.version.created", version)
        ).get();
        final int docs = randomIntBetween(0, 128);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("source", "type")
                .setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        internalCluster().ensureAtLeastNumDataNodes(2);
        // ensure all shards are allocated otherwise the ensure green below might not succeed since we require the merge node
        // if we change the setting too quickly we will end up with one replica unassigned which can't be assigned anymore due
        // to the require._name below.
        ensureGreen();
        // relocate all shards to one node such that we can merge it.
        client().admin().indices().prepareUpdateSettings("source")
            .setSettings(Settings.builder()
                .put("index.blocks.write", true)).get();
        ensureGreen();

        final IndicesStatsResponse sourceStats = client().admin().indices().prepareStats("source").setSegments(true).get();

        // disable rebalancing to be able to capture the right stats. balancing can move the target primary
        // making it hard to pin point the source shards.
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(
            EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none"
        )).get();
        try {

            final boolean createWithReplicas = randomBoolean();
            assertAcked(client().admin().indices().prepareResizeIndex("source", "target")
                .setResizeType(ResizeType.CLONE)
                .setSettings(Settings.builder()
                    .put("index.number_of_replicas", createWithReplicas ? 1 : 0)
                    .putNull("index.blocks.write")
                    .build()).get());
            ensureGreen();

            final IndicesStatsResponse targetStats = client().admin().indices().prepareStats("target").get();
            assertThat(targetStats.getIndex("target").getIndexShards().keySet().size(), equalTo(numPrimaryShards));

            for (int i = 0; i < numPrimaryShards; i++) {
                final SeqNoStats sourceSeqNoStats = sourceStats.getIndex("source").getIndexShards().get(i).getAt(0).getSeqNoStats();
                final SeqNoStats targetSeqNoStats = targetStats.getIndex("target").getIndexShards().get(i).getAt(0).getSeqNoStats();
                assertEquals(sourceSeqNoStats.getMaxSeqNo(), targetSeqNoStats.getMaxSeqNo());
                assertEquals(targetSeqNoStats.getMaxSeqNo(), targetSeqNoStats.getLocalCheckpoint());
            }

            final int size = docs > 0 ? 2 * docs : 1;
            assertHitCount(client().prepareSearch("target").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), docs);

            if (createWithReplicas == false) {
                // bump replicas
                client().admin().indices().prepareUpdateSettings("target")
                    .setSettings(Settings.builder()
                        .put("index.number_of_replicas", 1)).get();
                ensureGreen();
                assertHitCount(client().prepareSearch("target").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), docs);
            }

            for (int i = docs; i < 2 * docs; i++) {
                client().prepareIndex("target", "type")
                    .setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
            }
            flushAndRefresh();
            assertHitCount(client().prepareSearch("target").setSize(2 * size).setQuery(new TermsQueryBuilder("foo", "bar")).get(),
                2 * docs);
            assertHitCount(client().prepareSearch("source").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), docs);
            GetSettingsResponse target = client().admin().indices().prepareGetSettings("target").get();
            assertEquals(version, target.getIndexToSettings().get("target").getAsVersion("index.version.created", null));
        } finally {
            // clean up
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(
                EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), (String)null
            )).get();
        }

    }

}
