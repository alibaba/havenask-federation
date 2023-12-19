/*
 * Copyright (c) 2021, Alibaba Group;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.havenask.cluster.routing;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.Nullable;
import org.havenask.common.bytes.BytesReference;
import org.havenask.common.xcontent.XContentType;
import org.havenask.common.xcontent.json.JsonXContent;
import org.havenask.index.shard.ShardId;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.VersionUtils;

public class IndexRoutingTests extends HavenaskTestCase {
    public void testGenerateShardId() {
        int[][] possibleValues = new int[][] { { 8, 4, 2 }, { 20, 10, 2 }, { 36, 12, 3 }, { 15, 5, 1 } };
        for (int i = 0; i < 10; i++) {
            int[] shardSplits = randomFrom(possibleValues);
            assertEquals(shardSplits[0], (shardSplits[0] / shardSplits[1]) * shardSplits[1]);
            assertEquals(shardSplits[1], (shardSplits[1] / shardSplits[2]) * shardSplits[2]);
            IndexMetadata metadata = IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(shardSplits[0])
                .numberOfReplicas(1)
                .build();
            String term = randomAlphaOfLength(10);
            final int shard = shardIdFromSimple(IndexRouting.fromIndexMetadata(metadata), term, null);
            IndexMetadata shrunk = IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(shardSplits[1])
                .numberOfReplicas(1)
                .setRoutingNumShards(shardSplits[0])
                .build();
            int shrunkShard = shardIdFromSimple(IndexRouting.fromIndexMetadata(shrunk), term, null);

            Set<ShardId> shardIds = IndexMetadata.selectShrinkShards(shrunkShard, metadata, shrunk.getNumberOfShards());
            assertEquals(1, shardIds.stream().filter((sid) -> sid.id() == shard).count());

            shrunk = IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(shardSplits[2])
                .numberOfReplicas(1)
                .setRoutingNumShards(shardSplits[0])
                .build();
            shrunkShard = shardIdFromSimple(IndexRouting.fromIndexMetadata(shrunk), term, null);
            shardIds = IndexMetadata.selectShrinkShards(shrunkShard, metadata, shrunk.getNumberOfShards());
            assertEquals(Arrays.toString(shardSplits), 1, shardIds.stream().filter((sid) -> sid.id() == shard).count());
        }
    }

    public void testGenerateShardIdSplit() {
        int[][] possibleValues = new int[][] { { 2, 4, 8 }, { 2, 10, 20 }, { 3, 12, 36 }, { 1, 5, 15 } };
        for (int i = 0; i < 10; i++) {
            int[] shardSplits = randomFrom(possibleValues);
            assertEquals(shardSplits[0], (shardSplits[0] * shardSplits[1]) / shardSplits[1]);
            assertEquals(shardSplits[1], (shardSplits[1] * shardSplits[2]) / shardSplits[2]);
            IndexMetadata metadata = IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(shardSplits[0])
                .numberOfReplicas(1)
                .setRoutingNumShards(shardSplits[2])
                .build();
            String term = randomAlphaOfLength(10);
            final int shard = shardIdFromSimple(IndexRouting.fromIndexMetadata(metadata), term, null);
            IndexMetadata split = IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(shardSplits[1])
                .numberOfReplicas(1)
                .setRoutingNumShards(shardSplits[2])
                .build();
            int shrunkShard = shardIdFromSimple(IndexRouting.fromIndexMetadata(split), term, null);

            ShardId shardId = IndexMetadata.selectSplitShard(shrunkShard, metadata, split.getNumberOfShards());
            assertNotNull(shardId);
            assertEquals(shard, shardId.getId());

            split = IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(shardSplits[2])
                .numberOfReplicas(1)
                .setRoutingNumShards(shardSplits[2])
                .build();
            shrunkShard = shardIdFromSimple(IndexRouting.fromIndexMetadata(split), term, null);
            shardId = IndexMetadata.selectSplitShard(shrunkShard, metadata, split.getNumberOfShards());
            assertNotNull(shardId);
            assertEquals(shard, shardId.getId());
        }
    }

    public void testCollectSearchShardsInStandardIndex() {
        for (int shards = 1; shards < 5; shards++) {
            IndexRouting indexRouting = IndexRouting.fromIndexMetadata(
                IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(shards).numberOfReplicas(1).build()
            );

            for (int i = 0; i < 20; i++) {
                String routing = randomUnicodeOfLengthBetween(1, 50);

                Set<Integer> searchShardSet = new HashSet<>();
                indexRouting.collectSearchShards(routing, searchShardSet::add);
                assertThat(searchShardSet, hasSize(1));
            }
        }
    }

    public void testPartitionedIndex() {
        // make sure the same routing value always has each _id fall within the configured partition size
        for (int shards = 1; shards < 5; shards++) {
            for (int partitionSize = 1; partitionSize == 1 || partitionSize < shards; partitionSize++) {
                IndexRouting indexRouting = IndexRouting.fromIndexMetadata(
                    IndexMetadata.builder("test")
                        .settings(settings(Version.CURRENT))
                        .numberOfShards(shards)
                        .routingPartitionSize(partitionSize)
                        .numberOfReplicas(1)
                        .build()
                );

                for (int i = 0; i < 20; i++) {
                    String routing = randomUnicodeOfLengthBetween(1, 50);

                    Set<Integer> shardSet = new HashSet<>();
                    for (int k = 0; k < 150; k++) {
                        String id = randomUnicodeOfLengthBetween(1, 50);
                        shardSet.add(shardIdFromSimple(indexRouting, id, routing));
                    }
                    assertThat(shardSet, hasSize(partitionSize));

                    Set<Integer> searchShardSet = new HashSet<>();
                    indexRouting.collectSearchShards(routing, searchShardSet::add);
                    assertThat(searchShardSet, hasSize(partitionSize));
                }
            }
        }
    }

    public void testPartitionedIndexShrunk() {
        Map<String, Map<String, Integer>> routingIdToShard = new HashMap<>();

        Map<String, Integer> routingA = new HashMap<>();
        routingA.put("a_0", 1);
        routingA.put("a_1", 2);
        routingA.put("a_2", 2);
        routingA.put("a_3", 2);
        routingA.put("a_4", 1);
        routingA.put("a_5", 2);
        routingIdToShard.put("a", routingA);

        Map<String, Integer> routingB = new HashMap<>();
        routingB.put("b_0", 0);
        routingB.put("b_1", 0);
        routingB.put("b_2", 0);
        routingB.put("b_3", 0);
        routingB.put("b_4", 3);
        routingB.put("b_5", 3);
        routingIdToShard.put("b", routingB);

        Map<String, Integer> routingC = new HashMap<>();
        routingC.put("c_0", 1);
        routingC.put("c_1", 1);
        routingC.put("c_2", 0);
        routingC.put("c_3", 0);
        routingC.put("c_4", 0);
        routingC.put("c_5", 1);
        routingIdToShard.put("c", routingC);

        Map<String, Integer> routingD = new HashMap<>();
        routingD.put("d_0", 2);
        routingD.put("d_1", 2);
        routingD.put("d_2", 3);
        routingD.put("d_3", 3);
        routingD.put("d_4", 3);
        routingD.put("d_5", 3);
        routingIdToShard.put("d", routingD);

        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(
            IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .setRoutingNumShards(8)
                .numberOfShards(4)
                .routingPartitionSize(3)
                .numberOfReplicas(1)
                .build()
        );

        for (Map.Entry<String, Map<String, Integer>> routingIdEntry : routingIdToShard.entrySet()) {
            String routing = routingIdEntry.getKey();

            for (Map.Entry<String, Integer> idEntry : routingIdEntry.getValue().entrySet()) {
                String id = idEntry.getKey();
                int shard = idEntry.getValue();

                assertEquals(shard, shardIdFromSimple(indexRouting, id, routing));
            }
        }
    }

    public void testPartitionedIndexBWC() {
        Map<String, Map<String, Integer>> routingIdToShard = new HashMap<>();

        Map<String, Integer> routingA = new HashMap<>();
        routingA.put("a_0", 3);
        routingA.put("a_1", 2);
        routingA.put("a_2", 2);
        routingA.put("a_3", 3);
        routingIdToShard.put("a", routingA);

        Map<String, Integer> routingB = new HashMap<>();
        routingB.put("b_0", 5);
        routingB.put("b_1", 0);
        routingB.put("b_2", 0);
        routingB.put("b_3", 0);
        routingIdToShard.put("b", routingB);

        Map<String, Integer> routingC = new HashMap<>();
        routingC.put("c_0", 4);
        routingC.put("c_1", 4);
        routingC.put("c_2", 3);
        routingC.put("c_3", 4);
        routingIdToShard.put("c", routingC);

        Map<String, Integer> routingD = new HashMap<>();
        routingD.put("d_0", 3);
        routingD.put("d_1", 4);
        routingD.put("d_2", 4);
        routingD.put("d_3", 4);
        routingIdToShard.put("d", routingD);

        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(
            IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(6)
                .routingPartitionSize(2)
                .numberOfReplicas(1)
                .build()
        );

        for (Map.Entry<String, Map<String, Integer>> routingIdEntry : routingIdToShard.entrySet()) {
            String routing = routingIdEntry.getKey();

            for (Map.Entry<String, Integer> idEntry : routingIdEntry.getValue().entrySet()) {
                String id = idEntry.getKey();
                int shard = idEntry.getValue();

                assertEquals(shard, shardIdFromSimple(indexRouting, id, routing));
            }
        }
    }

    /**
     * Ensures that all changes to the hash-function / shard selection are BWC
     */
    public void testBWC() {
        Map<String, Integer> termToShard = new TreeMap<>();
        termToShard.put("sEERfFzPSI", 1);
        termToShard.put("cNRiIrjzYd", 7);
        termToShard.put("BgfLBXUyWT", 5);
        termToShard.put("cnepjZhQnb", 3);
        termToShard.put("OKCmuYkeCK", 6);
        termToShard.put("OutXGRQUja", 5);
        termToShard.put("yCdyocKWou", 1);
        termToShard.put("KXuNWWNgVj", 2);
        termToShard.put("DGJOYrpESx", 4);
        termToShard.put("upLDybdTGs", 5);
        termToShard.put("yhZhzCPQby", 1);
        termToShard.put("EyCVeiCouA", 1);
        termToShard.put("tFyVdQauWR", 6);
        termToShard.put("nyeRYDnDQr", 6);
        termToShard.put("hswhrppvDH", 0);
        termToShard.put("BSiWvDOsNE", 5);
        termToShard.put("YHicpFBSaY", 1);
        termToShard.put("EquPtdKaBZ", 4);
        termToShard.put("rSjLZHCDfT", 5);
        termToShard.put("qoZALVcite", 7);
        termToShard.put("yDCCPVBiCm", 7);
        termToShard.put("ngizYtQgGK", 5);
        termToShard.put("FYQRIBcNqz", 0);
        termToShard.put("EBzEDAPODe", 2);
        termToShard.put("YePigbXgKb", 1);
        termToShard.put("PeGJjomyik", 3);
        termToShard.put("cyQIvDmyYD", 7);
        termToShard.put("yIEfZrYfRk", 5);
        termToShard.put("kblouyFUbu", 7);
        termToShard.put("xvIGbRiGJF", 3);
        termToShard.put("KWimwsREPf", 4);
        termToShard.put("wsNavvIcdk", 7);
        termToShard.put("xkWaPcCmpT", 0);
        termToShard.put("FKKTOnJMDy", 7);
        termToShard.put("RuLzobYixn", 2);
        termToShard.put("mFohLeFRvF", 4);
        termToShard.put("aAMXnamRJg", 7);
        termToShard.put("zKBMYJDmBI", 0);
        termToShard.put("ElSVuJQQuw", 7);
        termToShard.put("pezPtTQAAm", 7);
        termToShard.put("zBjjNEjAex", 2);
        termToShard.put("PGgHcLNPYX", 7);
        termToShard.put("hOkpeQqTDF", 3);
        termToShard.put("chZXraUPBH", 7);
        termToShard.put("FAIcSmmNXq", 5);
        termToShard.put("EZmDicyayC", 0);
        termToShard.put("GRIueBeIyL", 7);
        termToShard.put("qCChjGZYLp", 3);
        termToShard.put("IsSZQwwnUT", 3);
        termToShard.put("MGlxLFyyCK", 3);
        termToShard.put("YmscwrKSpB", 0);
        termToShard.put("czSljcjMop", 5);
        termToShard.put("XhfGWwNlng", 1);
        termToShard.put("cWpKJjlzgj", 7);
        termToShard.put("eDzIfMKbvk", 1);
        termToShard.put("WFFWYBfnTb", 0);
        termToShard.put("oDdHJxGxja", 7);
        termToShard.put("PDOQQqgIKE", 1);
        termToShard.put("bGEIEBLATe", 6);
        termToShard.put("xpRkJPWVpu", 2);
        termToShard.put("kTwZnPEeIi", 2);
        termToShard.put("DifcuqSsKk", 1);
        termToShard.put("CEmLmljpXe", 5);
        termToShard.put("cuNKtLtyJQ", 7);
        termToShard.put("yNjiAnxAmt", 5);
        termToShard.put("bVDJDCeaFm", 2);
        termToShard.put("vdnUhGLFtl", 0);
        termToShard.put("LnqSYezXbr", 5);
        termToShard.put("EzHgydDCSR", 3);
        termToShard.put("ZSKjhJlcpn", 1);
        termToShard.put("WRjUoZwtUz", 3);
        termToShard.put("RiBbcCdIgk", 4);
        termToShard.put("yizTqyjuDn", 4);
        termToShard.put("QnFjcpcZUT", 4);
        termToShard.put("agYhXYUUpl", 7);
        termToShard.put("UOjiTugjNC", 7);
        termToShard.put("nICGuWTdfV", 0);
        termToShard.put("NrnSmcnUVF", 2);
        termToShard.put("ZSzFcbpDqP", 3);
        termToShard.put("YOhahLSzzE", 5);
        termToShard.put("iWswCilUaT", 1);
        termToShard.put("zXAamKsRwj", 2);
        termToShard.put("aqGsrUPHFq", 5);
        termToShard.put("eDItImYWTS", 1);
        termToShard.put("JAYDZMRcpW", 4);
        termToShard.put("lmvAaEPflK", 7);
        termToShard.put("IKuOwPjKCx", 5);
        termToShard.put("schsINzlYB", 1);
        termToShard.put("OqbFNxrKrF", 2);
        termToShard.put("QrklDfvEJU", 6);
        termToShard.put("VLxKRKdLbx", 4);
        termToShard.put("imoydNTZhV", 1);
        termToShard.put("uFZyTyOMRO", 4);
        termToShard.put("nVAZVMPNNx", 3);
        termToShard.put("rPIdESYaAO", 5);
        termToShard.put("nbZWPWJsIM", 0);
        termToShard.put("wRZXPSoEgd", 3);
        termToShard.put("nGzpgwsSBc", 4);
        termToShard.put("AITyyoyLLs", 4);
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(
            IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(8).numberOfReplicas(1).build()
        );
        for (Map.Entry<String, Integer> entry : termToShard.entrySet()) {
            String key = entry.getKey();
            int shardId;
            switch (between(0, 2)) {
                case 0:
                    shardId = shardIdFromSimple(indexRouting, key, null);
                    break;
                case 1:
                    shardId = shardIdFromSimple(indexRouting, randomAlphaOfLength(5), key);
                    break;
                case 2:
                    AtomicInteger s = new AtomicInteger(-1);
                    indexRouting.collectSearchShards(key, r -> {
                        int old = s.getAndSet(r);
                        assertThat("only called once", old, equalTo(-1));
                    });
                    shardId = s.get();
                    break;
                default:
                    throw new AssertionError("invalid option");
            }
            assertEquals(shardId, entry.getValue().intValue());
        }
    }

    /**
     * Extract a shardId from a "simple" {@link IndexRouting} using a randomly
     * chosen method. All of the random methods <strong>should</strong> return
     * the same results.
     */
    private int shardIdFromSimple(IndexRouting indexRouting, String id, @Nullable String routing) {
        switch (between(0, 3)) {
            case 0:
                return indexRouting.indexShard(id, routing, null, null);
            case 1:
                return indexRouting.updateShard(id, routing);
            case 2:
                return indexRouting.deleteShard(id, routing);
            case 3:
                return indexRouting.getShard(id, routing);
            default:
                throw new AssertionError("invalid option");
        }
    }

    public void testRoutingPathSpecifiedRouting() throws IOException {
        IndexRouting routing = indexRoutingForPath(between(1, 5), "foo");
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> routing.indexShard(
                null,
                randomAlphaOfLength(5),
                XContentType.JSON,
                source(org.havenask.common.collect.Map.of("foo", randomAlphaOfLength(5)))
            )
        );
        assertThat(e.getMessage(), containsString("routing value is not the same with source routing field"));
    }

    public void testRoutingPathEmptySource() throws IOException {
        String routingPath = randomAlphaOfLength(5) + ", " + randomAlphaOfLength(5);
        IndexRouting routing = indexRoutingForPath(between(1, 5), routingPath);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> routing.indexShard(randomAlphaOfLength(5), null, XContentType.JSON, source(org.havenask.common.collect.Map.of()))
        );
        assertThat(
            e.getMessage(),
            equalTo("Error extracting routing: source didn't contain any routing dimension fields [" + routingPath + "]")
        );
    }

    public void testRoutingPathMismatchSource() throws IOException {
        IndexRouting routing = indexRoutingForPath(between(1, 5), "foo, foo2");
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> routing.indexShard(
                randomAlphaOfLength(5),
                null,
                XContentType.JSON,
                source(org.havenask.common.collect.Map.of("bar", "dog"))
            )
        );
        assertThat(e.getMessage(), equalTo("Error extracting routing: source didn't contain any routing dimension fields [foo, foo2]"));
    }

    public void testRoutingPathUpdate() throws IOException {
        IndexRouting routing = indexRoutingForPath(between(1, 5), "foo");
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> routing.updateShard(randomAlphaOfLength(5), randomBoolean() ? null : randomAlphaOfLength(5))
        );
        assertThat(e.getMessage(), equalTo("update is not supported because the destination index [test] is in time series mode"));
    }

    public void testRoutingPathDelete() throws IOException {
        IndexRouting routing = indexRoutingForPath(between(1, 5), "foo");
        Exception e = expectThrows(IllegalArgumentException.class, () -> routing.deleteShard(randomAlphaOfLength(5), null));
        assertThat(e.getMessage(), equalTo("A routing value is required"));
    }

    public void testRoutingPathGet() throws IOException {
        IndexRouting routing = indexRoutingForPath(between(1, 5), "foo");
        Exception e = expectThrows(IllegalArgumentException.class, () -> routing.getShard(randomAlphaOfLength(5), null));
        assertThat(e.getMessage(), equalTo("A routing value is required"));
    }

    public void testRoutingPathCollectSearchWithRouting() {
        IndexRouting routing = indexRoutingForPath(between(1, 5), "foo");
        routing.collectSearchShards(randomAlphaOfLength(5), (value) -> {});
    }

    public void testRoutingPathOneTopLevel() throws IOException {
        int shards = between(2, 1000);
        IndexRouting routing = indexRoutingForPath(shards, "foo,foo2");
        assertIndexShard(
            routing,
            org.havenask.common.collect.Map.of("foo", "cat", "bar", "dog"),
            Math.floorMod(hash(org.havenask.common.collect.List.of("foo", "cat")), shards)
        );
    }

    public void testRoutingPathManyTopLevel() throws IOException {
        int shards = between(2, 1000);
        IndexRouting routing = indexRoutingForPath(shards, "f*");
        assertIndexShard(
            routing,
            org.havenask.common.collect.Map.of("foo", "cat", "bar", "dog", "foa", "a", "fob", "b"),
            Math.floorMod(hash(org.havenask.common.collect.List.of("foa", "a", "fob", "b", "foo", "cat")), shards) // Note that the fields
                                                                                                                   // are
            // sorted
        );
    }

    public void testRoutingPathOneSub() throws IOException {
        int shards = between(2, 1000);
        IndexRouting routing = indexRoutingForPath(shards, "foo.*");
        assertIndexShard(
            routing,
            org.havenask.common.collect.Map.of("foo", org.havenask.common.collect.Map.of("bar", "cat"), "baz", "dog"),
            Math.floorMod(hash(org.havenask.common.collect.List.of("foo.bar", "cat")), shards)
        );
    }

    public void testRoutingPathManySubs() throws IOException {
        int shards = between(2, 1000);
        IndexRouting routing = indexRoutingForPath(shards, "foo.*,bar.*,baz.*");
        assertIndexShard(
            routing,
            org.havenask.common.collect.Map.of(
                "foo",
                org.havenask.common.collect.Map.of("a", "cat"),
                "bar",
                org.havenask.common.collect.Map.of("thing", "yay", "this", "too")
            ),
            Math.floorMod(hash(org.havenask.common.collect.List.of("bar.thing", "yay", "bar.this", "too", "foo.a", "cat")), shards)
        );
    }

    public void testRoutingPathBwc() throws IOException {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        IndexRouting routing = indexRoutingForPath(version, 8, "dim.*,other.*,top");
        assertIndexShard(routing, org.havenask.common.collect.Map.of("dim", org.havenask.common.collect.Map.of("a", "a")), 4);
        assertIndexShard(routing, org.havenask.common.collect.Map.of("dim", org.havenask.common.collect.Map.of("a", "b")), 5);
        assertIndexShard(routing, org.havenask.common.collect.Map.of("dim", org.havenask.common.collect.Map.of("c", "d")), 4);
        assertIndexShard(routing, org.havenask.common.collect.Map.of("other", org.havenask.common.collect.Map.of("a", "a")), 7);
        assertIndexShard(routing, org.havenask.common.collect.Map.of("top", "a"), 5);
        assertIndexShard(routing, org.havenask.common.collect.Map.of("dim", org.havenask.common.collect.Map.of("c", "d"), "top", "b"), 0);
    }

    private IndexRouting indexRoutingForPath(int shards, String path) {
        return indexRoutingForPath(Version.CURRENT, shards, path);
    }

    private IndexRouting indexRoutingForPath(Version createdVersion, int shards, String path) {
        return IndexRouting.fromIndexMetadata(
            IndexMetadata.builder("test")
                .settings(settings(createdVersion).put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), path))
                .numberOfShards(shards)
                .numberOfReplicas(1)
                .build()
        );
    }

    private void assertIndexShard(IndexRouting routing, Map<String, Object> source, int expected) throws IOException {
        assertThat(routing.indexShard(randomAlphaOfLength(5), null, XContentType.JSON, source(source)), equalTo(expected));
    }

    private BytesReference source(Map<String, Object> doc) throws IOException {
        return BytesReference.bytes(JsonXContent.contentBuilder().value(doc));
    }

    /**
     * Build the hash we expect from the extracter.
     */
    private int hash(List<String> keysAndValues) {
        assertThat(keysAndValues.size() % 2, equalTo(0));
        int hash = 0;
        for (int i = 0; i < keysAndValues.size(); i += 2) {
            int keyHash = StringHelper.murmurhash3_x86_32(new BytesRef(keysAndValues.get(i)), 0);
            int valueHash = StringHelper.murmurhash3_x86_32(new BytesRef(keysAndValues.get(i + 1)), 0);
            hash = hash * 31 + (keyHash ^ valueHash);
        }
        return hash;
    }
}
