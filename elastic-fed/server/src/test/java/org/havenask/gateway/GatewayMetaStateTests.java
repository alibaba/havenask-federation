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

package org.havenask.gateway;

import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.IndexTemplateMetadata;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.metadata.MetadataIndexUpgradeService;
import org.havenask.common.settings.Settings;
import org.havenask.plugins.MetadataUpgrader;
import org.havenask.test.HavenaskTestCase;
import org.havenask.test.TestCustomMetadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.Matchers.equalTo;

public class GatewayMetaStateTests extends HavenaskTestCase {

    public void testUpdateTemplateMetadataOnUpgrade() {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.singletonList(
                        templates -> {
                            templates.put("added_test_template", IndexTemplateMetadata.builder("added_test_template")
                                .patterns(randomIndexPatterns()).build());
                            return templates;
                        }
                    ));

        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertNotSame(upgrade, metadata);
        assertFalse(Metadata.isGlobalStateEquals(upgrade, metadata));
        assertTrue(upgrade.templates().containsKey("added_test_template"));
    }

    public void testNoMetadataUpgrade() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.emptyList());
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertSame(upgrade, metadata);
        assertTrue(Metadata.isGlobalStateEquals(upgrade, metadata));
        for (IndexMetadata indexMetadata : upgrade) {
            assertTrue(metadata.hasIndexMetadata(indexMetadata));
        }
    }

    public void testCustomMetadataValidation() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.emptyList());
        try {
            GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("custom meta data too old"));
        }
    }

    public void testIndexMetadataUpgrade() {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.emptyList());
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(true), metadataUpgrader);
        assertNotSame(upgrade, metadata);
        assertTrue(Metadata.isGlobalStateEquals(upgrade, metadata));
        for (IndexMetadata indexMetadata : upgrade) {
            assertFalse(metadata.hasIndexMetadata(indexMetadata));
        }
    }

    public void testCustomMetadataNoChange() {
        Metadata metadata = randomMetadata(new CustomMetadata1("data"));
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.singletonList(HashMap::new));
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertSame(upgrade, metadata);
        assertTrue(Metadata.isGlobalStateEquals(upgrade, metadata));
        for (IndexMetadata indexMetadata : upgrade) {
            assertTrue(metadata.hasIndexMetadata(indexMetadata));
        }
    }

    public void testIndexTemplateValidation() {
        Metadata metadata = randomMetadata();
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Collections.singletonList(
                        customs -> {
                            throw new IllegalStateException("template is incompatible");
                        }));
        String message = expectThrows(IllegalStateException.class,
            () -> GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader)).getMessage();
        assertThat(message, equalTo("template is incompatible"));
    }


    public void testMultipleIndexTemplateUpgrade() {
        final Metadata metadata;
        switch (randomIntBetween(0, 2)) {
            case 0:
                metadata = randomMetadataWithIndexTemplates("template1", "template2");
                break;
            case 1:
                metadata = randomMetadataWithIndexTemplates(randomBoolean() ? "template1" : "template2");
                break;
            case 2:
                metadata = randomMetadata();
                break;
            default:
                throw new IllegalStateException("should never happen");
        }
        MetadataUpgrader metadataUpgrader = new MetadataUpgrader(Arrays.asList(
                indexTemplateMetadatas -> {
                    indexTemplateMetadatas.put("template1", IndexTemplateMetadata.builder("template1")
                        .patterns(randomIndexPatterns())
                        .settings(Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 20).build())
                        .build());
                    return indexTemplateMetadatas;

                },
                indexTemplateMetadatas -> {
                    indexTemplateMetadatas.put("template2", IndexTemplateMetadata.builder("template2")
                        .patterns(randomIndexPatterns())
                        .settings(Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 10).build()).build());
                    return indexTemplateMetadatas;

                }
            ));
        Metadata upgrade = GatewayMetaState.upgradeMetadata(metadata, new MockMetadataIndexUpgradeService(false), metadataUpgrader);
        assertNotSame(upgrade, metadata);
        assertFalse(Metadata.isGlobalStateEquals(upgrade, metadata));
        assertNotNull(upgrade.templates().get("template1"));
        assertThat(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(upgrade.templates().get("template1").settings()), equalTo(20));
        assertNotNull(upgrade.templates().get("template2"));
        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(upgrade.templates().get("template2").settings()), equalTo(10));
        for (IndexMetadata indexMetadata : upgrade) {
            assertTrue(metadata.hasIndexMetadata(indexMetadata));
        }
    }

    private static class MockMetadataIndexUpgradeService extends MetadataIndexUpgradeService {
        private final boolean upgrade;

        MockMetadataIndexUpgradeService(boolean upgrade) {
            super(Settings.EMPTY, null, null, null, null, null);
            this.upgrade = upgrade;
        }

        @Override
        public IndexMetadata upgradeIndexMetadata(IndexMetadata indexMetadata, Version minimumIndexCompatibilityVersion) {
            return upgrade ? IndexMetadata.builder(indexMetadata).build() : indexMetadata;
        }
    }

    private static class CustomMetadata1 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_1";

        CustomMetadata1(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private static Metadata randomMetadata(TestCustomMetadata... customMetadatas) {
        Metadata.Builder builder = Metadata.builder();
        for (TestCustomMetadata customMetadata : customMetadatas) {
            builder.putCustom(customMetadata.getWriteableName(), customMetadata);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(
                IndexMetadata.builder(randomAlphaOfLength(10))
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
        }
        return builder.build();
    }

    private static Metadata randomMetadataWithIndexTemplates(String... templates) {
        Metadata.Builder builder = Metadata.builder();
        for (String template : templates) {
            IndexTemplateMetadata templateMetadata = IndexTemplateMetadata.builder(template)
                .settings(settings(Version.CURRENT)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 3))
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 5)))
                .patterns(randomIndexPatterns())
                .build();
            builder.put(templateMetadata);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(
                IndexMetadata.builder(randomAlphaOfLength(10))
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
        }
        return builder.build();
    }

    private static List<String> randomIndexPatterns() {
        return Arrays.asList(Objects.requireNonNull(generateRandomStringArray(10, 100, false, false)));
    }
}
