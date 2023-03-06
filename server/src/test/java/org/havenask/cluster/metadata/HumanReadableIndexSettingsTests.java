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

import org.havenask.Version;
import org.havenask.common.settings.Settings;
import org.havenask.test.HavenaskTestCase;
import org.havenask.cluster.metadata.IndexMetadata;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.havenask.test.VersionUtils.randomVersion;

public class HumanReadableIndexSettingsTests extends HavenaskTestCase {
    public void testHumanReadableSettings() {
        Version versionCreated = randomVersion(random());
        Version versionUpgraded = randomVersion(random());
        long created = System.currentTimeMillis();
        Settings testSettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, versionCreated)
                .put(IndexMetadata.SETTING_VERSION_UPGRADED, versionUpgraded)
                .put(IndexMetadata.SETTING_CREATION_DATE, created)
                .build();

        Settings humanSettings = IndexMetadata.addHumanReadableSettings(testSettings);

        assertEquals(versionCreated.toString(), humanSettings.get(IndexMetadata.SETTING_VERSION_CREATED_STRING, null));
        assertEquals(versionUpgraded.toString(), humanSettings.get(IndexMetadata.SETTING_VERSION_UPGRADED_STRING, null));
        ZonedDateTime creationDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(created), ZoneOffset.UTC);
        assertEquals(creationDate.toString(), humanSettings.get(IndexMetadata.SETTING_CREATION_DATE_STRING, null));
    }
}
