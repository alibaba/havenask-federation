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

package org.havenask.repositories.gcs;

import org.havenask.cluster.service.ClusterService;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.common.xcontent.NamedXContentRegistry;
import org.havenask.env.Environment;
import org.havenask.indices.recovery.RecoverySettings;
import org.havenask.plugins.Plugin;
import org.havenask.plugins.ReloadablePlugin;
import org.havenask.plugins.RepositoryPlugin;
import org.havenask.repositories.Repository;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class GoogleCloudStoragePlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    // package-private for tests
    final GoogleCloudStorageService storageService;

    public GoogleCloudStoragePlugin(final Settings settings) {
        this.storageService = createStorageService();
        // eagerly load client settings so that secure settings are readable (not closed)
        reload(settings);
    }

    // overridable for tests
    protected GoogleCloudStorageService createStorageService() {
        return new GoogleCloudStorageService();
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                           ClusterService clusterService, RecoverySettings recoverySettings) {
        return Collections.singletonMap(GoogleCloudStorageRepository.TYPE,
            metadata -> new GoogleCloudStorageRepository(metadata, namedXContentRegistry, this.storageService, clusterService,
                recoverySettings));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING,
            GoogleCloudStorageClientSettings.ENDPOINT_SETTING,
            GoogleCloudStorageClientSettings.PROJECT_ID_SETTING,
            GoogleCloudStorageClientSettings.CONNECT_TIMEOUT_SETTING,
            GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING,
            GoogleCloudStorageClientSettings.APPLICATION_NAME_SETTING,
            GoogleCloudStorageClientSettings.TOKEN_URI_SETTING);
    }

    @Override
    public void reload(Settings settings) {
        // Secure settings should be readable inside this method. Duplicate client
        // settings in a format (`GoogleCloudStorageClientSettings`) that does not
        // require for the `SecureSettings` to be open. Pass that around (the
        // `GoogleCloudStorageClientSettings` instance) instead of the `Settings`
        // instance.
        final Map<String, GoogleCloudStorageClientSettings> clientsSettings = GoogleCloudStorageClientSettings.load(settings);
        this.storageService.refreshAndClearCache(clientsSettings);
    }
}
