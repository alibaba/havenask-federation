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

package org.havenask.plugin.discovery.gce;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.util.ClassInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.havenask.cloud.gce.GceInstancesService;
import org.havenask.cloud.gce.GceInstancesServiceImpl;
import org.havenask.cloud.gce.GceMetadataService;
import org.havenask.cloud.gce.network.GceNameResolver;
import org.havenask.cloud.gce.util.Access;
import org.havenask.common.Booleans;
import org.havenask.common.network.NetworkService;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.core.internal.io.IOUtils;
import org.havenask.discovery.SeedHostsProvider;
import org.havenask.discovery.gce.GceSeedHostsProvider;
import org.havenask.plugins.DiscoveryPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class GceDiscoveryPlugin extends Plugin implements DiscoveryPlugin, Closeable {

    /** Determines whether settings those reroutes GCE call should be allowed (for testing purposes only). */
    private static final boolean ALLOW_REROUTE_GCE_SETTINGS =
        Booleans.parseBoolean(System.getProperty("havenask.allow_reroute_gce_settings", "false"));

    public static final String GCE = "gce";
    protected final Settings settings;
    private static final Logger logger = LogManager.getLogger(GceDiscoveryPlugin.class);
    // stashed when created in order to properly close
    private final SetOnce<GceInstancesService> gceInstancesService = new SetOnce<>();

    static {
        /*
         * GCE's http client changes access levels because its silly and we
         * can't allow that on any old stack so we pull it here, up front,
         * so we can cleanly check the permissions for it. Without this changing
         * the permission can fail if any part of core is on the stack because
         * our plugin permissions don't allow core to "reach through" plugins to
         * change the permission. Because that'd be silly.
         */
        Access.doPrivilegedVoid( () -> ClassInfo.of(HttpHeaders.class, true));
    }

    public GceDiscoveryPlugin(Settings settings) {
        this.settings = settings;
        logger.trace("starting gce discovery plugin...");
    }

    // overrideable for tests
    protected GceInstancesService createGceInstancesService() {
        return new GceInstancesServiceImpl(settings);
    }

    @Override
    public Map<String, Supplier<SeedHostsProvider>> getSeedHostProviders(TransportService transportService,
                                                                         NetworkService networkService) {
        return Collections.singletonMap(GCE, () -> {
            gceInstancesService.set(createGceInstancesService());
            return new GceSeedHostsProvider(settings, gceInstancesService.get(), transportService, networkService);
        });
    }

    @Override
    public NetworkService.CustomNameResolver getCustomNameResolver(Settings settings) {
        logger.debug("Register _gce_, _gce:xxx network names");
        return new GceNameResolver(new GceMetadataService(settings));
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>(
            Arrays.asList(
                // Register GCE settings
                GceInstancesService.PROJECT_SETTING,
                GceInstancesService.ZONE_SETTING,
                GceSeedHostsProvider.TAGS_SETTING,
                GceInstancesService.REFRESH_SETTING,
                GceInstancesService.RETRY_SETTING,
                GceInstancesService.MAX_WAIT_SETTING)
        );

        if (ALLOW_REROUTE_GCE_SETTINGS) {
            settings.add(GceMetadataService.GCE_HOST);
            settings.add(GceInstancesServiceImpl.GCE_ROOT_URL);
        }
        return Collections.unmodifiableList(settings);
    }



    @Override
    public void close() throws IOException {
        IOUtils.close(gceInstancesService.get());
    }
}
