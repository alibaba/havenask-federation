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

package org.havenask.cloud.gce;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.havenask.common.inject.AbstractModule;
import org.havenask.common.settings.Settings;

public class GceModule extends AbstractModule {
    // pkg private so tests can override with mock
    static Class<? extends GceInstancesService> computeServiceImpl = GceInstancesServiceImpl.class;

    protected final Settings settings;
    protected final Logger logger = LogManager.getLogger(GceModule.class);

    public GceModule(Settings settings) {
        this.settings = settings;
    }

    public static Class<? extends GceInstancesService> getComputeServiceImpl() {
        return computeServiceImpl;
    }

    @Override
    protected void configure() {
        logger.debug("configure GceModule (bind compute service)");
        bind(GceInstancesService.class).to(computeServiceImpl).asEagerSingleton();
    }
}
