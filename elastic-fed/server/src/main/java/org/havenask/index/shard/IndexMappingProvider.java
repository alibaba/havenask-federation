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

package org.havenask.index.shard;

import java.util.Collections;
import java.util.Map;

import org.havenask.common.settings.Settings;
import org.havenask.index.mapper.MapperService;

/**
 * An {@link IndexMappingProvider} is a provider for index level mappings that can be set
 */
public interface IndexMappingProvider {
    /**
     * Returns explicitly set default index mappings for the given index. This should not
     * return null.
     */
    default Map<String, Object> getAdditionalIndexMapping(Settings settings) {
        return Collections.emptyMap();
    }

    /**
     * validate the index mapping
     * throw {@link UnsupportedOperationException} if the index mapping is not valid
     */
    default void validateIndexMapping(String table, Settings indexSettings, MapperService mapperService)
        throws UnsupportedOperationException {

    }
}
