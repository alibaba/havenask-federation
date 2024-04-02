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

package org.havenask.aliyun.oss.service;

import static org.havenask.common.settings.Setting.Property;
import static org.havenask.common.settings.Setting.boolSetting;
import static org.havenask.common.settings.Setting.byteSizeSetting;
import static org.havenask.common.settings.Setting.simpleString;

import org.apache.commons.lang.StringUtils;
import org.havenask.common.settings.SecureString;
import org.havenask.common.settings.Setting;
import org.havenask.common.unit.ByteSizeUnit;
import org.havenask.common.unit.ByteSizeValue;

public class OssClientSettings {
    private static final ByteSizeValue MIN_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.MB);
    private static final ByteSizeValue MAX_CHUNK_SIZE = new ByteSizeValue(1, ByteSizeUnit.GB);

    public static final Setting<SecureString> ACCESS_KEY_ID = new Setting<>(
        "access_key_id",
        StringUtils.EMPTY,
        SecureString::new,
        Property.Filtered,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<SecureString> SECRET_ACCESS_KEY = new Setting<>(
        "secret_access_key",
        StringUtils.EMPTY,
        SecureString::new,
        Property.Filtered,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<String> ENDPOINT = Setting.simpleString("endpoint", Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<SecureString> SECURITY_TOKEN = new Setting<>(
        "security_token",
        StringUtils.EMPTY,
        SecureString::new,
        Property.Filtered,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<String> BUCKET = simpleString("bucket", Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> BASE_PATH = simpleString("base_path", Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<Boolean> COMPRESS = boolSetting("compress", false, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<ByteSizeValue> CHUNK_SIZE = byteSizeSetting(
        "chunk_size",
        MAX_CHUNK_SIZE,
        MIN_CHUNK_SIZE,
        MAX_CHUNK_SIZE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<SecureString> ECS_RAM_ROLE = new Setting<>(
        "ecs_ram_role",
        StringUtils.EMPTY,
        SecureString::new,
        Property.Filtered,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<SecureString> AUTO_SNAPSHOT_BUCKET = new Setting<>(
        "auto_snapshot_bucket",
        StringUtils.EMPTY,
        SecureString::new,
        Property.Filtered,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Boolean> SUPPORT_CNAME = boolSetting(
        "support_cname",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<String> RAM_ROLE_CREDENTIALS_ENDPOINT = Setting.simpleString(
        "ram_role.credentials_endpoint",
        "",
        Property.NodeScope
    );
}
