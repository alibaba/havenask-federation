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

package org.havenask.engine.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * fastjson的格式化json格式，都放到这个了，
 * 后面去掉fastjson直接改这里就可以了；
 *
 * fastjson的parse接口，还需要提供一个接口出来
 */
public class JsonPrettyFormatter {
    public static <T> String toJsonString(T object) {
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> JSON.toJSONString(object, SerializerFeature.PrettyFormat));
    }

    public static <T> T fromJsonString(String jsonString, Class<T> clazz) {
        return AccessController.doPrivileged((PrivilegedAction<T>) () -> JSON.parseObject(jsonString, clazz));
    }

    public static JSONObject fromString(String jsonString) {
        return AccessController.doPrivileged((PrivilegedAction<JSONObject>) () -> return JSON.parseObject(jsonString););
    }
}
