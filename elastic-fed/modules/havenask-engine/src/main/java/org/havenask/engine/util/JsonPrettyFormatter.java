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

import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;

/**
 * TODO（zhixu.zt) 去掉fastjson做准备
 *
 * fastjson的格式化json格式，都放到这个了，
 * 后面去掉fastjson直接改这里就可以了；
 *
 * fastjson的parse接口，还需要提供一个接口出来
 */
public class JsonPrettyFormatter {

    private static final SerializeConfig gConfig;
    private static final ParserConfig gDConfig;

    static {
        gConfig = new SerializeConfig();
        gConfig.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;

        gDConfig = new ParserConfig();
        gDConfig.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
    }

    public static <T> String toString(T object) {
        return JSON.toJSONString(object, gConfig, SerializerFeature.PrettyFormat);
    }

    public static <T> String toJsonString(T object) {
        return JSON.toJSONString(object, SerializerFeature.PrettyFormat);
    }

    public static <T> T fromJsonString(String jsonString, Class<T> clazz) {
        return JSON.parseObject(jsonString, clazz);
    }

    public static <T> T fromString(String jsonString, Class<T> clazz) {
        return JSON.parseObject(jsonString, clazz, gDConfig);
    }

    public static JSONObject fromString(String jsonString) {
        return JSON.parseObject(jsonString);
    }

    public static Map<String, String> from(String jsonString) {
        return JSONObject.parseObject(jsonString, new TypeReference<Map<String, String>>() {
        });
    }
}
