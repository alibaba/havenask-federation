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

package org.havenask.engine.index.config;

import java.util.LinkedHashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;

public class EntryTable {

    public enum Type {
        FILE,
        DIR
    }

    public Map<String, Map<String, File>> files = new LinkedHashMap<>();
    public Map<String, Map<String, File>> packageFiles = new LinkedHashMap<>();

    public static class File {
        public String name;
        public Type type;
        public long length;
    }

    public static EntryTable parse(String content) {
        JSONObject jsonObject = JSON.parseObject(content, Feature.OrderedField);
        EntryTable entryTable = new EntryTable();
        entryTable.files = parseFiles(jsonObject.getJSONObject("files"));
        entryTable.packageFiles = parseFiles(jsonObject.getJSONObject("package_files"));
        return entryTable;
    }

    private static Map<String, Map<String, File>> parseFiles(JSONObject jsonObject) {
        if (jsonObject == null || jsonObject.size() == 0) {
            return new LinkedHashMap<>();
        }

        Map<String, Map<String, File>> files = new LinkedHashMap<>();
        jsonObject.forEach((key, value) -> {
            Map<String, File> fileMap = new LinkedHashMap<>();
            JSONObject fileObject = (JSONObject) value;
            fileObject.forEach((name, fileValue) -> {
                File file = new File();
                file.name = name;
                JSONObject fileJson = (JSONObject) fileValue;
                file.length = fileJson.getLong("length");
                if (file.length == -2) {
                    file.type = Type.DIR;
                    file.length = Integer.MAX_VALUE;
                } else {
                    file.type = Type.FILE;
                }
                fileMap.put(file.name, file);
            });
            files.put(key, fileMap);
        });

        return files;
    }

    @Override
    public String toString() {
        JSONObject filesJson = new JSONObject(true);
        files.forEach((name, fileMap) -> {
            JSONObject fileMapJson = new JSONObject(true);
            fileMap.forEach((fileName, file) -> {
                JSONObject fileJson = new JSONObject();
                if (file.type == Type.DIR) {
                    fileJson.put("length", -2);
                } else {
                    fileJson.put("length", file.length);
                }
                fileMapJson.put(fileName, fileJson);
            });
            filesJson.put(name, fileMapJson);
        });

        JSONObject packageFilesJson = new JSONObject(true);
        packageFiles.forEach((name, fileMap) -> {
            JSONObject fileMapJson = new JSONObject(true);
            fileMap.forEach((fileName, file) -> {
                JSONObject fileJson = new JSONObject();
                if (file.type == Type.DIR) {
                    fileJson.put("length", -2);
                } else {
                    fileJson.put("length", file.length);
                }
                fileMapJson.put(fileName, fileJson);
            });
            packageFilesJson.put(name, fileMapJson);
        });
        JSONObject jsonObject = new JSONObject(true);
        jsonObject.put("files", filesJson);
        jsonObject.put("package_files", packageFilesJson);
        return jsonObject.toJSONString();
    }
}
