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

    public Map<String, File> files = new LinkedHashMap<>();
    public Map<String, File> packageFiles = new LinkedHashMap<>();

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

    private static Map<String, File> parseFiles(JSONObject jsonObject) {
        if (jsonObject == null) {
            return new LinkedHashMap<>();
        }

        JSONObject filesJson = jsonObject.getJSONObject("");
        if (filesJson == null) {
            return new LinkedHashMap<>();
        }

        Map<String, File> files = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : filesJson.entrySet()) {
            File file = new File();
            file.name = entry.getKey();
            JSONObject fileObject = (JSONObject) entry.getValue();
            file.length = fileObject.getLong("length");
            if (file.length == -2) {
                file.type = Type.DIR;
                file.length = Integer.MAX_VALUE;
            } else {
                file.type = Type.FILE;
            }
            files.put(file.name, file);
        }
        return files;
    }

    @Override
    public String toString() {
        JSONObject filesJson = new JSONObject(true);
        files.forEach((name, file) -> {
            JSONObject fileJson = new JSONObject();
            fileJson.put("length", file.length);
            filesJson.put(name, fileJson);
        });
        JSONObject packageFilesJson = new JSONObject(true);
        packageFiles.forEach((name, file) -> {
            JSONObject fileJson = new JSONObject();
            fileJson.put("length", file.length);
            packageFilesJson.put(name, fileJson);
        });
        JSONObject jsonObject = new JSONObject(true);
        if (filesJson.size() > 0) {
            JSONObject filesParent = new JSONObject();
            filesParent.put("", filesJson);
            jsonObject.put("files", filesParent);
        } else {
            jsonObject.put("files", new JSONObject());
        }

        if (packageFilesJson.size() > 0) {
            JSONObject packageFilesParent = new JSONObject();
            packageFilesParent.put("", packageFilesJson);
            jsonObject.put("package_files", packageFilesParent);
        } else {
            jsonObject.put("package_files", new JSONObject());
        }
        return jsonObject.toJSONString();
    }
}
