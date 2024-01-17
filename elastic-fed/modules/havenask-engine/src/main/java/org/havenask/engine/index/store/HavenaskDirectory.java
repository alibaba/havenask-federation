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

package org.havenask.engine.index.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;

public class HavenaskDirectory extends FilterDirectory {
    private final Path shardPath;

    protected HavenaskDirectory(Directory in, Path shardPath) {
        super(in);
        this.shardPath = shardPath;
    }

    @Override
    public String[] listAll() throws IOException {
        List<String> files = listHavenaskFiles();
        files.addAll(List.of(in.listAll()));
        return files.toArray(new String[0]);
    }

    List<String> listHavenaskFiles() throws IOException {
        List<Path> files = retryListAllHavenaskFiles(shardPath);
        String shardPathStr = shardPath.toString();
        List<String> fileNames = new ArrayList<>();
        files.forEach(path -> {
            String fileName = path.toString().substring(shardPathStr.length() + 1);
            fileNames.add(fileName);
        });
        return fileNames;
    }

    static List<Path> retryListAllHavenaskFiles(Path dir) {
        try {
            return listAllHavenaskFiles(dir);
        } catch (IOException e) {
            return retryListAllHavenaskFiles(dir);
        }
    }

    static List<Path> listAllHavenaskFiles(Path dir) throws IOException {
        List<Path> files = new ArrayList<>();
        try (Stream<Path> stream = Files.list(dir)) {
            stream.forEach(path -> {
                if (Files.isDirectory(path)) {
                    files.addAll(retryListAllHavenaskFiles(path));
                } else {
                    files.add(path);
                }
            });
        }
        return files;
    }

    @Override
    public void sync(Collection<String> names) throws IOException {

    }
}
