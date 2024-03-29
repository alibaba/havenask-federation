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

package org.havenask.bootstrap;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public class JarHell {
    private JarHell() {}

    public static void checkJarHell() throws Exception {}

    public static void checkJarHell(URL urls[]) throws Exception {}

    public static void checkJarHell(Consumer<String> output) throws Exception {}

    public static void checkVersionFormat(String targetVersion) {}

    public static void checkJavaVersion(String resource, String targetVersion) {}

    // public static URL[] parseClassPath() {
    // return new URL[] {};
    // }

    public static Set<URL> parseClassPath() {
        return new HashSet<>();
    }

    public static void main(String args[]) throws Exception {
        System.out.println("checking for jar hell...");
        checkJarHell(System.out::println);
        System.out.println("no jar hell found");
    }

}
