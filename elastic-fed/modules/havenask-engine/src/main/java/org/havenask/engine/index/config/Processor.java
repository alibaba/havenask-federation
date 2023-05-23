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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Processor {
    public ProcessorChainConfig[] processor_chain_config;

    public static class ProcessorChainConfig {
        public List<String> clusters = new ArrayList<>();
        public List<Module> modules = new ArrayList<>();
        public List<ProcessChain> document_processor_chain = new ArrayList<>() {
            {
                add(new ProcessChain());
            }
        };
    }

    public static Processor getEmptyProcessor() {
        Processor processor = new Processor();
        processor.processor_chain_config = new ProcessorChainConfig[0];
        return processor;
    }

    public static class ProcessorConfig {
        public int processor_queue_size = 2000;
        public int processor_thread_num = 30;
    }

    public static class Module {
        public String module_name;
        public String module_path;
        public Map<String, String> parameters;
    }

    public static class ProcessChain {
        public String class_name = "TokenizeDocumentProcessor";
        public String module_name = "";
        public Map<String, String> parameters = new HashMap<>();

        public ProcessChain() {}

        public ProcessChain(String class_name) {
            this.class_name = class_name;
        }
    }

    public static class ProcessorRuleConfig {
        public int parallel_num = 1;
        public int partition_count = 1;
    }

}
