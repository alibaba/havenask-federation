package org.havenask.engine.index.config;

import java.util.Map;

public class Processor {
    public ProcessorChainConfig[] processor_chain_config;

    public static class ProcessorChainConfig {
        public String table_name;
        public String[] clusters;
        public Module[] modules;
        public ProcessChain[] document_processor_chain;
        public ProcessChain[] sub_document_processor_chain;
    }

    static public Processor getEmptyProcessor() {
        Processor processor = new Processor();
        processor.processor_chain_config = new ProcessorChainConfig[0];
        return processor;
    }

    public static class ProcessorConfig {
        public int processor_queue_size;
        public int processor_thread_num;
    }

    public static class Module {
        public String module_name;
        public String module_path;
        public Map<String,String> parameters;
    }

    public static class ProcessChain {
        public String class_name;
        public String module_name;
        public Map<String,String> parameters;

        public ProcessChain() {}

        public ProcessChain(String class_name) {
            this.class_name = class_name;
        }
    }
}
