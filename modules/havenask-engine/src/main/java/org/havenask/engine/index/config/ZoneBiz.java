package org.havenask.engine.index.config;

import java.util.List;

import org.havenask.engine.index.config.BizConfig.HashMode;

public class ZoneBiz {
    public ClusterConfig cluster_config;
    public FunctionConfig function_config;

    public static class ClusterConfig {
        public String table_name;
        public HashMode hash_mode;
        public JoinConfig join_config;
    }

    public static class JoinConfig {
        public List<JoinInfo> join_infos;
    }

    public static class JoinInfo {

    }

    public static class FunctionConfig {
        public List<Function_> functions;
        public List<Module> modules;
    }

    public static class Function_ {

    }

}
