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
import java.util.List;

import org.havenask.engine.index.config.Processor.ProcessorChainConfig;
import org.havenask.engine.index.config.Processor.ProcessorConfig;
import org.havenask.engine.index.config.Processor.ProcessorRuleConfig;
import org.havenask.engine.util.JsonPrettyFormatter;

/**
 * data table config
 */
public class DataTable {
    public List<String> data_descriptions = new ArrayList<>();
    public List<ProcessorChainConfig> processor_chain_config = new ArrayList<>();
    public ProcessorConfig processor_config = new ProcessorConfig();
    public ProcessorRuleConfig processor_rule_config = new ProcessorRuleConfig();

    @Override
    public String toString() {
        return JsonPrettyFormatter.toJsonString(this);
    }
}
