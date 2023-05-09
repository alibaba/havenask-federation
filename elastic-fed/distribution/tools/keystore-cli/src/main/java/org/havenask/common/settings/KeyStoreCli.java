/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.common.settings;

import org.havenask.cli.LoggingAwareMultiCommand;
import org.havenask.cli.Terminal;

/**
 * A cli tool for managing secrets in the havenask keystore.
 */
public class KeyStoreCli extends LoggingAwareMultiCommand {

    private KeyStoreCli() {
        super("A tool for managing settings stored in the havenask keystore");
        subcommands.put("create", new CreateKeyStoreCommand());
        subcommands.put("list", new ListKeyStoreCommand());
        subcommands.put("add", new AddStringKeyStoreCommand());
        subcommands.put("add-file", new AddFileKeyStoreCommand());
        subcommands.put("remove", new RemoveSettingKeyStoreCommand());
        subcommands.put("upgrade", new UpgradeKeyStoreCommand());
        subcommands.put("passwd", new ChangeKeyStorePasswordCommand());
        subcommands.put("has-passwd", new HasPasswordKeyStoreCommand());
    }

    public static void main(String[] args) throws Exception {
        exit(new KeyStoreCli().main(args, Terminal.DEFAULT));
    }

}
