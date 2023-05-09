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

package org.havenask.repositories.azure;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.havenask.action.ActionRunnable;
import org.havenask.action.support.PlainActionFuture;
import org.havenask.action.support.master.AcknowledgedResponse;
import org.havenask.common.Strings;
import org.havenask.common.collect.Tuple;
import org.havenask.common.settings.MockSecureSettings;
import org.havenask.common.settings.SecureSettings;
import org.havenask.common.settings.Settings;
import org.havenask.plugins.Plugin;
import org.havenask.repositories.AbstractThirdPartyRepositoryTestCase;
import org.havenask.repositories.blobstore.BlobStoreRepository;

import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AzureStorageCleanupThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(AzureRepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        final String endpoint = System.getProperty("test.azure.endpoint_suffix");
        if (Strings.hasText(endpoint)) {
            return Settings.builder()
                .put(super.nodeSettings())
                .put("azure.client.default.endpoint_suffix", endpoint)
                .build();
        }
        return super.nodeSettings();
    }

    @Override
    protected SecureSettings credentials() {
        assertThat(System.getProperty("test.azure.account"), not(blankOrNullString()));
        final boolean hasSasToken = Strings.hasText(System.getProperty("test.azure.sas_token"));
        if (hasSasToken == false) {
            assertThat(System.getProperty("test.azure.key"), not(blankOrNullString()));
        } else {
            assertThat(System.getProperty("test.azure.key"), blankOrNullString());
        }
        assertThat(System.getProperty("test.azure.container"), not(blankOrNullString()));
        assertThat(System.getProperty("test.azure.base"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.default.account", System.getProperty("test.azure.account"));
        if (hasSasToken) {
            secureSettings.setString("azure.client.default.sas_token", System.getProperty("test.azure.sas_token"));
        } else {
            secureSettings.setString("azure.client.default.key", System.getProperty("test.azure.key"));
        }
        return secureSettings;
    }

    @Override
    protected void createRepository(String repoName) {
        AcknowledgedResponse putRepositoryResponse = client().admin().cluster().preparePutRepository(repoName)
            .setType("azure")
            .setSettings(Settings.builder()
                .put("container", System.getProperty("test.azure.container"))
                .put("base_path", System.getProperty("test.azure.base"))
            ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
        if (Strings.hasText(System.getProperty("test.azure.sas_token"))) {
            ensureSasTokenPermissions();
        }
    }

    private void ensureSasTokenPermissions() {
        final BlobStoreRepository repository = getRepository();
        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        repository.threadPool().generic().execute(ActionRunnable.wrap(future, l -> {
            final AzureBlobStore blobStore = (AzureBlobStore) repository.blobStore();
            final String account = "default";
            final Tuple<CloudBlobClient, Supplier<OperationContext>> client = blobStore.getService().client(account);
            final CloudBlobContainer blobContainer = client.v1().getContainerReference(blobStore.toString());
            try {
                SocketAccess.doPrivilegedException(() -> blobContainer.exists(null, null, client.v2().get()));
                future.onFailure(new RuntimeException(
                    "The SAS token used in this test allowed for checking container existence. This test only supports tokens " +
                        "that grant only the documented permission requirements for the Azure repository plugin."));
            } catch (StorageException e) {
                if (e.getHttpStatusCode() == HttpURLConnection.HTTP_FORBIDDEN) {
                    future.onResponse(null);
                } else {
                    future.onFailure(e);
                }
            }
        }));
        future.actionGet();
    }
}
