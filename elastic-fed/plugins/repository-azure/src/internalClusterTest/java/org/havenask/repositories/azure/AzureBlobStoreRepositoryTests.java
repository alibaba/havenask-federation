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

import com.microsoft.azure.storage.Constants;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.RetryPolicyFactory;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import fixture.azure.AzureHttpHandler;

import org.havenask.common.SuppressForbidden;
import org.havenask.common.regex.Regex;
import org.havenask.common.settings.MockSecureSettings;
import org.havenask.common.settings.Settings;
import org.havenask.common.unit.ByteSizeUnit;
import org.havenask.plugins.Plugin;
import org.havenask.repositories.blobstore.HavenaskMockAPIBasedRepositoryIntegTestCase;
import org.havenask.rest.RestStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
public class AzureBlobStoreRepositoryTests extends HavenaskMockAPIBasedRepositoryIntegTestCase {

    @Override
    protected String repositoryType() {
        return AzureRepository.TYPE;
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(super.repositorySettings())
            .put(AzureRepository.Repository.CONTAINER_SETTING.getKey(), "container")
            .put(AzureStorageSettings.ACCOUNT_SETTING.getKey(), "test")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestAzureRepositoryPlugin.class);
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Collections.singletonMap("/container", new AzureHTTPStatsCollectorHandler(new AzureBlobStoreHttpHandler("container")));
    }

    @Override
    protected HttpHandler createErroneousHttpHandler(final HttpHandler delegate) {
        return new AzureErroneousHttpHandler(delegate, randomIntBetween(2, 3));
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(10).getBytes(StandardCharsets.UTF_8));
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(AzureStorageSettings.ACCOUNT_SETTING.getConcreteSettingForNamespace("test").getKey(), "account");
        secureSettings.setString(AzureStorageSettings.KEY_SETTING.getConcreteSettingForNamespace("test").getKey(), key);

        final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + httpServerUrl();
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(AzureStorageSettings.ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace("test").getKey(), endpoint)
            .setSecureSettings(secureSettings)
            .build();
    }

    /**
     * AzureRepositoryPlugin that allows to set low values for the Azure's client retry policy
     * and for BlobRequestOptions#getSingleBlobPutThresholdInBytes().
     */
    public static class TestAzureRepositoryPlugin extends AzureRepositoryPlugin {

        public TestAzureRepositoryPlugin(Settings settings) {
            super(settings);
        }

        @Override
        AzureStorageService createAzureStoreService(final Settings settings) {
            return new AzureStorageService(settings) {
                @Override
                RetryPolicyFactory createRetryPolicy(final AzureStorageSettings azureStorageSettings) {
                    return new RetryExponentialRetry(1, 100, 500, azureStorageSettings.getMaxRetries());
                }

                @Override
                BlobRequestOptions getBlobRequestOptionsForWriteBlob() {
                    BlobRequestOptions options = new BlobRequestOptions();
                    options.setSingleBlobPutThresholdInBytes(Math.toIntExact(ByteSizeUnit.MB.toBytes(1)));
                    return options;
                }
            };
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpHandler to emulate an Azure endpoint")
    private static class AzureBlobStoreHttpHandler extends AzureHttpHandler implements BlobStoreHttpHandler {

        AzureBlobStoreHttpHandler(final String container) {
            super(container);
        }
    }

    /**
     * HTTP handler that injects random Azure service errors
     *
     * Note: it is not a good idea to allow this handler to simulate too many errors as it would
     * slow down the test suite.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
    private static class AzureErroneousHttpHandler extends ErroneousHttpHandler {

        AzureErroneousHttpHandler(final HttpHandler delegate, final int maxErrorsPerRequest) {
            super(delegate, maxErrorsPerRequest);
        }

        @Override
        protected void handleAsError(final HttpExchange exchange) throws IOException {
            try {
                drainInputStream(exchange.getRequestBody());
                AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
            } finally {
                exchange.close();
            }
        }

        @Override
        protected String requestUniqueId(final HttpExchange exchange) {
            final String requestId = exchange.getRequestHeaders().getFirst(Constants.HeaderConstants.CLIENT_REQUEST_ID_HEADER);
            final String range = exchange.getRequestHeaders().getFirst(Constants.HeaderConstants.STORAGE_RANGE_HEADER);
            return exchange.getRequestMethod()
                + " " + requestId
                + (range != null ? " " + range : "");
        }
    }

    /**
     * HTTP handler that keeps track of requests performed against Azure Storage.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
    private static class AzureHTTPStatsCollectorHandler extends HttpStatsCollectorHandler {

        private static final Pattern listPattern = Pattern.compile("GET /[a-zA-Z0-9]+\\??.+");

        private AzureHTTPStatsCollectorHandler(HttpHandler delegate) {
            super(delegate);
        }

        @Override
        protected void maybeTrack(String request, Headers headers) {
            if (Regex.simpleMatch("GET /*/*", request)) {
                trackRequest("GetBlob");
            } else if (Regex.simpleMatch("HEAD /*/*", request)) {
                trackRequest("GetBlobProperties");
            } else if (listPattern.matcher(request).matches()) {
                trackRequest("ListBlobs");
            } else if (isPutBlock(request)) {
                trackRequest("PutBlock");
            } else if (isPutBlockList(request)) {
                trackRequest("PutBlockList");
            } else if (Regex.simpleMatch("PUT /*/*", request)) {
                trackRequest("PutBlob");
            }
        }

        // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block
        private boolean isPutBlock(String request) {
            return Regex.simpleMatch("PUT /*/*?*comp=block*", request) && request.contains("blockid=");
        }

        // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list
        private boolean isPutBlockList(String request) {
            return Regex.simpleMatch("PUT /*/*?*comp=blocklist*", request);
        }
    }
}
