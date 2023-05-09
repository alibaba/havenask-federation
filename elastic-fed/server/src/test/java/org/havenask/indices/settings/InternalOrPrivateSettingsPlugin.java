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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.havenask.indices.settings;

import org.havenask.action.ActionListener;
import org.havenask.action.ActionRequest;
import org.havenask.action.ActionRequestValidationException;
import org.havenask.action.ActionResponse;
import org.havenask.action.ActionType;
import org.havenask.action.support.ActionFilters;
import org.havenask.action.support.master.MasterNodeRequest;
import org.havenask.action.support.master.TransportMasterNodeAction;
import org.havenask.cluster.ClusterState;
import org.havenask.cluster.ClusterStateUpdateTask;
import org.havenask.cluster.block.ClusterBlockException;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.cluster.metadata.IndexNameExpressionResolver;
import org.havenask.cluster.metadata.Metadata;
import org.havenask.cluster.service.ClusterService;
import org.havenask.common.inject.Inject;
import org.havenask.common.io.stream.StreamInput;
import org.havenask.common.io.stream.StreamOutput;
import org.havenask.common.settings.Setting;
import org.havenask.common.settings.Settings;
import org.havenask.plugins.ActionPlugin;
import org.havenask.plugins.Plugin;
import org.havenask.threadpool.ThreadPool;
import org.havenask.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class InternalOrPrivateSettingsPlugin extends Plugin implements ActionPlugin {

    static final Setting<String> INDEX_INTERNAL_SETTING =
            Setting.simpleString("index.internal", Setting.Property.IndexScope, Setting.Property.InternalIndex);

    static final Setting<String> INDEX_PRIVATE_SETTING =
            Setting.simpleString("index.private", Setting.Property.IndexScope, Setting.Property.PrivateIndex);

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(INDEX_INTERNAL_SETTING, INDEX_PRIVATE_SETTING);
    }

    public static class UpdateInternalOrPrivateAction extends ActionType<UpdateInternalOrPrivateAction.Response> {

        public static final UpdateInternalOrPrivateAction INSTANCE = new UpdateInternalOrPrivateAction();
        private static final String NAME = "indices:admin/settings/update-internal-or-private-index";

        public UpdateInternalOrPrivateAction() {
            super(NAME, UpdateInternalOrPrivateAction.Response::new);
        }

        public static class Request extends MasterNodeRequest<Request> {

            private String index;
            private String key;
            private String value;

            Request() {}

            Request(StreamInput in) throws IOException {
                super(in);
                index = in.readString();
                key = in.readString();
                value = in.readString();
            }

            public Request(final String index, final String key, final String value) {
                this.index = index;
                this.key = key;
                this.value = value;
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public void writeTo(final StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeString(index);
                out.writeString(key);
                out.writeString(value);
            }

        }

        static class Response extends ActionResponse {
            Response() {}

            Response(StreamInput in) throws IOException {
                super(in);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {}
        }

    }

    public static class TransportUpdateInternalOrPrivateAction
            extends TransportMasterNodeAction<UpdateInternalOrPrivateAction.Request, UpdateInternalOrPrivateAction.Response> {

        @Inject
        public TransportUpdateInternalOrPrivateAction(
                final TransportService transportService,
                final ClusterService clusterService,
                final ThreadPool threadPool,
                final ActionFilters actionFilters,
                final IndexNameExpressionResolver indexNameExpressionResolver) {
            super(
                    UpdateInternalOrPrivateAction.NAME,
                    transportService,
                    clusterService,
                    threadPool,
                    actionFilters,
                    UpdateInternalOrPrivateAction.Request::new,
                    indexNameExpressionResolver);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected UpdateInternalOrPrivateAction.Response read(StreamInput in) throws IOException {
            return new UpdateInternalOrPrivateAction.Response(in);
        }

        @Override
        protected void masterOperation(
                final UpdateInternalOrPrivateAction.Request request,
                final ClusterState state,
                final ActionListener<UpdateInternalOrPrivateAction.Response> listener) throws Exception {
            clusterService.submitStateUpdateTask("update-index-internal-or-private", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(final ClusterState currentState) throws Exception {
                    final Metadata.Builder builder = Metadata.builder(currentState.metadata());
                    final IndexMetadata.Builder imdBuilder = IndexMetadata.builder(currentState.metadata().index(request.index));
                    final Settings.Builder settingsBuilder =
                            Settings.builder()
                                    .put(currentState.metadata().index(request.index).getSettings())
                                    .put(request.key, request.value);
                    imdBuilder.settings(settingsBuilder);
                    imdBuilder.settingsVersion(1 + imdBuilder.settingsVersion());
                    builder.put(imdBuilder.build(), true);
                    return ClusterState.builder(currentState).metadata(builder).build();
                }

                @Override
                public void clusterStateProcessed(final String source, final ClusterState oldState, final ClusterState newState) {
                    listener.onResponse(new UpdateInternalOrPrivateAction.Response());
                }

                @Override
                public void onFailure(final String source, final Exception e) {
                    listener.onFailure(e);
                }

            });
        }

        @Override
        protected ClusterBlockException checkBlock(final UpdateInternalOrPrivateAction.Request request, final ClusterState state) {
            return null;
        }

    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Collections.singletonList(
                new ActionHandler<>(UpdateInternalOrPrivateAction.INSTANCE, TransportUpdateInternalOrPrivateAction.class));
    }

}
