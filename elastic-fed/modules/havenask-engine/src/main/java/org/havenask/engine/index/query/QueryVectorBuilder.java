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

package org.havenask.engine.index.query;

import org.havenask.action.ActionListener;
import org.havenask.client.Client;
import org.havenask.common.io.stream.VersionedNamedWriteable;
import org.havenask.common.xcontent.ToXContentObject;

/**
 * Provides a mechanism for building a KNN query vector in an asynchronous manner during the rewrite phase
 */
public interface QueryVectorBuilder extends VersionedNamedWriteable, ToXContentObject {

    /**
     * Method for building a vector via the client. This method is called during RerwiteAndFetch.
     * Typical implementation for this method will:
     *  1. call some asynchronous client action
     *  2. Handle failure/success for that action (usually passing failure to the provided listener)
     *  3. Parse the success case and extract the query vector
     *  4. Pass the extracted query vector to the provided listener
     *
     * @param client for performing asynchronous actions against the cluster
     * @param listener listener to accept the created vector
     */
    void buildVector(Client client, ActionListener<float[]> listener);

}
