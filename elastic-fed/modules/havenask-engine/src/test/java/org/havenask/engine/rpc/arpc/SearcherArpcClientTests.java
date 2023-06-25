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

package org.havenask.engine.rpc.arpc;


import org.havenask.engine.index.engine.HashAlgorithm;
import org.havenask.engine.rpc.WriteRequest;
import org.havenask.engine.rpc.WriteResponse;
import org.havenask.index.engine.Engine;
import org.havenask.test.HavenaskTestCase;

import java.util.HashMap;
import java.util.Map;

public class SearcherArpcClientTests extends HavenaskTestCase {
    /**
     * repeat 100 times of write request, need to create corresponding index before running the test
     */
    public void testSearcherArpcClient() {
        String indexName = "product_havenask";

        SearcherArpcClient searcherArpcClient = new SearcherArpcClient(39300);

        for (int i = 1; i <= 100; i ++ ) {
            String doc_id = Integer.toString(80 + i);
            Map<String, String> haDoc = new HashMap<>();
            haDoc.put("_id", doc_id);
            haDoc.put("productName", "TV");
            haDoc.put("annual_rate", "0.6");
            WriteRequest writeRequest = buildWriteRequest(indexName, doc_id, Engine.Operation.TYPE.INDEX, haDoc);
            WriteResponse writeResponse = searcherArpcClient.write(writeRequest);
            System.out.println(writeResponse.getCheckpoint());
            System.out.println(writeResponse.getErrorCode());
            System.out.println(writeResponse.getErrorMessage());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        searcherArpcClient.close();
    }


    static WriteRequest buildWriteRequest(String table, String id, Engine.Operation.TYPE type, Map<String, String> haDoc) {
        StringBuilder message = new StringBuilder();
        switch (type) {
            case INDEX:
                message.append("CMD=add\u001F\n");
                break;
            case DELETE:
                message.append("CMD=delete\u001F\n");
                break;
            default:
                throw new IllegalArgumentException("invalid operation type!");
        }

        for (Map.Entry<String, String> entry : haDoc.entrySet()) {
            message.append(entry.getKey()).append("=").append(entry.getValue()).append("\u001F\n");
        }
        message.append("\u001E\n");
        long hashId = HashAlgorithm.getHashId(id);
        return new WriteRequest(table, (int) hashId, message.toString());
    }
}
