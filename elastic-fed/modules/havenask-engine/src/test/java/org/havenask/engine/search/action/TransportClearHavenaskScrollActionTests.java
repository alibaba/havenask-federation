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

package org.havenask.engine.search.action;

import org.havenask.test.HavenaskTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.havenask.engine.search.action.TransportClearHavenaskScrollAction.separateElasticScrollIdAndHavenaskScrollId;

public class TransportClearHavenaskScrollActionTests extends HavenaskTestCase {
    public void testSeparateElasticScrollIdAndHavenaskScrollId() {
        // test clear all scrollIds
        {
            List<String> scrollIds = new ArrayList<>();
            scrollIds.add("_all");

            List<String> elasticScrollIds = new ArrayList<>();
            List<String> havenaskScrollIds = new ArrayList<>();

            separateElasticScrollIdAndHavenaskScrollId(scrollIds, elasticScrollIds, havenaskScrollIds);
            assertEquals(1, elasticScrollIds.size());
            assertEquals("_all", elasticScrollIds.get(0));
            assertEquals(1, havenaskScrollIds.size());
            assertEquals("_all", havenaskScrollIds.get(0));
        }

        // test separate elastic scrollId and havenask scrollId
        {
            List<String> scrollIds = new ArrayList<>();

            // es scroll id
            scrollIds.add(
                "FGluY2x1ZGVfY29udGV4dF91dWlkDXF1ZXJ5QW5kRmV0Y2gBFmh0YnM5eTJyU2h"
                    + "PbUgxQm05YWg1T1EAAAAAAAAAFhZpR204NkpQclE0eWFaWXE1MU16cVNn"
            );

            // havenask scroll ids
            scrollIds.add("EmhhdmVuYXNrX3Njcm9sbF9pZBZpR204NkpQclE0eWFaWXE1MU16cVNnFms3a3VETW5IU05PN3lQSlh1SDUxYXc=");
            scrollIds.add("EmhhdmVuYXNrX3Njcm9sbF9pZBZpWGMzN0pQclQ4eWFQRWM0N1FlYVNnFnU4cXNQV25RUFNPN3lQSkhhaFc3YXc=");

            List<String> elasticScrollIds = new ArrayList<>();
            List<String> havenaskScrollIds = new ArrayList<>();
            separateElasticScrollIdAndHavenaskScrollId(scrollIds, elasticScrollIds, havenaskScrollIds);
            assertEquals(1, elasticScrollIds.size());
            assertEquals(2, havenaskScrollIds.size());
        }
    }
}
