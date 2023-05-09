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

import org.havenask.action.ActionRequestValidationException;
import org.havenask.common.io.stream.Writeable.Reader;
import org.havenask.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.equalTo;

public class HavenaskSqlRequestTests extends AbstractWireSerializingTestCase<HavenaskSqlRequest> {

    @Override
    protected Reader<HavenaskSqlRequest> instanceReader() {
        return HavenaskSqlRequest::new;
    }

    @Override
    protected HavenaskSqlRequest createTestInstance() {
        return new HavenaskSqlRequest("select * from test", null);
    }

    public void testValidateRequest() {
        HavenaskSqlRequest req = new HavenaskSqlRequest("select * from test", "kvpair");
        ActionRequestValidationException e = req.validate();
        assertNull(e);
    }

    public void testValidateRequestWithoutName() {
        HavenaskSqlRequest req = new HavenaskSqlRequest(null, "kvpair");
        ActionRequestValidationException e = req.validate();
        assertNotNull(e);
        assertThat(e.validationErrors().size(), equalTo(1));
        assertThat(e.validationErrors().get(0), equalTo("sql is null"));
    }
}
