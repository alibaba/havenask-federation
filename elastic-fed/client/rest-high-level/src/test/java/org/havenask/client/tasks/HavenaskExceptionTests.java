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

package org.havenask.client.tasks;

import org.havenask.client.AbstractResponseTestCase;
import org.havenask.common.xcontent.XContentParser;
import org.havenask.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;

public class HavenaskExceptionTests extends AbstractResponseTestCase<org.havenask.HavenaskException,
    org.havenask.client.tasks.HavenaskException> {

    @Override
    protected org.havenask.HavenaskException createServerTestInstance(XContentType xContentType) {
        IllegalStateException ies = new IllegalStateException("illegal_state");
        IllegalArgumentException iae = new IllegalArgumentException("argument", ies);
        org.havenask.HavenaskException exception = new org.havenask.HavenaskException("elastic_exception", iae);
        exception.addHeader("key","value");
        exception.addMetadata("havenask.meta","data");
        exception.addSuppressed(new NumberFormatException("3/0"));
        return exception;
    }

    @Override
    protected HavenaskException doParseToClientInstance(XContentParser parser) throws IOException {
        parser.nextToken();
        return HavenaskException.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.havenask.HavenaskException serverTestInstance, HavenaskException clientInstance) {

        IllegalArgumentException sCauseLevel1 = (IllegalArgumentException) serverTestInstance.getCause();
        HavenaskException cCauseLevel1 = clientInstance.getCause();

        assertTrue(sCauseLevel1 !=null);
        assertTrue(cCauseLevel1 !=null);

        IllegalStateException causeLevel2 = (IllegalStateException) serverTestInstance.getCause().getCause();
        HavenaskException cCauseLevel2 = clientInstance.getCause().getCause();
        assertTrue(causeLevel2 !=null);
        assertTrue(cCauseLevel2 !=null);


        HavenaskException cause = new HavenaskException(
            "Havenask exception [type=illegal_state_exception, reason=illegal_state]"
        );
        HavenaskException caused1 = new HavenaskException(
            "Havenask exception [type=illegal_argument_exception, reason=argument]",cause
        );
        HavenaskException caused2 = new HavenaskException(
            "Havenask exception [type=exception, reason=elastic_exception]",caused1
        );

        caused2.addHeader("key", Collections.singletonList("value"));
        HavenaskException supp = new HavenaskException(
            "Havenask exception [type=number_format_exception, reason=3/0]"
        );
        caused2.addSuppressed(Collections.singletonList(supp));

        assertEquals(caused2,clientInstance);

    }

}
