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

package org.havenask.common.xcontent;

import org.havenask.test.HavenaskTestCase;

import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class XContentTypeTests extends HavenaskTestCase {

    public void testFromJson() throws Exception {
        String mediaType = "application/json";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromJsonUppercase() throws Exception {
        String mediaType = "application/json".toUpperCase(Locale.ROOT);
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromYaml() throws Exception {
        String mediaType = "application/yaml";
        XContentType expectedXContentType = XContentType.YAML;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromSmile() throws Exception {
        String mediaType = "application/smile";
        XContentType expectedXContentType = XContentType.SMILE;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromCbor() throws Exception {
        String mediaType = "application/cbor";
        XContentType expectedXContentType = XContentType.CBOR;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromWildcard() throws Exception {
        String mediaType = "application/*";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromWildcardUppercase() throws Exception {
        String mediaType = "APPLICATION/*";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromRubbish() throws Exception {
        assertThat(XContentType.fromMediaTypeOrFormat(null), nullValue());
        assertThat(XContentType.fromMediaTypeOrFormat(""), nullValue());
        assertThat(XContentType.fromMediaTypeOrFormat("text/plain"), nullValue());
        assertThat(XContentType.fromMediaTypeOrFormat("gobbly;goop"), nullValue());
    }

    public void testVersionedMediaType() throws Exception {
        assertThat(XContentType.fromMediaTypeOrFormat("application/vnd.havenask+json;compatible-with=7"),
            equalTo(XContentType.JSON));
        assertThat(XContentType.fromMediaTypeOrFormat("application/vnd.havenask+yaml;compatible-with=7"),
            equalTo(XContentType.YAML));
        assertThat(XContentType.fromMediaTypeOrFormat("application/vnd.havenask+cbor;compatible-with=7"),
            equalTo(XContentType.CBOR));
        assertThat(XContentType.fromMediaTypeOrFormat("application/vnd.havenask+smile;compatible-with=7"),
            equalTo(XContentType.SMILE));

        assertThat(XContentType.fromMediaTypeOrFormat("application/vnd.havenask+json ;compatible-with=7"),
            equalTo(XContentType.JSON));
        assertThat(XContentType.fromMediaTypeOrFormat("application/vnd.havenask+json ;compatible-with=7;charset=utf-8"),
            equalTo(XContentType.JSON));
    }
}
