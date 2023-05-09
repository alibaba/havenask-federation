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

package org.havenask.index.mapper;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.util.BytesRef;
import org.havenask.Version;
import org.havenask.cluster.metadata.IndexMetadata;
import org.havenask.common.network.InetAddresses;
import org.havenask.common.settings.Settings;
import org.havenask.index.mapper.FieldTypeTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;

public class IpFieldTypeTests extends FieldTypeTestCase {

    public void testValueFormat() throws Exception {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");
        String ip = "2001:db8::2:1";
        BytesRef asBytes = new BytesRef(InetAddressPoint.encode(InetAddress.getByName(ip)));
        assertEquals(ip, ft.docValueFormat(null, null).format(asBytes));

        ip = "192.168.1.7";
        asBytes = new BytesRef(InetAddressPoint.encode(InetAddress.getByName(ip)));
        assertEquals(ip, ft.docValueFormat(null, null).format(asBytes));
    }

    public void testValueForSearch() {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");
        String ip = "2001:db8::2:1";
        BytesRef asBytes = new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ip)));
        assertEquals(ip, ft.valueForDisplay(asBytes));

        ip = "192.168.1.7";
        asBytes = new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ip)));
        assertEquals(ip, ft.valueForDisplay(asBytes));
    }

    public void testTermQuery() {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");

        String ip = "2001:db8::2:1";
        assertEquals(InetAddressPoint.newExactQuery("field", InetAddresses.forString(ip)), ft.termQuery(ip, null));

        ip = "192.168.1.7";
        assertEquals(InetAddressPoint.newExactQuery("field", InetAddresses.forString(ip)), ft.termQuery(ip, null));

        ip = "2001:db8::2:1";
        String prefix = ip + "/64";
        assertEquals(InetAddressPoint.newPrefixQuery("field", InetAddresses.forString(ip), 64), ft.termQuery(prefix, null));

        ip = "192.168.1.7";
        prefix = ip + "/16";
        assertEquals(InetAddressPoint.newPrefixQuery("field", InetAddresses.forString(ip), 16), ft.termQuery(prefix, null));

        MappedFieldType unsearchable = new IpFieldMapper.IpFieldType("field", false, false, true, null, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> unsearchable.termQuery("::1", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testTermsQuery() {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");

        assertEquals(InetAddressPoint.newSetQuery("field", InetAddresses.forString("::2"), InetAddresses.forString("::5")),
                ft.termsQuery(Arrays.asList(InetAddresses.forString("::2"), InetAddresses.forString("::5")), null));
        assertEquals(InetAddressPoint.newSetQuery("field", InetAddresses.forString("::2"), InetAddresses.forString("::5")),
                ft.termsQuery(Arrays.asList("::2", "::5"), null));

        // if the list includes a prefix query we fallback to a bool query
        assertEquals(new ConstantScoreQuery(new BooleanQuery.Builder()
                .add(ft.termQuery("::42", null), Occur.SHOULD)
                .add(ft.termQuery("::2/16", null), Occur.SHOULD).build()),
                ft.termsQuery(Arrays.asList("::42", "::2/16"), null));
    }

    public void testRangeQuery() {
        MappedFieldType ft = new IpFieldMapper.IpFieldType("field");

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("::"),
                        InetAddressPoint.MAX_VALUE),
                ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("::"),
                        InetAddresses.forString("192.168.2.0")),
                ft.rangeQuery(null, "192.168.2.0", randomBoolean(), true, null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("::"),
                        InetAddresses.forString("192.168.1.255")),
                ft.rangeQuery(null, "192.168.2.0", randomBoolean(), false, null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("2001:db8::"),
                        InetAddressPoint.MAX_VALUE),
                ft.rangeQuery("2001:db8::", null, true, randomBoolean(), null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("2001:db8::1"),
                        InetAddressPoint.MAX_VALUE),
                ft.rangeQuery("2001:db8::", null, false, randomBoolean(), null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("2001:db8::"),
                        InetAddresses.forString("2001:db8::ffff")),
                ft.rangeQuery("2001:db8::", "2001:db8::ffff", true, true, null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("2001:db8::1"),
                        InetAddresses.forString("2001:db8::fffe")),
                ft.rangeQuery("2001:db8::", "2001:db8::ffff", false, false, null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("2001:db8::2"),
                        InetAddresses.forString("2001:db8::")),
                // same lo/hi values but inclusive=false so this won't match anything
                ft.rangeQuery("2001:db8::1", "2001:db8::1", false, false, null, null, null, null));

        // Upper bound is the min IP and is not inclusive
        assertEquals(new MatchNoDocsQuery(),
                ft.rangeQuery("::", "::", true, false, null, null, null, null));

        // Lower bound is the max IP and is not inclusive
        assertEquals(new MatchNoDocsQuery(),
                ft.rangeQuery("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                        false, true, null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("::"),
                        InetAddresses.forString("::fffe:ffff:ffff")),
                // same lo/hi values but inclusive=false so this won't match anything
                ft.rangeQuery("::", "0.0.0.0", true, false, null, null, null, null));

        assertEquals(
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("::1:0:0:0"),
                        InetAddressPoint.MAX_VALUE),
                // same lo/hi values but inclusive=false so this won't match anything
                ft.rangeQuery("255.255.255.255", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", false, true, null, null, null, null));

        assertEquals(
                // lower bound is ipv4, upper bound is ipv6
                InetAddressPoint.newRangeQuery("field",
                        InetAddresses.forString("192.168.1.7"),
                        InetAddresses.forString("2001:db8::")),
                ft.rangeQuery("::ffff:c0a8:107", "2001:db8::", true, true, null, null, null, null));

        MappedFieldType unsearchable = new IpFieldMapper.IpFieldType("field", false, false, true, null, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> unsearchable.rangeQuery("::1", "2001::", true, true, null, null, null, null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        MappedFieldType mapper
            = new IpFieldMapper.Builder("field", true, Version.CURRENT).build(context).fieldType();
        assertEquals(Collections.singletonList("2001:db8::2:1"), fetchSourceValue(mapper, "2001:db8::2:1"));
        assertEquals(Collections.singletonList("2001:db8::2:1"), fetchSourceValue(mapper, "2001:db8:0:0:0:0:2:1"));
        assertEquals(Collections.singletonList("::1"), fetchSourceValue(mapper, "0:0:0:0:0:0:0:1"));

        MappedFieldType nullValueMapper = new IpFieldMapper.Builder("field", true, Version.CURRENT)
            .nullValue("2001:db8:0:0:0:0:2:7")
            .build(context)
            .fieldType();
        assertEquals(Collections.singletonList("2001:db8::2:7"), fetchSourceValue(nullValueMapper, null));
    }
}
