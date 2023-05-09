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

package org.havenask.ingest.common;

import org.havenask.bootstrap.JavaVersion;
import org.havenask.common.time.DateFormatter;
import org.havenask.common.time.DateUtils;
import org.havenask.test.HavenaskTestCase;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.function.Function;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class DateFormatTests extends HavenaskTestCase {

    public void testParseJava() {
        Function<String, ZonedDateTime> javaFunction = DateFormat.Java.getFunction("MMM dd HH:mm:ss Z",
                ZoneOffset.ofHours(-8), Locale.ENGLISH);
        assertThat(javaFunction.apply("Nov 24 01:29:01 -0800").toInstant()
                        .atZone(ZoneId.of("GMT-8"))
                        .format(DateTimeFormatter.ofPattern("MM dd HH:mm:ss", Locale.ENGLISH)),
                equalTo("11 24 01:29:01"));
    }

    public void testParseYearOfEraJavaWithTimeZone() {
        Function<String, ZonedDateTime> javaFunction = DateFormat.Java.getFunction("yyyy-MM-dd'T'HH:mm:ss.SSSZZ",
            ZoneOffset.UTC, Locale.ROOT);
        ZonedDateTime datetime = javaFunction.apply("2018-02-05T13:44:56.657+0100");
        String expectedDateTime = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneOffset.UTC).format(datetime);
        assertThat(expectedDateTime, is("2018-02-05T12:44:56.657Z"));
    }

    public void testParseYearJavaWithTimeZone() {
        Function<String, ZonedDateTime> javaFunction = DateFormat.Java.getFunction("uuuu-MM-dd'T'HH:mm:ss.SSSZZ",
            ZoneOffset.UTC, Locale.ROOT);
        ZonedDateTime datetime = javaFunction.apply("2018-02-05T13:44:56.657+0100");
        String expectedDateTime = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneOffset.UTC).format(datetime);
        assertThat(expectedDateTime, is("2018-02-05T12:44:56.657Z"));
    }

    public void testParseJavaDefaultYear() {
        String format = randomFrom("8dd/MM", "dd/MM");
        ZoneId timezone = DateUtils.of("Europe/Amsterdam");
        Function<String, ZonedDateTime> javaFunction = DateFormat.Java.getFunction(format, timezone, Locale.ENGLISH);
        int year = ZonedDateTime.now(ZoneOffset.UTC).getYear();
        ZonedDateTime dateTime = javaFunction.apply("12/06");
        assertThat(dateTime.getYear(), is(year));
    }

    public void testParseWeekBased() {
        assumeFalse("won't work in jdk8 " +
                "because SPI mechanism is not looking at classpath - needs ISOCalendarDataProvider in jre's ext/libs",
            JavaVersion.current().equals(JavaVersion.parse("8")));
        String format = randomFrom("YYYY-ww");
        ZoneId timezone = DateUtils.of("Europe/Amsterdam");
        Function<String, ZonedDateTime> javaFunction = DateFormat.Java.getFunction(format, timezone, Locale.ROOT);
        ZonedDateTime dateTime = javaFunction.apply("2020-33");
        assertThat(dateTime, equalTo(ZonedDateTime.of(2020,8,10,0,0,0,0,timezone)));
    }

    public void testParseWeekBasedWithLocale() {
        assumeFalse("won't work in jdk8 " +
                "because SPI mechanism is not looking at classpath - needs ISOCalendarDataProvider in jre's ext/libs",
            JavaVersion.current().equals(JavaVersion.parse("8")));
        String format = randomFrom("YYYY-ww");
        ZoneId timezone = DateUtils.of("Europe/Amsterdam");
        Function<String, ZonedDateTime> javaFunction = DateFormat.Java.getFunction(format, timezone, Locale.US);
        ZonedDateTime dateTime = javaFunction.apply("2020-33");
        //33rd week of 2020 starts on 9th August 2020 as per US locale
        assertThat(dateTime, equalTo(ZonedDateTime.of(2020,8,9,0,0,0,0,timezone)));
    }

    public void testParseUnixMs() {
        assertThat(DateFormat.UnixMs.getFunction(null, ZoneOffset.UTC, null).apply("1000500").toInstant().toEpochMilli(),
            equalTo(1000500L));
    }

    public void testParseUnix() {
        assertThat(DateFormat.Unix.getFunction(null, ZoneOffset.UTC, null).apply("1000.5").toInstant().toEpochMilli(),
            equalTo(1000500L));
    }

    public void testParseUnixWithMsPrecision() {
        assertThat(DateFormat.Unix.getFunction(null, ZoneOffset.UTC, null).apply("1495718015").toInstant().toEpochMilli(),
            equalTo(1495718015000L));
    }

    public void testParseISO8601() {
        assertThat(DateFormat.Iso8601.getFunction(null, ZoneOffset.UTC, null).apply("2001-01-01T00:00:00-0800")
                                     .toInstant().toEpochMilli(), equalTo(978336000000L));
        assertThat(DateFormat.Iso8601.getFunction(null, ZoneOffset.UTC, null).apply("2001-01-01T00:00:00-0800").toString(),
                equalTo("2001-01-01T08:00Z"));
    }

    public void testParseWhenZoneNotPresentInText() {
        assertThat(DateFormat.Iso8601.getFunction(null, ZoneOffset.of("+0100"), null).apply("2001-01-01T00:00:00")
                                     .toInstant().toEpochMilli(), equalTo(978303600000L));
        assertThat(DateFormat.Iso8601.getFunction(null, ZoneOffset.of("+0100"), null).apply("2001-01-01T00:00:00").toString(),
            equalTo("2001-01-01T00:00+01:00"));
    }

    public void testParseISO8601Failure() {
        Function<String, ZonedDateTime> function = DateFormat.Iso8601.getFunction(null, ZoneOffset.UTC, null);
        try {
            function.apply("2001-01-0:00-0800");
            fail("parse should have failed");
        } catch(IllegalArgumentException e) {
            //all good
        }
    }

    public void testTAI64NParse() {
        String input = "4000000050d506482dbdf024";
        String expected = "2012-12-22T03:00:46.767+02:00";
        assertThat(DateFormat.Tai64n.getFunction(null, ZoneOffset.ofHours(2), null)
                .apply((randomBoolean() ? "@" : "") + input).toString(), equalTo(expected));
    }

    public void testFromString() {
        assertThat(DateFormat.fromString("UNIX_MS"), equalTo(DateFormat.UnixMs));
        assertThat(DateFormat.fromString("unix_ms"), equalTo(DateFormat.Java));
        assertThat(DateFormat.fromString("UNIX"), equalTo(DateFormat.Unix));
        assertThat(DateFormat.fromString("unix"), equalTo(DateFormat.Java));
        assertThat(DateFormat.fromString("ISO8601"), equalTo(DateFormat.Iso8601));
        assertThat(DateFormat.fromString("iso8601"), equalTo(DateFormat.Java));
        assertThat(DateFormat.fromString("TAI64N"), equalTo(DateFormat.Tai64n));
        assertThat(DateFormat.fromString("tai64n"), equalTo(DateFormat.Java));
        assertThat(DateFormat.fromString("prefix-" + randomAlphaOfLengthBetween(1, 10)), equalTo(DateFormat.Java));
    }
}
