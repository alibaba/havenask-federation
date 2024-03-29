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

package org.havenask.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DateHelper {
    private static final Logger logger = LogManager.getLogger(DateHelper.class);

    public static Date convertStringToDate(String dateString) {
        String dateRegex = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat sdf = new SimpleDateFormat(dateRegex);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date returnDate = null;
        try {
            returnDate = sdf.parse(dateString);
        } catch (ParseException e) {
            logger.error("convert String to Date type error", e);
        }
        return returnDate;
    }
}
