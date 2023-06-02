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

package com.alibaba.search.common.arpc.util;

public class Mutex<T> {

    private T val;

    public Mutex(T val) {

        this.val = val;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Mutex<?> mutex = (Mutex<?>) o;

        return val != null ? val.equals(mutex.val) : mutex.val == null;

    }

    @Override
    public int hashCode() {

        return val != null ? val.hashCode() : 0;
    }

    @Override
    public String toString() {

        return String.valueOf(val);
    }

}
