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

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * mutext pool，用户缓存锁，当锁对象仅被当前pool引用会自动被回收
 */
public class MutexPool<T> {

    private Map<Mutex<T>, WeakReference<Mutex<T>>> pool = new WeakHashMap<Mutex<T>, WeakReference<Mutex<T>>>();

    public Mutex<T> getMutex(T key) {

        if (key == null) {
            throw new NullPointerException();
        }
        Mutex<T> mk = new Mutex<T>(key);
        WeakReference<Mutex<T>> ref = pool.get(mk);
        if (ref != null && ref.get() != null) {
            return ref.get();
        }
        synchronized (pool) {
            ref = pool.get(mk);
            if (ref == null || ref.get() == null) {
                pool.put(mk, new WeakReference<>(mk));
            }
        }

        return pool.get(mk).get();
    }
}
