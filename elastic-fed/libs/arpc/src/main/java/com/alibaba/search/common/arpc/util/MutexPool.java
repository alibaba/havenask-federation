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
