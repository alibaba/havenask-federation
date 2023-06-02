package com.alibaba.search.common.arpc.util;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.Map;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

public class MutexPoolTest {

    @Test
    public void testMutex() throws Exception {

        MutexPool<String> mutexPool = new MutexPool<>();
        Field field = MutexPool.class.getDeclaredField("pool");
        field.setAccessible(true);
        Map<Mutex<String>, WeakReference<Mutex<String>>> pool = (Map<Mutex<String>, WeakReference<Mutex<String>>>) field
            .get(mutexPool);
        Mutex<String> mutex = mutexPool.getMutex(new String("123"));
        Assert.assertThat(mutex, CoreMatchers.notNullValue());
        Assert.assertThat(pool.size(), CoreMatchers.equalTo(1));
        Assert.assertThat(pool.get(mutex).get() == mutex, CoreMatchers.equalTo(true));

        Mutex<String> mutex2 = mutexPool.getMutex(new String("123"));
        Assert.assertThat(mutex2 == mutex, CoreMatchers.equalTo(true));
        Assert.assertThat(pool.size(), CoreMatchers.equalTo(1));
        Assert.assertThat(pool.get(mutex2).get() == mutex, CoreMatchers.equalTo(true));

        String s = new String("1234");
        Mutex<String> mutex3 = mutexPool.getMutex(s);
        Assert.assertThat(mutex.equals(mutex3), CoreMatchers.equalTo(false));
        Assert.assertThat(pool.size(), CoreMatchers.equalTo(2));
        Assert.assertThat(pool.get(mutex3).get() == mutex3, CoreMatchers.equalTo(true));

        String cnostString = "1234";
        Assert.assertThat(s == cnostString, CoreMatchers.equalTo(false));
        Mutex<String> mutex4 = mutexPool.getMutex("1234");
        Assert.assertThat(mutex3 == mutex4, CoreMatchers.equalTo(true));
        Assert.assertThat(pool.size(), CoreMatchers.equalTo(2));
        Assert.assertThat(pool.get(mutex4).get() == mutex4, CoreMatchers.equalTo(true));

        Mutex<String> mutex5 = mutexPool.getMutex("12345");
        Assert.assertThat(pool.size(), CoreMatchers.equalTo(3));
        Assert.assertThat(pool.get(mutex5).get() == mutex5, CoreMatchers.equalTo(true));

        mutex3 = null;
        mutex4 = null;
        // 强制gc，mutex3不再被引用，会被回收
        System.gc();
        WeakReference<Mutex<String>> ref = pool.get(new Mutex<>("1234"));
        Assert.assertThat(ref == null || ref.get() == null, CoreMatchers.equalTo(true));
        ref = pool.get(new Mutex<>("123"));
        Assert.assertThat(ref != null && ref.get() == mutex, CoreMatchers.equalTo(true));

        mutex5 = null;
        // 强制gc，mutex5不再被引用，会被回收
        System.gc();
        ref = pool.get(new Mutex<>("12345"));
        Assert.assertThat(ref == null || ref.get() == null, CoreMatchers.equalTo(true));
    }
}
