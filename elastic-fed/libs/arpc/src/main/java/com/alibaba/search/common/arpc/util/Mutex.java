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
