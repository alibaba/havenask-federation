package com.alibaba.search.common.arpc.util;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.exceptions.ArpcException;

public class IpAddressUtilTest extends TestCase {
    private final static Logger logger = LoggerFactory
            .getLogger(IpAddressUtilTest.class);

    public IpAddressUtilTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testEncodeToLongStringInt() throws Exception {
        assertEquals(0, IpAddressUtil.encodeToLong("0.0.0.0", 0));
        assertEquals(-4294967295L,
                IpAddressUtil.encodeToLong("255.255.255.255", 1));
        assertEquals(72623859706101761L,
                IpAddressUtil.encodeToLong("1.2.3.4", 1));
        assertEquals(9223372032559808513L,
                IpAddressUtil.encodeToLong("127.255.255.255", 1));
        assertEquals(-9151314447111815167L,
                IpAddressUtil.encodeToLong("128.255.255.255", 1));
        try {
            IpAddressUtil.encodeToLong("xxzzzx", 1);
            fail();
        } catch (ArpcException e) {
        }
    }
}
