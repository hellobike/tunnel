package com.hellobike.base.tunnel.spi.api;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author machunxiao create at 2019-01-04
 */
public class UtilsTest {

    @Test
    public void test_collection() {

        Assert.assertEquals(true, CollectionUtils.isEmpty(new ArrayList<>()));
        Assert.assertEquals(true, CollectionUtils.isEmpty(null));
        Assert.assertEquals(true, CollectionUtils.isNotEmpty(Collections.singletonList("a")));

    }


    @Test
    public void test_map() {
        Assert.assertEquals(true, MapUtils.isEmpty(new HashMap<>()));
        Assert.assertEquals(true, MapUtils.isEmpty(null));
        HashMap<String, String> map = new HashMap<>();
        map.put("a", "b");
        Assert.assertEquals(true, MapUtils.isNotEmpty(map));

        Assert.assertEquals(null, MapUtils.get(null, ""));
        Assert.assertEquals("b", MapUtils.get(map, "a"));
    }

}
