/*
 * Copyright 2018 Shanghai Junzheng Network Technology Co.,Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain CONFIG_NAME copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hellobike.base.tunnel.filter;

import com.hellobike.base.tunnel.model.ColumnData;
import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.model.EventType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author machunxiao 2018-11-07
 */
public class FilterTest {

    @Test
    public void test_ColumnFilter() {
        Event event = new Event();

        ColumnFilter filter = new ColumnFilter(null);
        boolean ret = filter.filter(event);
        Assert.assertTrue(ret);

        filter = new ColumnFilter(new ArrayList<>());
        ret = filter.filter(event);
        Assert.assertTrue(ret);

        filter = new ColumnFilter(new ArrayList<>(Arrays.asList("Tom", "Age")));
        List<ColumnData> columnDataList = new ArrayList<>();
        event.setDataList(columnDataList);
        ret = filter.filter(event);
        Assert.assertFalse(ret);

        ColumnData data = new ColumnData();
        data.setName("om");
        columnDataList.add(data);
        ret = filter.filter(event);
        Assert.assertTrue(ret);

    }

    @Test
    public void test_EventTypeFilter() {
        EventTypeFilter filter = new EventTypeFilter(EventType.DELETE);
        boolean ret = filter.filter(null);
        Assert.assertFalse(ret);

        Event event = new Event();
        event.setEventType(EventType.DELETE);
        ret = filter.filter(event);
        Assert.assertTrue(ret);

        event.setEventType(EventType.INSERT);
        ret = filter.filter(event);
        Assert.assertFalse(ret);
    }

    @Test
    public void test_SchemaFilter() {
        SchemaFilter filter = new SchemaFilter(null);
        boolean ret = filter.filter(null);
        Assert.assertFalse(ret);

        Event event = new Event();
        ret = filter.filter(event);
        Assert.assertFalse(ret);

        filter = new SchemaFilter(null);
        event.setSchema("Tom");
        ret = filter.filter(event);
        Assert.assertTrue(ret);

        filter = new SchemaFilter("Tom");
        event.setSchema("Tom");
        ret = filter.filter(event);
        Assert.assertTrue(ret);

        filter = new SchemaFilter("Tom");
        event.setSchema("Jack");
        ret = filter.filter(event);
        Assert.assertFalse(ret);

    }

    @Test
    public void test_TableNameFilter() {
        TableNameFilter filter = new TableNameFilter(null);
        boolean ret = filter.filter(null);
        Assert.assertFalse(ret);

        Event event = new Event();
        ret = filter.filter(event);
        Assert.assertFalse(ret);

        filter = new TableNameFilter("Tom.*");
        event.setTable("Tom123");
        ret = filter.filter(event);
        Assert.assertTrue(ret);

    }

}
