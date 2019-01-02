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
package com.hellobike.base.tunnel.publisher.hbase;

import com.hellobike.base.tunnel.config.HBaseConfig;
import com.hellobike.base.tunnel.model.ColumnData;
import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.model.EventType;
import com.hellobike.base.tunnel.utils.TimeUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * @author machunxiao create at 2018-11-28
 */
public class HBaseClientTest {

    @Test
    public void test_HBaseClient() {
        HBaseClient client = HBaseClient.getInstance();
        Assert.assertNotNull(client);
    }

    @Test
    public void test_HBaseClient_insert() {
        HBaseClient client = HBaseClient.getInstance();
        String rowKey = "number";
        Event event = new Event();
        event.setSchema("db1");
        event.setTable("go_hbase_score");
        event.setEventType(EventType.INSERT);
        List<ColumnData> dataList = event.getDataList();
        ColumnData cd1 = new ColumnData();
        cd1.setName("number");
        cd1.setValue("1123");
        cd1.setDataType("integer");
        dataList.add(cd1);
        HBaseConfig config = new HBaseConfig();
        config.setFamily("f1");
        config.setQualifier("q1");
        config.setHbaseTable("go_hbase_score");
        config.setPks(Collections.singletonList("number"));
        config.setHbaseKey(Collections.singletonList("number"));
        client.insert(config, event);

        TimeUtils.sleepInMills(5000L);
        client.close();

    }

    @Test
    public void test_HBaseClient_update() {
        HBaseClient client = HBaseClient.getInstance();
        String rowKey = "pk001";
        Event event = new Event();
        HBaseConfig config = new HBaseConfig();
        client.update(config, event);
    }

    @Test
    public void test_HBaseClient_delete() {
        HBaseClient client = HBaseClient.getInstance();
        String rowKey = "pk001";
        Event event = new Event();
        event.setTable("tb1");
        HBaseConfig config = new HBaseConfig();
        client.delete(config, event);

    }

    @Test
    public void test_HBaseClient_get() {
    }
}
