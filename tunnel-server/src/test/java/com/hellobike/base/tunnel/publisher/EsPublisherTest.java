/*
 * Copyright 2018 Shanghai Junzheng Network Technology Co.,Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hellobike.base.tunnel.publisher;

import com.hellobike.base.tunnel.config.EsConfig;
import com.hellobike.base.tunnel.filter.IEventFilter;
import com.hellobike.base.tunnel.filter.TableNameFilter;
import com.hellobike.base.tunnel.model.ColumnData;
import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.model.EventType;
import com.hellobike.base.tunnel.model.InvokeContext;
import com.hellobike.base.tunnel.publisher.es.EsPublisher;
import com.hellobike.base.tunnel.utils.TimeUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author machunxiao 2018-11-08
 */
public class EsPublisherTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(EsPublisherTest.class);

    @Test
    public void test_EsPublisher() {
        EmbeddedElastic elastic = EmbeddedElastic.builder()
                .withElasticVersion("5.0.0")
                .withSetting(PopularProperties.HTTP_PORT, 9200)
                .build();
        try {
            elastic.start();
        } catch (Exception e) {
            //
        }
        // http://localhost:9200
        String table = "";
        List<IEventFilter> filters = new ArrayList<>(Collections.singleton(new TableNameFilter(table)));

        EsConfig esConfig = new EsConfig();
        esConfig.setServer("http://localhost:9200");
        esConfig.setEsIdFieldNames(Collections.singletonList("id"));
        esConfig.setPkFieldNames(Collections.singletonList("id"));
        esConfig.setSchema("test1");
        esConfig.setTable("t_user");
        esConfig.setIndex("idx_t_u");
        esConfig.setType("logs");
        esConfig.setFilters(filters);

        List<EsConfig> esConfigs = new ArrayList<>(Collections.singleton(esConfig));
        EsPublisher publisher = new EsPublisher(esConfigs);

        ColumnData cd1 = new ColumnData();
        cd1.setName("id");
        cd1.setDataType("integer");
        cd1.setValue("1001");

        ColumnData cd2 = new ColumnData();
        cd2.setName("name");
        cd2.setDataType("varchar");
        cd2.setValue("tom hanks");

        ColumnData cd3 = new ColumnData();
        cd3.setName("sex");
        cd3.setDataType("varchar");
        cd3.setValue("mail");

        ColumnData cd4 = new ColumnData();
        cd4.setName("email");
        cd4.setDataType("varchar");
        cd4.setValue("xxx@tom.com");
        List<ColumnData> dataList = new ArrayList<>(Arrays.asList(cd1, cd2, cd3, cd4));

        Event event = new Event();
        event.setSchema("test1");
        event.setTable("t_user");
        event.setEventType(EventType.INSERT);
        event.setDataList(dataList);

        CountDownLatch latch = new CountDownLatch(1);
        InvokeContext ctx = new InvokeContext();
        ctx.setEvent(event);
        publisher.publish(ctx, new IPublisher.Callback() {
            @Override
            public void onSuccess() {
                LOGGER.info("success");
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.info("failure", t);
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            //
        }

        event.setEventType(EventType.UPDATE);
        CountDownLatch latch3 = new CountDownLatch(1);
        publisher.publish(ctx, new IPublisher.Callback() {
            @Override
            public void onSuccess() {
                LOGGER.info("success3");
                latch3.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.info("failure3", t);
                latch3.countDown();
            }
        });

        event.setEventType(EventType.BEGIN);
        publisher.publish(ctx, null);


        event.setEventType(EventType.DELETE);
        CountDownLatch latch2 = new CountDownLatch(1);
        publisher.publish(ctx, new IPublisher.Callback() {
            @Override
            public void onSuccess() {
                LOGGER.info("success2");
                latch2.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.info("failure2", t);
                latch2.countDown();
            }
        });
        try {
            latch2.await();
        } catch (InterruptedException e) {
            //
        }

        try {
            elastic.stop();
        } catch (Exception e) {
            //
        }
        publisher.close();
    }

    @Test
    public void test_BlockingQueue() {
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(16);
        for (int i = 0; i < 16; i++) {
            queue.add("idx-" + i);
        }
        List<String> list = new ArrayList<>();
        queue.drainTo(list, 8);
        Assert.assertEquals(queue.size(), list.size());

    }

    @Test
    public void test_cost() {

        int i = 0;
        while (i < 10) {
            long s = System.currentTimeMillis();
            func1();
            long e = System.currentTimeMillis();

            long cost = 1000 - (e - s);

            TimeUtils.sleepInMills(cost);
            i++;
        }

    }

    private void func1() {
        TimeUtils.sleepInMills(100L);
        LOGGER.info("called");
    }
}
