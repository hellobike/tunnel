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
package com.hellobike.base.tunnel.publisher;

import com.hellobike.base.tunnel.config.KafkaConfig;
import com.hellobike.base.tunnel.filter.IEventFilter;
import com.hellobike.base.tunnel.filter.TableNameFilter;
import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.publisher.kafka.KafkaPublisher;
import net.manub.embeddedkafka.EmbeddedKafka;
import net.manub.embeddedkafka.EmbeddedKafkaConfig;
import net.manub.embeddedkafka.EmbeddedKafkaConfigImpl;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.HashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author machunxiao 2018-11-08
 */
public class KafkaPublisherTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPublisherTest.class);

    @Test
    public void test_KafkaPublisher() {

        EmbeddedKafkaConfig cfg = new EmbeddedKafkaConfigImpl(9093, 2182, new HashMap<>(), new HashMap<>(), new HashMap<>());
        EmbeddedKafka.start(cfg);
        String address = "localhost:9093";
        KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.setServer(address);
        kafkaConfig.setTopic("test1");
        kafkaConfig.setPartition(-1);
        List<KafkaConfig> kafkaConfigs = new ArrayList<>(Collections.singleton(kafkaConfig));

        String table = "";
        List<IEventFilter> filters = new ArrayList<>(Collections.singleton(new TableNameFilter(table)));
        kafkaConfig.setFilters(filters);
        KafkaPublisher publisher = new KafkaPublisher(kafkaConfigs);

        Event event = new Event();
        event.setTable("t1");
        CountDownLatch latch1 = new CountDownLatch(1);
        IPublisher.Callback callback = new IPublisher.Callback() {
            @Override
            public void onSuccess() {
                LOGGER.info("success");
                latch1.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.info("failure", t);
                latch1.countDown();
            }
        };
        publisher.publish(event, callback);
        try {
            latch1.await();
        } catch (InterruptedException e) {
            //
        }
        publisher.publish(event, null);


        table = "abc";
        filters = new ArrayList<>(Collections.singleton(new TableNameFilter(table)));
        kafkaConfig.setFilters(filters);
        publisher = new KafkaPublisher(kafkaConfigs);
        CountDownLatch latch2 = new CountDownLatch(1);
        event.setTable("def");

        publisher.publish(event, new IPublisher.Callback() {
            @Override
            public void onSuccess() {
                latch2.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.warn("", t);
                latch2.countDown();
            }
        });
        try {
            latch2.await();
        } catch (InterruptedException e) {
            //
        }
        EmbeddedKafka.stop();

    }
}
