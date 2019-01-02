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

package com.hellobike.base.tunnel.publisher.kafka;

import com.alibaba.fastjson.JSON;
import com.hellobike.base.tunnel.config.KafkaConfig;
import com.hellobike.base.tunnel.model.ColumnData;
import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.monitor.Statics;
import com.hellobike.base.tunnel.monitor.TunnelMonitorFactory;
import com.hellobike.base.tunnel.publisher.BasePublisher;
import com.hellobike.base.tunnel.publisher.IPublisher;
import com.hellobike.base.tunnel.spi.api.CollectionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author machunxiao 2018-10-25
 */
public class KafkaPublisher extends BasePublisher implements IPublisher {

    private static final Logger                     /**/ log = LoggerFactory.getLogger(KafkaPublisher.class);

    private final List<KafkaConfig>                 /**/ kafkaConfigs;
    private final KafkaProducer<String, String>     /**/ producer;

    public KafkaPublisher(List<KafkaConfig> kafkaConfigs) {
        this.kafkaConfigs = kafkaConfigs;
        this.producer = new KafkaProducer<>(getProperties(kafkaConfigs));
    }

    private static Properties getProperties(List<KafkaConfig> kafkaConfigs) {
        KafkaConfig kafkaConfig = kafkaConfigs.get(0);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getServer());
        props.put(ProducerConfig.ACKS_CONFIG, kafkaConfig.getAckConfig());
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaConfig.getKeySerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaConfig.getValSerializer());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        return props;
    }

    @Override
    public void publish(Event event, Callback callback) {

        this.kafkaConfigs.forEach(kafkaConfig -> internalPublish(kafkaConfig, event, callback));

    }

    @Override
    public void close() {
        this.producer.close();
        log.info("KafkaPublisher Closed");
    }

    private void internalPublish(KafkaConfig kafkaConfig, Event event, Callback callback) {
        if (CollectionUtils.isEmpty(kafkaConfig.getFilters())
                || kafkaConfig.getFilters().stream().allMatch(filter -> filter.filter(event))) {
            sendToKafka(kafkaConfig, event, callback);
        }
    }

    private void sendToKafka(KafkaConfig kafkaConfig, Event event, Callback callback) {
        String value = JSON.toJSONString(event);
        ProducerRecord<String, String> record = new ProducerRecord<>(kafkaConfig.getTopic(), getPrimaryKey(kafkaConfig, event), value);
        String[] errors = new String[1];
        try {
            if (callback == null) {
                producer.send(record);
            } else {
                producer.send(record,
                        (metadata, exception) -> {
                            if (exception != null) {
                                errors[0] = exception.getMessage();
                                KafkaPublisher.this.onFailure(callback, exception);
                            } else {
                                KafkaPublisher.this.onSuccess(callback);
                            }
                        });
            }
        } catch (Exception e) {
            errors[0] = e.getMessage();
            throw new RuntimeException(e);
        } finally {
            Statics statics = Statics.createStatics(
                    "AppTunnelService",
                    event.getSchema(),
                    event.getSlotName(),
                    event.getTable(),
                    1,
                    "kafka",
                    errors[0] == null ? "" : errors[0]
            );
            TunnelMonitorFactory.getTunnelMonitor().collect(statics);
        }
    }

    private String getPrimaryKey(KafkaConfig kafkaConfig, Event event) {
        List<String> pkNames = kafkaConfig.getPkNames();
        if (CollectionUtils.isEmpty(pkNames)) {
            return null;
        }
        Map<String, String> data = event.getDataList().stream().collect(Collectors.toMap(ColumnData::getName, ColumnData::getValue));
        return kafkaConfig.getPkNames().stream().map(data::get).reduce((s1, s2) -> s1 + "_" + s2).orElse(null);
    }

}
