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

package com.hellobike.base.tunnel.spi.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author machunxiao create at 2018-12-24
 */
class KafkaClient {

    private KafkaClientConfig clientConfig;
    private KafkaProducer<String, String> producer;

    KafkaClient(KafkaClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        this.producer = new KafkaProducer<>(getProperties(clientConfig));
    }

    private static Properties getProperties(KafkaClientConfig clientConfig) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clientConfig.getServer());
        props.put(ProducerConfig.ACKS_CONFIG, clientConfig.getAckConfig());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, clientConfig.getKeySerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, clientConfig.getValSerializer());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        return props;
    }

    void send(ProducerRecord<String, String> record) {
        this.producer.send(record);
    }

    void close() {
        this.producer.close();
    }
}
