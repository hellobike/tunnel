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

import com.alibaba.fastjson.JSON;
import com.hellobike.base.tunnel.spi.api.*;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author machunxiao create at 2018-12-25
 */
public class KafkaHandler implements OutputHandler {

    private KafkaClient kafkaClient;

    public KafkaHandler(KafkaClientConfig clientConfig) {
        this.kafkaClient = new KafkaClient(clientConfig);
    }

    @Override
    public void handle(Invocation invocation) {
        this.kafkaClient.send(toProducerRecord(invocation));
    }

    @Override
    public void close() {
        this.kafkaClient.close();
    }

    private ProducerRecord<String, String> toProducerRecord(Invocation invocation) {
        KafkaConfig kafkaConfig = invocation.getParameter(Constants.CONFIG_NAME);
        String topic = kafkaConfig.getTopic();
        String key = toPrimaryKey(invocation);
        String val = toJsonString(invocation);
        return new ProducerRecord<>(topic, key, val);
    }

    private String toPrimaryKey(Invocation invocation) {
        KafkaConfig kafkaConfig = invocation.getParameter(Constants.CONFIG_NAME);
        LinkedHashSet<String> pkNames = kafkaConfig.getPkNames();
        if (CollectionUtils.isEmpty(pkNames)) {
            return null;
        }
        Map<String, String> data = invocation.getEvent().getDataList().stream()
                .collect(Collectors.toMap(CellData::getName, CellData::getValue));
        return pkNames.stream()
                .map(data::get)
                .reduce((s1, s2) -> s1 + "_" + s2)
                .orElse(null);
    }

    private String toJsonString(Invocation invocation) {
        return JSON.toJSONString(invocation.getEvent());
    }

}
