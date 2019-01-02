package com.hellobike.base.tunnel.config;

import com.hellobike.base.tunnel.filter.IEventFilter;

import java.util.List;

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

/**
 * @author machunxiao 2018-11-01
 */
public class KafkaConfig {

    private String topic;
    private String server;
    private String ackConfig = "all";
    private int retryTimes;
    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String valSerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private int partition = 0;
    private List<IEventFilter> filters;

    private List<String> pkNames;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getAckConfig() {
        return ackConfig;
    }

    public void setAckConfig(String ackConfig) {
        this.ackConfig = ackConfig;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
    }

    public String getValSerializer() {
        return valSerializer;
    }

    public void setValSerializer(String valSerializer) {
        this.valSerializer = valSerializer;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public List<IEventFilter> getFilters() {
        return filters;
    }

    public void setFilters(List<IEventFilter> filters) {
        this.filters = filters;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }
}
