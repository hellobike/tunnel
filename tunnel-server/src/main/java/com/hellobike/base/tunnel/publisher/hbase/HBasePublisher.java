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
import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.publisher.BasePublisher;
import com.hellobike.base.tunnel.publisher.IPublisher;

import java.util.List;
import java.util.Objects;

/**
 * @author machunxiao create at 2018-11-27
 */
public class HBasePublisher extends BasePublisher implements IPublisher {

    private final HBaseClient hBaseClient;
    private final List<HBaseConfig> configs;

    public HBasePublisher(List<HBaseConfig> configs) {
        this.hBaseClient = getHBaseClient(configs);
        this.configs = configs;
    }

    @Override
    public void publish(Event event, Callback callback) {

        for (HBaseConfig config : configs) {
            if (!config.getFilters().stream().allMatch(filter -> filter.filter(event))) {
                continue;
            }
            switch (event.getEventType()) {
                case INSERT:
                    hBaseClient.insert(config, event);
                    break;
                case UPDATE:
                    hBaseClient.update(config, event);
                    break;
                case DELETE:
                    hBaseClient.delete(config, event);
                    break;
                default:
                    break;
            }
        }
    }


    @Override
    public void close() {
        this.hBaseClient.close();
    }


    private HBaseClient getHBaseClient(List<HBaseConfig> configs) {
        String quorum = configs.stream()
                .map(HBaseConfig::getQuorum)
                .filter(Objects::nonNull)
                .findFirst().orElse(null);
        return quorum == null ? HBaseClient.getInstance() : HBaseClient.getInstance(quorum);
    }

}
