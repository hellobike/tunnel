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

package com.hellobike.base.tunnel.publisher.hdfs;

import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.publisher.BasePublisher;
import com.hellobike.base.tunnel.publisher.IPublisher;

import java.util.regex.Pattern;

/**
 * @author machunxiao create at 2018-11-30
 */
public class HdfsPublisher extends BasePublisher implements IPublisher {

    private final HdfsConfig hdfsConfig;
    private final HdfsClient hdfsClient;

    public HdfsPublisher(HdfsConfig hdfsConfig) {
        this.hdfsConfig = hdfsConfig;
        this.hdfsClient = new HdfsClient(this.hdfsConfig.getAddress(), this.hdfsConfig.getFileName());
    }

    @Override
    public void publish(Event event, Callback callback) {
        for (HdfsRule rule : hdfsConfig.getRules()) {
            if (!testTableName(rule.getTable(), event.getTable())) {
                continue;
            }
            switch (event.getEventType()) {
                case INSERT:
                    this.hdfsClient.append(hdfsConfig, rule, event);
                    break;
                case DELETE:
                    this.hdfsClient.delete(hdfsConfig, rule, event);
                    break;
                case UPDATE:
                    this.hdfsClient.update(hdfsConfig, rule, event);
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void close() {

    }

    private boolean testTableName(String tableFilter, String table) {
        return Pattern.compile(tableFilter).matcher(table).matches();
    }

}
