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

package com.hellobike.base.tunnel.publisher.hive;

import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.publisher.BasePublisher;
import com.hellobike.base.tunnel.publisher.IPublisher;

import java.util.List;
import java.util.regex.Pattern;

/**
 * @author machunxiao create at 2018-11-27
 */
public class HivePublisher extends BasePublisher implements IPublisher {

    private final HiveConfig hiveConfig;
    private final HdfsClient hdfsClient;

    public HivePublisher(HiveConfig hiveConfig) {
        this.hiveConfig = hiveConfig;
        this.hdfsClient = new HdfsClient(hiveConfig);
    }

    @Override
    public void publish(Event event, Callback callback) {
        //
        List<HiveRule> rules = this.hiveConfig.getRules();
        for (HiveRule rule : rules) {
            String table = rule.getTable();
            if (!testTableName(table, event.getTable())) {
                continue;
            }
            switch (event.getEventType()) {
                case INSERT:
                    hdfsClient.insert(rule, event);
                    break;
                case DELETE:
                    break;
                case UPDATE:
                    break;
                default:
                    break;
            }
        }

    }

    @Override
    public void close() {
        hdfsClient.close();
    }


    private boolean testTableName(String tableFilter, String table) {
        return Pattern.compile(tableFilter).matcher(table).matches();
    }

}
