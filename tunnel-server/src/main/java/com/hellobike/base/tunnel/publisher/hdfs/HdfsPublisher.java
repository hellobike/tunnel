package com.hellobike.base.tunnel.publisher.hdfs;

import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.publisher.BasePublisher;
import com.hellobike.base.tunnel.publisher.IPublisher;

import java.util.List;

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

/**
 * @author machunxiao create at 2018-11-30
 */
public class HdfsPublisher extends BasePublisher implements IPublisher {

    private List<HdfsConfig> configs;
    private HdfsClient hdfsClient;

    public HdfsPublisher(List<HdfsConfig> configs) {
        this.configs = configs;
        this.hdfsClient = new HdfsClient();
    }

    @Override
    public void publish(Event event, Callback callback) {
        for (HdfsConfig config : configs) {
            switch (event.getEventType()) {
                case INSERT:
                    this.hdfsClient.append(config, event);
                    break;
                case DELETE:
                    this.hdfsClient.delete(config, event);
                    break;
                case UPDATE:
                    this.hdfsClient.update(config, event);
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void close() {

    }


}
