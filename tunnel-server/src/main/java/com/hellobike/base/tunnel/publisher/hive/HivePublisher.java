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

package com.hellobike.base.tunnel.publisher.hive;

import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.publisher.BasePublisher;
import com.hellobike.base.tunnel.publisher.IPublisher;

/**
 * @author machunxiao create at 2018-11-27
 */
public class HivePublisher extends BasePublisher implements IPublisher {

    private final HiveClient hiveClient;

    public HivePublisher(HiveConfig hiveConfig) {
        this.hiveClient = new HiveClient(hiveConfig);
    }

    @Override
    public void publish(Event event, Callback callback) {
        //
        switch (event.getEventType()) {
            case INSERT:
                hiveClient.insert(event);
                break;
            case DELETE:
                hiveClient.delete(event);
                break;
            case UPDATE:
                hiveClient.update(event);
                break;
            default:
                break;
        }

    }

    @Override
    public void close() {
        hiveClient.close();
    }


}
