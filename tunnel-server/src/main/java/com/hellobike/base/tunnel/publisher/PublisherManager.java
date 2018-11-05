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

package com.hellobike.base.tunnel.publisher;

import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.model.InvokeContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author machunxiao 2018-10-25
 */
public class PublisherManager implements IPublisher {

    private static final PublisherManager INSTANCE = new PublisherManager();

    private Map<String, IPublisher> publishers = new ConcurrentHashMap<>();

    private PublisherManager() {
    }

    public static PublisherManager getInstance() {
        return INSTANCE;
    }

    @Override
    public void publish(Event event, Callback callback) {
        this.publishers.get(event.getSlotName()).publish(event, callback);
    }

    @Override
    public void publish(InvokeContext context, Callback callback) {
        this.publishers.get(context.getSlotName()).publish(context, callback);
    }

    @Override
    public void close() {
        this.publishers.values().forEach(IPublisher::close);
        this.publishers.clear();
    }

    @Override
    public void publish(List<InvokeContext> contexts) {
        if (contexts.isEmpty()) {
            return;
        }
    }

    public void putPublisher(String slotName, IPublisher publisher) {
        this.publishers.put(slotName, publisher);
    }
}
