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
import com.hellobike.base.tunnel.publisher.IPublisher.Callback;
import org.junit.Test;

import java.util.Collections;

/**
 * @author machunxiao create at 2018-12-05
 */
public class HBasePublisherTest {

    @Test
    public void test_publish() {
        HBaseConfig config = new HBaseConfig();
        config.setFamily("tb1_family");
        config.setQualifier("tb1_qualifier");
        config.setPks(Collections.singletonList("id"));

        HBasePublisher publisher = new HBasePublisher(Collections.singletonList(config));
        Callback callback = new EmptyCallback();
        Event event = new Event();
        publisher.publish(event, callback);
    }

    private static class EmptyCallback implements Callback {

        @Override
        public void onFailure(Throwable t) {

        }

        @Override
        public void onSuccess() {

        }
    }

}
