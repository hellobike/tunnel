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
package com.hellobike.base.tunnel.publisher;

import com.hellobike.base.tunnel.model.Event;
import org.junit.Test;

/**
 * @author machunxiao 2018-11-12
 */
public class PublisherManagerTest {

    @Test
    public void test_PublisherManager() {

        PublisherManager.getInstance().putPublisher("test123", (event, callback) -> { /**/ });
        PublisherManager.getInstance().putPublisher("test456", (event, callback) -> { /**/ });
        PublisherManager.getInstance().publish((Event) null, null);
        PublisherManager.getInstance().publish((Event) null, new IPublisher.Callback() {
            @Override
            public void onSuccess() {

            }

            @Override
            public void onFailure(Throwable t) {

            }
        });
        PublisherManager.getInstance().publish(new Event(), null);
        PublisherManager.getInstance().publish(new Event(), new IPublisher.Callback() {

            @Override
            public void onSuccess() {

            }

            @Override
            public void onFailure(Throwable t) {

            }
        });

    }
}
