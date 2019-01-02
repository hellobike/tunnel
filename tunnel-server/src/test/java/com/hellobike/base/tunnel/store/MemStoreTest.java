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
package com.hellobike.base.tunnel.store;

import com.hellobike.base.tunnel.model.InvokeContext;
import com.hellobike.base.tunnel.publisher.PublisherManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * @author machunxiao 2018-11-08
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(PublisherManager.class)
public class MemStoreTest {

    @Test
    public void test_store() {

        PublisherManager mgr = Mockito.mock(PublisherManager.class);
        PowerMockito.mockStatic(PublisherManager.class);
        Mockito.when(PublisherManager.getInstance()).thenReturn(mgr);

        PublisherManager actual = PublisherManager.getInstance();
        assertEquals(mgr, actual);

        MemStore memStore = new MemStore();
        memStore.store(null);
        InvokeContext ctx = new InvokeContext();
        ctx.setSlotName("testSlotName");
        memStore.store(ctx);
    }
}
