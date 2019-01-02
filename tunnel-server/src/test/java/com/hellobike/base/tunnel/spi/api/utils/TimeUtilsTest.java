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
package com.hellobike.base.tunnel.spi.api.utils;

import com.hellobike.base.tunnel.utils.TimeUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * @author machunxiao create at 2018-12-05
 */
@RunWith(PowerMockRunner.class)
public class TimeUtilsTest {

    @Test
    public void test_sleepInMills() {
        TimeUtils.sleepInMills(-1L);
        TimeUtils.sleepInMills(1L);
        PowerMockito.spy(Thread.class);
        PowerMockito.doThrow(new InterruptedException()).when(Thread.class);
        TimeUtils.sleepInMills(2L);
    }
}
