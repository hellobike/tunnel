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
package com.hellobike.base.tunnel;

import com.hellobike.base.tunnel.utils.TimeUtils;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author machunxiao create at 2018-12-05
 */
public class TunnelServerTest {

    private static volatile boolean stopped = false;

    @Test
    public void test_threadPool() {
        int total = 4;
        ThreadPoolExecutor executor = new ThreadPoolExecutor(total, total, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100));

        for (int i = 0; i < total; i++) {
            executor.submit(new Task(i));
        }

        try {
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            //
        }
        stopped = true;
        executor.shutdown();
    }

    private static class Task implements Runnable {

        private final int idx;

        private Task(int idx) {
            this.idx = idx;
        }

        @Override
        public void run() {
            while (!stopped) {
                TimeUtils.sleepInMills(1000);
            }
        }

        @Override
        public String toString() {
            return "Task{" +
                    "idx=" + idx +
                    '}';
        }
    }
}
