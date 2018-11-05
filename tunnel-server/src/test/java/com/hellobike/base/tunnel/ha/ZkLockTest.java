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
package com.hellobike.base.tunnel.ha;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author machunxiao 2018-11-07
 */
public class ZkLockTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkLockTest.class);

    private static final ExecutorService EXECUTOR =
            new ThreadPoolExecutor(
                    10, 10,
                    0, TimeUnit.MICROSECONDS,
                    new ArrayBlockingQueue<>(10),
                    new ThreadFactory() {
                        private final AtomicInteger index = new AtomicInteger();

                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(r, "TestLock-" + index.incrementAndGet());
                            t.setDaemon(false);
                            return t;
                        }
                    });

    @Test
    public void test_lock() {
        ZkLock lock = new ZkLock("localhost:2181", "/tunnel/test/lock");
        lock.tryLock();
        lock.unlock();

        int total = 10;
        CountDownLatch latch = new CountDownLatch(total);
        CyclicBarrier barrier = new CyclicBarrier(total);
        Runnable task = () -> {
            try {
                barrier.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            LOGGER.info("thread:{} active", Thread.currentThread().getName());
            if (lock.tryLock()) {
                try {
                    LOGGER.info("thread:{} lock success", Thread.currentThread().getName());
                    Thread.sleep(10L);
                } catch (Exception e) {
                    //
                } finally {
                    lock.unlock();
                    LOGGER.info("thread:{} release lock success", Thread.currentThread().getName());
                    latch.countDown();
                }
            }
        };

        for (int i = 0; i < total; i++) {
            EXECUTOR.submit(task);
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            //
        }

        EXECUTOR.shutdown();
        lock.close();

    }
}
