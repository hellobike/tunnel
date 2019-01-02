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

package com.hellobike.base.tunnel.spi.es;

import com.hellobike.base.tunnel.spi.api.Invocation;
import com.hellobike.base.tunnel.spi.api.NamedThreadFactory;
import com.hellobike.base.tunnel.spi.api.OutputHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author machunxiao create at 2019-01-03
 */
public class EsHandler implements OutputHandler {

    private static final int THREAD_CPU = 8;
    private static final int CRITICAL_NUM = 10240;
    private static final int MAX_CACHED = THREAD_CPU * 10240;

    private final ScheduledThreadPoolExecutor executor;
    private final BlockingQueue<Invocation> queue = new LinkedBlockingQueue<>(MAX_CACHED);
    private final EsClient esClient;

    public EsHandler(EsClientConfig clientConfig) {
        this.executor = new ScheduledThreadPoolExecutor(THREAD_CPU, new NamedThreadFactory("Es"));
        this.esClient = new EsClient(clientConfig);
        this.executor.scheduleAtFixedRate(this::forceFlush, 0L, 1000L, TimeUnit.MILLISECONDS);
    }

    @Override
    public void handle(Invocation invocation) {
        appendToQueue(invocation);
        maybeFlush();
    }

    @Override
    public void close() {
        this.executor.shutdown();
        this.esClient.close();
        EsJdbcManager.close();
    }

    private void appendToQueue(Invocation invocation) {
        try {
            queue.put(invocation);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void maybeFlush() {
        if (queue.size() >= CRITICAL_NUM) {
            forceFlush();
        }
    }

    private void forceFlush() {
        int capacity = Math.min(4096, queue.size());
        List<Invocation> invocations = new ArrayList<>(capacity);
        queue.drainTo(invocations, capacity);
        this.executor.submit(new EsTask(this.esClient, invocations));
    }

}
