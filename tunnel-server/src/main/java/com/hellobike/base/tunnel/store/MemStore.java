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

import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.model.InvokeContext;
import com.hellobike.base.tunnel.publisher.PublisherManager;
import com.hellobike.base.tunnel.spi.api.CollectionUtils;
import com.hellobike.base.tunnel.utils.NamedThreadFactory;
import com.hellobike.base.tunnel.utils.TimeUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author machunxiao 2018-10-25
 */
public class MemStore implements IStore, AutoCloseable {

    private static final int PER_CONTEXTS = 10240;
    private static final int MAX_CONTEXTS = PER_CONTEXTS * 5;

    private static final int CPU_NUMBERS = Runtime.getRuntime().availableProcessors();
    private static final int THD_NUMBERS = CPU_NUMBERS << 1;

    private final AtomicBoolean started = new AtomicBoolean(Boolean.FALSE);
    private final Map<String, BlockingQueue<InvokeContext>> caches;
    private final ThreadPoolExecutor executor;

    public MemStore() {
        this.caches = new ConcurrentHashMap<>();
        this.executor = new ThreadPoolExecutor(
                THD_NUMBERS, THD_NUMBERS,
                60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(MAX_CONTEXTS),
                new NamedThreadFactory("")
        );

        this.started.compareAndSet(Boolean.FALSE, Boolean.TRUE);
        for (int i = 0; i < THD_NUMBERS; i++) {
            this.executor.submit(new ConsumeTask());
        }
    }

    private static boolean eventIsEmpty(Event event) {
        return event == null ||
                (event.getSchema() == null
                        && event.getTable() == null
                        && event.getEventType() == null
                        && CollectionUtils.isEmpty(event.getDataList()));
    }

    private static boolean ctxIsEmpty(InvokeContext ctx) {
        return ctx == null || eventIsEmpty(ctx.getEvent());
    }

    @Override
    public void store(InvokeContext ctx) {
        if (ctxIsEmpty(ctx)) {
            return;
        }
        PublisherManager.getInstance().publish(ctx, null);
    }

    @Override
    public void close() {
        this.started.compareAndSet(Boolean.TRUE, Boolean.FALSE);
        this.executor.shutdown();
        this.caches.values().forEach(BlockingQueue::clear);
        this.caches.values().clear();
        this.caches.clear();
    }

    public void asyncPublish(InvokeContext ctx) {
        if (ctxIsEmpty(ctx)) {
            return;
        }

        BlockingQueue<InvokeContext> queue = putToCache(ctx);
        if (queue.size() > PER_CONTEXTS) {
            forceFlushMemory(queue);
        }
    }

    private synchronized BlockingQueue<InvokeContext> putToCache(InvokeContext context) {
        BlockingQueue<InvokeContext> blockingQueue = caches.get(context.getSlotName());
        if (blockingQueue == null) {
            caches.put(context.getSlotName(), new LinkedBlockingQueue<>(MAX_CONTEXTS));
            blockingQueue = caches.get(context.getSlotName());
        }
        try {
            blockingQueue.put(context);
        } catch (Exception e) {
            //
        }
        return blockingQueue;
    }

    private void forceFlushMemory(BlockingQueue<InvokeContext> queue) {
        if (CollectionUtils.isEmpty(queue)) {
            return;
        }
        int capacity = started.get() ? Math.min(PER_CONTEXTS, queue.size()) : queue.size();
        List<InvokeContext> contexts = new LinkedList<>();
        queue.drainTo(contexts, capacity);
        PublisherManager.getInstance().publish(contexts);
    }

    private class ConsumeTask implements Runnable {

        @Override
        public void run() {
            while (started.get()) {
                long s = System.currentTimeMillis();
                try {
                    for (BlockingQueue<InvokeContext> queue : caches.values()) {
                        forceFlushMemory(queue);
                    }
                } finally {
                    long e = System.currentTimeMillis();
                    TimeUtils.sleepOneSecond(s, e);
                }
            }

            for (BlockingQueue<InvokeContext> queue : caches.values()) {
                forceFlushMemory(queue);
            }
        }

    }

}
