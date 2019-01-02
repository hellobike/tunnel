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

package com.hellobike.base.tunnel.spi.api;

import java.util.HashMap;
import java.util.Map;

/**
 * @author machunxiao create at 2019-01-03
 */
public final class Dispatcher implements AutoCloseable {

    private static final Dispatcher INSTANCE = new Dispatcher();
    private static final OutputHandler EMPTY_HANDLER = new DefaultHandler();
    private static final DefaultFilter EMPTY_FILTER = new DefaultFilter();

    private final Map<String /*rule name*/, TunnelFilter> filters;
    private final Map<String /*slot name*/, OutputHandler> handlers;

    public static Dispatcher getInstance() {
        return INSTANCE;
    }

    private Dispatcher() {
        this.filters = new HashMap<>();
        this.handlers = new HashMap<>();
    }

    public void dispatch(Invocation invocation) {

        TunnelFilter filter = getTunnelFilter(invocation.getEvent().getTable());

        if (!filter.test(invocation)) {
            return;
        }

        String slotName = invocation.getSlotName();
        OutputHandler handler = getOutputHandler(slotName);
        handler.handle(invocation);
    }

    public void addRule(String ruleName, TunnelFilter rule) {
        this.filters.put(ruleName, rule);
    }

    public void addHandler(String slotName, OutputHandler handler) {
        this.handlers.put(slotName, handler);
    }

    @Override
    public void close() {
        this.filters.clear();
        this.handlers.clear();
    }

    private TunnelFilter getTunnelFilter(String table) {
        return this.filters.getOrDefault(table, EMPTY_FILTER);
    }

    private OutputHandler getOutputHandler(String slotName) {
        return this.handlers.getOrDefault(slotName, EMPTY_HANDLER);
    }

}
