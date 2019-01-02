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
package com.hellobike.base.tunnel.monitor;

import io.prometheus.client.Gauge;

/**
 * @author machunxiao create at 2018-12-25
 */
public class PrometheusMonitor implements TunnelMonitor {

    private final Gauge gauge;

    public PrometheusMonitor(ExporterConfig config) {
        this.gauge = Gauge.build()
                .name(config.getMetricName())
                .labelNames(config.getLabelNames())
                .help("Tunnel Requests.").register();
    }

    @Override
    public void collect(Statics statics) {
        this.gauge.labels(statics.getSlotName(), statics.getAppId(),
                statics.getDatabase(), statics.getTable(),
                statics.getTarget(), String.valueOf(statics.getTotal()),
                String.valueOf(statics.getCurrentTime()), statics.getError()).inc();
    }

}
