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

import io.opencensus.exporter.stats.prometheus.PrometheusStatsCollector;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

/**
 * @author machunxiao create at 2018-12-27
 */
public class PrometheusExporter implements TunnelExporter {

    private ExporterConfig exporterConfig;
    private PrometheusMonitor monitor;
    private HTTPServer server;

    public PrometheusExporter(ExporterConfig exporterConfig) {
        this.exporterConfig = exporterConfig;
        this.monitor = new PrometheusMonitor(this.exporterConfig);
    }

    @Override
    public void startup() {
        PrometheusStatsCollector.createAndRegister();
        DefaultExports.initialize();
        try {
            this.server = new HTTPServer(this.exporterConfig.getExportPort());
        } catch (Exception e) {
            //
        }
    }

    @Override
    public void destroy() {
        if (this.server != null) {
            this.server.stop();
        }
    }

    @Override
    public TunnelMonitor getTunnelMonitor() {
        return monitor;
    }

}
