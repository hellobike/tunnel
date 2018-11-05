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

/**
 * @author machunxiao create at 2018-12-27
 */
public class TunnelMonitorFactory {

    private static volatile TunnelExporter exporter;

    private TunnelMonitorFactory() {
    }

    public static TunnelMonitor getTunnelMonitor() {
        return exporter.getTunnelMonitor();
    }

    public static synchronized TunnelExporter initializeExporter(boolean useYukon, ExporterConfig config) {
        if (exporter == null) {
            synchronized (TunnelMonitorFactory.class) {
                exporter = new PrometheusExporter(config);
            }
        }

        return exporter;
    }

}
