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

import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

/**
 * @author machunxiao create at 2018-12-27
 */
public class PrometheusMain {

    public static void main(String[] args) {
        ExporterConfig config = new ExporterConfig();
        TunnelExporter exporter = new PrometheusExporter(config);
        TunnelMonitor monitor = exporter.getTunnelMonitor();
        exporter.startup();
        int total = 10;
        CountDownLatch latch = new CountDownLatch(total);
        Thread[] threads = new Thread[total];
        IntStream.range(0, total).forEach(i -> {
            threads[i] = new Thread(() -> {
                monitor.collect(Statics.createStatics("testApp" + i, "testDb" + i, "sn" + i, "table" + i, i, "target" + i, "error" + i));
                latch.countDown();
            });
            threads[i].start();
        });
        try {
            latch.await();
        } catch (Exception e) {
            //
        }

        exporter.destroy();
    }

}
