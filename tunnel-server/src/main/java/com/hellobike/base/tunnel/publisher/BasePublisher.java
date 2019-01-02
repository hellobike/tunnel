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

package com.hellobike.base.tunnel.publisher;

import com.hellobike.base.tunnel.monitor.Statics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author machunxiao 2018-11-15
 */
public abstract class BasePublisher {

    public static List<Statics> mapToStatics(Map<String, Long> data) {
        List<Statics> staticsList = new ArrayList<>();
        for (Map.Entry<String, Long> entry : data.entrySet()) {
            String k = entry.getKey();
            Long c = entry.getValue();
            String[] dst = k.split("@@");

            Statics statics = Statics.createStatics(
                    "AppTunnelService",
                    dst[0],
                    dst[1],
                    dst[2],
                    c.intValue(),
                    dst[3],
                    ""
            );
            staticsList.add(statics);

        }
        return staticsList;
    }

    protected void onSuccess(IPublisher.Callback callback) {
        if (callback != null) {
            callback.onSuccess();
        }
    }

    protected void onFailure(IPublisher.Callback callback, Exception e) {
        if (callback != null) {
            callback.onFailure(e);
        }
    }
}
