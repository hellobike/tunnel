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

package com.hellobike.base.tunnel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author machunxiao create at 2018-12-04
 */
public class TunnelContext {

    private static final Map<String, TunnelServer>      /**/ SERVERS        /**/ = new ConcurrentHashMap<>();

    private TunnelContext() {
    }

    public static TunnelServer findServer(String serverId) {
        return SERVERS.get(serverId);
    }

    public static void putServer(TunnelServer server) {
        SERVERS.put(server.getServerId(), server);
    }

    public static void remove(String serverId) {
        SERVERS.remove(serverId);
    }

    public static void close() {
        SERVERS.values()
                .forEach(TunnelServer::shutdown);
        SERVERS.clear();
    }

}
