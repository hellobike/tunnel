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

import com.hellobike.base.tunnel.spi.api.BaseTransportConfig;
import com.hellobike.base.tunnel.spi.api.TransportConfig;
import lombok.Data;

import java.util.LinkedHashSet;

/**
 * @author machunxiao create at 2019-01-03
 */
@Data
public class EsConfig extends BaseTransportConfig<EsConfig> implements TransportConfig<EsConfig> {

    private String index;
    private String type;
    private LinkedHashSet<String> esId;
    private String joinSql;
    private LinkedHashSet<String> parameters;
    private String separator = "_";

    @Override
    public EsConfig toRealConfig(String config) {
        return this;
    }

}
