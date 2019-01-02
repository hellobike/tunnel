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

package com.hellobike.base.tunnel.publisher.hive;

import com.hellobike.base.tunnel.filter.IEventFilter;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author machunxiao create at 2018-11-27
 */
@Data
public class HiveConfig {

    private String hiveUrl;
    private String username;
    private String password;
    private String dataDir;
    private String table;
    private String partition;
    private String[] hdfsAddresses;

    private List<IEventFilter> filters = new ArrayList<>();
    private List<HiveRule> rules = new ArrayList<>();

}
