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
package com.hellobike.base.tunnel.config.file;

import com.alibaba.fastjson.JSON;
import com.hellobike.base.tunnel.config.YmlConfig;
import com.hellobike.base.tunnel.constants.Constants;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

public class YmlFileConfigLoader extends FileConfigLoader {

    public YmlFileConfigLoader(String fileName) {
        super(fileName);
    }

    @Override
    protected Map<String, String> load(File file) {
        HashMap<String, String> map = new HashMap<>();
        try {
            Yaml yaml = new Yaml();
            YmlConfig ymlConfig = yaml.loadAs(new FileInputStream(file), YmlConfig.class);
            map.put(Constants.TUNNEL_KEY, JSON.toJSONString(ymlConfig.getTunnel_subscribe_config()));
            map.put(Constants.TUNNEL_ZK_KEY, ymlConfig.getTunnel_zookeeper_address());
        } catch (FileNotFoundException e) {
            logger.warn("config file {} not found", file);
        } catch (RuntimeException e) {
            logger.warn("parse config error: ", e);
        }
        return map;
    }

}
