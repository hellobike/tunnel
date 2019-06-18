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

import java.io.File;
import java.io.FileInputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesFileConfigLoader extends FileConfigLoader {

    public PropertiesFileConfigLoader(String fileName) {
        super(fileName);
    }

    @Override
    protected Map<String, String> load(File file) {
        Map<String, String> data = new LinkedHashMap<>();
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream(file));
            for (Map.Entry<Object, Object> e : prop.entrySet()) {
                Object key = e.getKey();
                Object value = e.getValue();
                if (key != null) {
                    data.put((String) key, (String) value);
                }
            }
        } catch (Exception e) {

        }
        return data;
    }
}
