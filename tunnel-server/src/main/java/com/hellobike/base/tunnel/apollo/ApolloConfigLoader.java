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

package com.hellobike.base.tunnel.apollo;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.model.ConfigChange;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import com.hellobike.base.tunnel.config.ConfigListener;
import com.hellobike.base.tunnel.config.ConfigLoader;

import java.util.Set;

/**
 * @author machunxiao create at 2018-12-26
 */
public class ApolloConfigLoader implements ConfigLoader {

    private final Config config;

    public ApolloConfigLoader(String appId, String metaDomain) {
        this.config = ConfigService.getAppConfig();
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        return this.config.getProperty(key, defaultValue);
    }

    @Override
    public void addChangeListener(ConfigListener configListener) {
        this.config.addChangeListener(new ApolloInnerConfigListener(configListener));
    }

    private static class ApolloInnerConfigListener implements ConfigChangeListener {

        private final ConfigListener configListener;

        public ApolloInnerConfigListener(ConfigListener configListener) {
            this.configListener = configListener;
        }

        @Override
        public void onChange(ConfigChangeEvent changeEvent) {
            Set<String> keys = changeEvent.changedKeys();
            for (String key : keys) {
                ConfigChange change = changeEvent.getChange(key);
                if (change == null) {
                    continue;
                }

                String oldValue = change.getOldValue();
                String newValue = change.getNewValue();
                configListener.onChange(key, oldValue, newValue);
            }
        }
    }
}
