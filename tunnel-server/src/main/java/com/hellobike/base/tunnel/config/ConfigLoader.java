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

package com.hellobike.base.tunnel.config;

/**
 * @author machunxiao create at 2018-12-26
 */
public interface ConfigLoader {

    /**
     * 获取配置
     *
     * @param key          key
     * @param defaultValue 默认值
     * @return
     */
    String getProperty(String key, String defaultValue);

    /**
     * 添加事件监听器
     *
     * @param configListener 监听器
     */
    void addChangeListener(ConfigListener configListener);

}
