package com.hellobike.base.tunnel.utils;

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

/**
 * @author machunxiao create at 2018-12-03
 */
public interface ObjectManager<T> {

    /**
     * 产生一个实例
     *
     * @return 实例
     * @throws Exception 错误信息
     */
    T newInstance() throws Exception;

    /**
     * 释放资源
     *
     * @param instance 实例
     */
    default void releaseInstance(T instance) {
    }


    default boolean validateInstance(T instance) {
        return true;
    }

}
