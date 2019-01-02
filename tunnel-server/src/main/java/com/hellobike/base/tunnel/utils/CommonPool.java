package com.hellobike.base.tunnel.utils;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

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
public class CommonPool<T> implements ObjectPool<T> {

    private final ObjectManager<T> manager;
    private final GenericObjectPool<T> pool;

    public CommonPool(ObjectManager<T> manager) {
        this.manager = manager;
        this.pool = newObjectPool();
    }

    @Override
    public T borrowObject() {
        try {
            return pool.borrowObject();
        } catch (Exception e) {
            throw new UnsupportedOperationException(e);
        }
    }

    @Override
    public void returnObject(T obj) {
        pool.returnObject(obj);
    }

    @Override
    public void close() {
        this.pool.close();
    }

    private GenericObjectPool<T> newObjectPool() {
        GenericObjectPool<T> pool = new GenericObjectPool<>(new CommonFactory()); // NOSONAR
        pool.setConfig(newPoolConfig());
        return pool;
    }

    private GenericObjectPoolConfig<T> newPoolConfig() {
        GenericObjectPoolConfig<T> config = new GenericObjectPoolConfig<>();
        config.setTestWhileIdle(true);
        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(false);
        config.setMaxTotal(100);
        config.setMinIdle(30);
        config.setMaxIdle(80);
        return config;
    }

    private class CommonFactory extends BasePooledObjectFactory<T> {

        @Override
        public T create() throws Exception {
            return manager.newInstance();
        }

        @Override
        public PooledObject<T> wrap(T t) {
            return new DefaultPooledObject<>(t);
        }

        @Override
        public boolean validateObject(PooledObject<T> p) {
            return manager.validateInstance(p.getObject());
        }

        @Override
        public void destroyObject(PooledObject<T> p) {
            manager.releaseInstance(p.getObject());
        }

    }

}
