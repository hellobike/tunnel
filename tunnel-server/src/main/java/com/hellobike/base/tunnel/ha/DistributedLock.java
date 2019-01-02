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
package com.hellobike.base.tunnel.ha;

import java.util.concurrent.TimeUnit;

/**
 * @author machunxiao create at 2018-12-26
 */
public interface DistributedLock {

    /**
     * 获取锁
     *
     * @throws LockingException
     */
    void lock() throws LockingException;

    /**
     * 尝试获取锁
     *
     * @param timeout 超时时间
     * @param unit    时间刻度
     * @return lock success:true else:false
     */
    boolean tryLock(long timeout, TimeUnit unit);

    /**
     * 释放锁
     *
     * @throws LockingException
     */
    void unlock() throws LockingException;

}
