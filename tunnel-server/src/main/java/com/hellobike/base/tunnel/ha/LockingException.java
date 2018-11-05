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
package com.hellobike.base.tunnel.ha;

/**
 * @author machunxiao create at 2018-12-26
 */
public class LockingException extends RuntimeException {

    private static final long serialVersionUID = 2256598708734964597L;

    public LockingException(String msg, Throwable e) {
        super(msg, e);
    }

    public LockingException(String msg) {
        super(msg);
    }
}
