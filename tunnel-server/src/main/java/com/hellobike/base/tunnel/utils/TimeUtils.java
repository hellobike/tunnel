package com.hellobike.base.tunnel.utils;

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

/**
 * @author machunxiao
 */
public class TimeUtils {

    private TimeUtils() {
    }

    public static void sleepInMills(long mills) {
        if (mills <= 0) {
            return;
        }
        try {
            Thread.sleep(mills);
        } catch (InterruptedException e) {
            //
        }

    }

    public static void sleepOneSecond(long s, long e) {
        long cost = 1000 + s - e;
        TimeUtils.sleepInMills(cost);
    }
}
