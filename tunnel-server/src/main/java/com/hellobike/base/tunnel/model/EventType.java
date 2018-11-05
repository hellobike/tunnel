package com.hellobike.base.tunnel.model;

import java.io.Serializable;

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
 * @author machunxiao 2018-10-25
 */
public enum EventType implements Serializable {
    // sql 语句类型
    BEGIN,
    COMMIT,
    INSERT,
    UPDATE,
    DELETE;

    private static final long serialVersionUID = 1L;

    public static EventType getEventType(String message) {
        if (EventType.INSERT.name().equalsIgnoreCase(message)) {
            return EventType.INSERT;
        } else if (EventType.DELETE.name().equalsIgnoreCase(message)) {
            return EventType.DELETE;
        } else if (EventType.UPDATE.name().equalsIgnoreCase(message)) {
            return EventType.UPDATE;
        } else if (EventType.BEGIN.name().equalsIgnoreCase(message)) {
            return EventType.BEGIN;
        } else if (EventType.COMMIT.name().equalsIgnoreCase(message)) {
            return EventType.COMMIT;
        } else {
            throw new IllegalArgumentException("unsupported event:" + message);
        }
    }
}
