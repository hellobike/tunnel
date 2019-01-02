package com.hellobike.base.tunnel.model;

import com.hellobike.base.tunnel.publisher.IPublisher;

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
 * @author machunxiao create at 2018-12-14
 */
public class InvokeContext {

    private String serverId;
    private String slotName;

    private String jdbcUrl;
    private String jdbcUser;
    private String jdbcPass;

    private long lsn;
    private String message;


    private String xid;
    private Event event;

    private IPublisher.Callback callback;

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public String getSlotName() {
        return slotName;
    }

    public void setSlotName(String slotName) {
        this.slotName = slotName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getJdbcUser() {
        return jdbcUser;
    }

    public void setJdbcUser(String jdbcUser) {
        this.jdbcUser = jdbcUser;
    }

    public String getJdbcPass() {
        return jdbcPass;
    }

    public void setJdbcPass(String jdbcPass) {
        this.jdbcPass = jdbcPass;
    }

    public long getLsn() {
        return lsn;
    }

    public void setLsn(long lsn) {
        this.lsn = lsn;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getXid() {
        return xid;
    }

    public void setXid(String xid) {
        this.xid = xid;
    }

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public IPublisher.Callback getCallback() {
        return callback;
    }

    public void setCallback(IPublisher.Callback callback) {
        this.callback = callback;
    }

}
