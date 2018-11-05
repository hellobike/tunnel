package com.hellobike.base.tunnel.config;

import com.hellobike.base.tunnel.filter.IEventFilter;

import java.util.List;

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
 * @author machunxiao create at 2018-12-10
 */
public class HBaseConfig {

    private String table;
    private List<String> pks;
    private String hbaseTable;
    private List<String> hbaseKey;

    private String family;
    private String qualifier;

    private String quorum;

    private List<IEventFilter> filters;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getPks() {
        return pks;
    }

    public void setPks(List<String> pks) {
        this.pks = pks;
    }

    public String getHbaseTable() {
        return hbaseTable;
    }

    public void setHbaseTable(String hbaseTable) {
        this.hbaseTable = hbaseTable;
    }

    public List<String> getHbaseKey() {
        return hbaseKey;
    }

    public void setHbaseKey(List<String> hbaseKey) {
        this.hbaseKey = hbaseKey;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public String getQuorum() {
        return quorum;
    }

    public void setQuorum(String quorum) {
        this.quorum = quorum;
    }

    public List<IEventFilter> getFilters() {
        return filters;
    }

    public void setFilters(List<IEventFilter> filters) {
        this.filters = filters;
    }
}
