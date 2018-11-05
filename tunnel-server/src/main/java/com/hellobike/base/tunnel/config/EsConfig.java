package com.hellobike.base.tunnel.config;

import com.hellobike.base.tunnel.filter.IEventFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
 * @author machunxiao 2018-11-01
 */
public class EsConfig {

    private String server;
    private String schema;
    private String table;

    private String index;
    private String type;
    private Map<String, String> fieldMappings;
    private List<String> pkFieldNames;
    private List<String> esIdFieldNames;

    private String separator = ";";

    private String dstTable;
    private String sql;
    private List<String> parameters;

    private List<IEventFilter> filters = new ArrayList<>();

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, String> getFieldMappings() {
        return fieldMappings;
    }

    public void setFieldMappings(Map<String, String> fieldMappings) {
        this.fieldMappings = fieldMappings;
    }

    public List<String> getPkFieldNames() {
        return pkFieldNames;
    }

    public void setPkFieldNames(List<String> pkFieldNames) {
        this.pkFieldNames = pkFieldNames;
    }

    public List<String> getEsIdFieldNames() {
        return esIdFieldNames;
    }

    public void setEsIdFieldNames(List<String> esIdFieldNames) {
        this.esIdFieldNames = esIdFieldNames;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public List<IEventFilter> getFilters() {
        return filters;
    }

    public void setFilters(List<IEventFilter> filters) {
        this.filters = filters;
    }

    public String getDstTable() {
        return dstTable;
    }

    public void setDstTable(String dstTable) {
        this.dstTable = dstTable;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<String> getParameters() {
        return parameters;
    }

    public void setParameters(List<String> parameters) {
        this.parameters = parameters;
    }
}
