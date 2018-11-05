package com.hellobike.base.tunnel.model;

import java.io.Serializable;
import java.util.Objects;

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
public class ColumnData implements Serializable {

    private static final long serialVersionUID = 4767055418095657107L;
    private String name;
    private String dataType;
    private String value;

    public ColumnData() {
    }

    public ColumnData(String name, String dataType, String value) {
        this.name = name;
        this.dataType = dataType;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnData data = (ColumnData) o;
        return Objects.equals(name, data.name) &&
                Objects.equals(dataType, data.dataType) &&
                Objects.equals(value, data.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, value);
    }

    @Override
    public String toString() {
        return "ColumnData{" +
                "name='" + name + '\'' +
                ", dataType='" + dataType + '\'' +
                ", value=" + value +
                '}';
    }
}
