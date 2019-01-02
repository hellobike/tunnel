package com.hellobike.base.tunnel.filter;

import com.hellobike.base.tunnel.model.ColumnData;
import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.spi.api.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

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
 * @author machunxiao 2018-10-30
 */
public class ColumnFilter implements IEventFilter {

    private final List<String> columnNames;

    public ColumnFilter(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public boolean filter(Event event) {

        if (CollectionUtils.isEmpty(columnNames)) {
            return true;
        }

        List<ColumnData> dataList = event.getDataList()
                .stream()
                .filter(cd -> {
                    String name = cd.getName();
                    return columnNames
                            .stream()
                            .anyMatch(s -> s.contains(name));
                }).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(dataList)) {
            return false;
        }

        return true;
    }

}
