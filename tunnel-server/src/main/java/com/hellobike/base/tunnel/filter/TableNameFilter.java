package com.hellobike.base.tunnel.filter;

import com.hellobike.base.tunnel.model.Event;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class TableNameFilter implements IEventFilter {

    private final String tableNameExp;
    private final Pattern pattern;

    public TableNameFilter(String tableNameExp) {
        this.tableNameExp = StringUtils.isBlank(tableNameExp) ? ".*" : tableNameExp;
        this.pattern = Pattern.compile(this.tableNameExp);
    }

    @Override
    public boolean filter(Event event) {
        if (event == null || event.getTable() == null) {
            return false;
        }
        Matcher matcher = pattern.matcher(event.getTable());
        return matcher.matches();
    }

}
