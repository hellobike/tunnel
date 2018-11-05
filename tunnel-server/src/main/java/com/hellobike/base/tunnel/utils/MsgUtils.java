package com.hellobike.base.tunnel.utils;

import com.hellobike.base.tunnel.model.Event;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
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
public class MsgUtils {


    private MsgUtils() {
    }

    public static String toUpdate(Event event, List<String> pkNames) {
        String schema = event.getSchema();
        String table = event.getTable();

        String kv = event.getDataList().stream().map(cd -> cd.getName() + "=" + cd.getValue()).reduce((s1, s2) -> s1 + "," + s2).orElse("");
        String pk = getPKValues(event, pkNames);

        return "update " + schema + "." + table + " set " + pk + " where " + kv;
    }

    public static String toDelete(Event event, List<String> pkNames) {
        String schema = event.getSchema();
        String table = event.getTable();
        String pk = getPKValues(event, pkNames);
        return "delete from " + schema + "." + table + " where " + pk;
    }

    public static String toInsert(Event event) {
        String schema = event.getSchema();
        String table = event.getTable();

        List<String> keys = new ArrayList<>();
        List<String> vals = new ArrayList<>();
        event.getDataList().forEach(cd -> {
            keys.add(cd.getName());
            vals.add("'" + cd.getValue() + "'");
        });

        return "insert into " + schema + "." + table + "(" + StringUtils.join(keys, ',') + ") values(" + StringUtils.join(vals, ',') + ")";
    }

    private static String getPKValues(Event event, List<String> pkNames) {
        return event.getDataList().stream()
                .filter(cd -> pkNames.contains(cd.getName()))
                .map(cd -> cd.getName() + "=" + cd.getValue())
                .reduce((s1, s2) -> s1 + "," + s2)
                .orElse("");

    }

}
