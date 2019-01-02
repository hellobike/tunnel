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

package com.hellobike.base.tunnel.publisher.hive;

import com.alibaba.druid.pool.DruidDataSource;
import com.hellobike.base.tunnel.model.ColumnData;
import com.hellobike.base.tunnel.model.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author machunxiao create at 2018-11-28
 */
@Slf4j
@Deprecated
public class HiveClient implements AutoCloseable {

    private final DruidDataSource dataSource;

    private final HiveConfig hiveConfig;

    public HiveClient(HiveConfig hiveConfig) {
        this.hiveConfig = hiveConfig;
        this.dataSource = new DruidDataSource();
        initDataSourceConfig();
    }

    public void insert(HiveRule rule, Event event) {
        String sql = toInsertSQL(rule, event);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(sql);
            log.info("execute hive insert sql:{} success", sql);
        } catch (Exception e) {
            //
            log.info("execute hive insert sql:{} failure", sql, e);
        }
    }

    public void update(HiveRule rule, Event event) {
        String sql = toUpdateSQL(rule, event);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            log.info("execute hive update sql:{} success", sql);
        } catch (Exception e) {
            //
            log.info("execute hive update sql:{} failure", sql, e);
        }
    }

    public void delete(HiveRule rule, Event event) {
        String sql = toDeleteSQL(rule, event);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            log.info("execute hive delete sql:{} success", sql);
        } catch (Exception e) {
            //
            log.info("execute hive delete sql:{} failure", sql, e);
        }
    }

    @Override
    public void close() {
        this.dataSource.close();
    }

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    private void initDataSourceConfig() {
        this.dataSource.setUsername(this.hiveConfig.getUsername());
        this.dataSource.setPassword(this.hiveConfig.getPassword());
        this.dataSource.setUrl(this.hiveConfig.getHiveUrl());
        this.dataSource.setValidationQuery("select 1");
    }

    private String toInsertSQL(HiveRule rule, Event event) {
        Map<String, String> kv = event.getDataList().stream()
                .collect(Collectors.toMap(ColumnData::getName, ColumnData::getValue));
        List<String> fields = rule.getFields();
        List<String> values = new ArrayList<>();
        for (String field : fields) {
            String value = kv.get(field);
            values.add("'" + value + "'");
        }
        return "insert into " + rule.getHiveTable() + "(" + StringUtils.join(fields, ",") + ") values(" + StringUtils.join(values, ",") + ")";
    }

    private String toUpdateSQL(HiveRule rule, Event event) {
        Map<String, String> kv = event.getDataList().stream()
                .collect(Collectors.toMap(ColumnData::getName, ColumnData::getValue));
        StringBuilder out = new StringBuilder();
        kv.forEach((k, v) -> out.append(k).append("=").append("'").append(v).append("',"));
        out.setCharAt(out.length() - 1, ' ');
        return "update " + rule.getHiveTable() + " set " + out.toString() + " where " + getPkValue(rule.getPks(), event.getDataList());
    }

    private String toDeleteSQL(HiveRule rule, Event event) {
        return "delete from " + rule.getHiveTable() + " where " + getPkValue(rule.getPks(), event.getDataList());
    }

    private String getPkValue(List<String> pks, List<ColumnData> columnDataList) {
        // (k1,k2) = (k1,k2)
        LinkedHashMap<String, String> tmp = new LinkedHashMap<>();
        for (String pk : pks) {
            tmp.put(pk, "");
        }
        for (ColumnData columnData : columnDataList) {
            String val = tmp.get(columnData.getName());
            if ("".equals(val)) {
                tmp.put(columnData.getName(), columnData.getValue());
            }
        }
        List<String> keys = new ArrayList<>();
        List<String> vals = new ArrayList<>();

        for (Map.Entry<String, String> e : tmp.entrySet()) {
            String key = e.getKey();
            String val = e.getValue();
            keys.add(key);
            vals.add("'" + val + "'");
        }

        return "(" + StringUtils.join(keys, ",") + ") = (" + StringUtils.join(vals, ",") + ")";
    }


}
