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
import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.utils.MsgUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author machunxiao create at 2018-11-28
 */
public class HiveClient implements AutoCloseable {

    private final DruidDataSource dataSource;

    private final HiveConfig hiveConfig;

    public HiveClient(HiveConfig hiveConfig) {
        this.hiveConfig = hiveConfig;
        this.dataSource = new DruidDataSource();
        initDataSourceConfig();
    }

    public void insert(Event event) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            String sql = MsgUtils.toInsert(event);
            stmt.execute(sql);
        } catch (SQLException e) {
            //
        }
    }

    public void update(Event event) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            String sql = MsgUtils.toUpdate(event, this.hiveConfig.getPks());
            stmt.execute(sql);
        } catch (SQLException e) {
            //
        }
    }

    public void delete(Event event) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            String sql = MsgUtils.toDelete(event, this.hiveConfig.getPks());
            stmt.execute(sql);
        } catch (SQLException e) {
            //
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

}
