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

package com.hellobike.base.tunnel.spi.es;

import com.alibaba.druid.pool.DruidDataSource;
import com.hellobike.base.tunnel.spi.api.CollectionUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author machunxiao create at 2019-01-04
 */
final class EsJdbcManager {

    private static final Map<String, DruidDataSource> DATA_SOURCES = new ConcurrentHashMap<>();

    public static List<Map<String, Object>> query(String sql, String slot, String url, String username, String password) {

        QueryRunner qr = new QueryRunner();
        Connection connection = null;
        try {
            connection = getConnection(slot, url, username, password);
            List<Map<String, Object>> result = qr.query(connection, sql, new MapListHandler());
            if (CollectionUtils.isNotEmpty(result)) {
                return result;
            }
        } catch (SQLException e) {
            //
        } finally {
            close(connection);
        }
        return new ArrayList<>();
    }

    public static void close() {
        DATA_SOURCES.values().forEach(DruidDataSource::close);
        DATA_SOURCES.clear();
    }

    private static Connection getConnection(String slot, String url, String username, String password) throws SQLException {
        return getDruidDataSource(slot, url, username, password).getConnection();
    }

    private static DruidDataSource getDruidDataSource(String slot, String url, String username, String password) {
        synchronized (EsJdbcManager.class) {
            DruidDataSource dataSource = DATA_SOURCES.get(slot);
            if (dataSource == null) {
                dataSource = createDruidDataSource(url, username, password);
                DATA_SOURCES.put(slot, dataSource);
            }
            return dataSource;
        }
    }

    private static DruidDataSource createDruidDataSource(String url, String username, String password) {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setValidationQuery("select 1");
        dataSource.setMinIdle(20);
        dataSource.setMaxActive(50);
        return dataSource;
    }

    private static void close(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                //
            }
        }
    }

    private EsJdbcManager() {
    }

}
