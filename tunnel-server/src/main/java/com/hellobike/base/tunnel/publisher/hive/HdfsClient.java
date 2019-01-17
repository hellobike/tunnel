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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author machunxiao create at 2019-01-15
 */
@Slf4j
public class HdfsClient implements AutoCloseable {

    private final DruidDataSource dataSource;
    private final HiveConfig hiveConfig;

    public HdfsClient(HiveConfig hiveConfig) {
        this.hiveConfig = hiveConfig;
        this.dataSource = new DruidDataSource();
        initDataSourceConfig();
    }

    public void insert(HiveRule rule, Event event) {

        Map<String, String> kv = event.getDataList().stream()
                .collect(Collectors.toMap(ColumnData::getName, ColumnData::getValue));
        Optional<String> opt = rule.getFields().stream()
                .map(kv::get)
                .filter(Objects::nonNull)
                .reduce((s1, s2) -> s1 + "," + s2);
        if (!opt.isPresent() || StringUtils.isBlank(opt.get())) {
            return;
        }
        String value = opt.get();

        try {
            Configuration hadoopConfig = new Configuration();
            hadoopConfig.set("fs.defaultFS", this.hiveConfig.getHdfsAddresses()[0]);

            FileSystem fs = FileSystem.get(hadoopConfig);

            String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
            String dir = hiveConfig.getDataDir() + "/" + today;

            Path parent = new Path(dir);
            if (!fs.exists(parent)) {
                fs.mkdirs(parent);
            }

            if (!appendPartition(dir, today)) {
                return;
            }

            String file = dir + "/" + today + ".xxx";
            Path hdfsPath = new Path(file);
            FSDataOutputStream fileOutputStream = null;
            BufferedWriter bw = null;
            try {
                if (fs.exists(hdfsPath)) {
                    fileOutputStream = fs.append(hdfsPath);
                } else {
                    fileOutputStream = fs.create(hdfsPath);
                }

                bw = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
                bw.write(value);
                bw.newLine();
                bw.flush();

                log.warn("WriteData To Hdfs Success");
            } finally {
                fs.close();
                if (fileOutputStream != null) {
                    fileOutputStream.close();
                }
                if (bw != null) {
                    bw.close();
                }
            }
        } catch (IOException e) {
            log.warn("WriteData To Hdfs Failure", e);
        }
    }

    @Override
    public void close() {
        this.dataSource.close();
    }

    private boolean appendPartition(String dataDir, String partition) {
        String sql = String.format("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s='%s') LOCATION '%s'",
                this.hiveConfig.getTable(), this.hiveConfig.getPartition(), partition, dataDir);
        try (Connection conn = this.dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            log.info("execute alter table success");
            return true;
        } catch (Exception e) {
            log.info("execute alter table failure", e);
        }
        return false;
    }

    private void initDataSourceConfig() {
        this.dataSource.setUsername(this.hiveConfig.getUsername());
        this.dataSource.setPassword(this.hiveConfig.getPassword());
        this.dataSource.setMaxActive(100);
        this.dataSource.setMinIdle(50);
        this.dataSource.setUrl(this.hiveConfig.getHiveUrl());
        this.dataSource.setValidationQuery("select 1");
    }
}
