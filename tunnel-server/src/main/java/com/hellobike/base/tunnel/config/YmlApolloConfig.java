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

package com.hellobike.base.tunnel.config;

import com.hellobike.base.tunnel.apollo.ApolloConfig;

public class YmlApolloConfig extends ApolloConfig {

    private String pg_dump_path;

    public String getPg_dump_path() {
        return pg_dump_path;
    }

    public void setPg_dump_path(String pg_dump_path) {
        this.setPgDumpPath(pg_dump_path);
        this.pg_dump_path = pg_dump_path;
    }

    public static class YmlHiveConf extends HiveConf {
        private String hdfs_address;
        private String data_dir;
        private String table_name;

        public String getHdfs_address() {
            return hdfs_address;
        }

        public void setHdfs_address(String hdfs_address) {
            this.setHdfsAddress(hdfs_address);
            this.hdfs_address = hdfs_address;
        }

        public String getData_dir() {
            return data_dir;
        }

        public void setData_dir(String data_dir) {
            this.setDataDir(data_dir);
            this.data_dir = data_dir;
        }

        public String getTable_name() {
            return table_name;
        }

        public void setTable_name(String table_name) {
            this.setTableName(table_name);
            this.table_name = table_name;
        }
    }

}
