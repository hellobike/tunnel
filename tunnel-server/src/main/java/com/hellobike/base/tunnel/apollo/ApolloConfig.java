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

package com.hellobike.base.tunnel.apollo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * 这个是兼容代码
 *
 * @author machunxiao 2018-11-01
 */
@Data
public class ApolloConfig {

    @JSONField(name = "pg_dump_path")
    private String pgDumpPath;
    private List<Subscribe> subscribes;

    @Data
    public static class Subscribe {

        private String slotName;
        private PgConnConf pgConnConf;
        private List<Rule> rules;
        private KafkaConf kafkaConf;
        private EsConf esConf;
        private HBaseConf hbaseConf;
        private HiveConf hiveConf;
        private HdfsConf hdfsConf;

    }

    @Data
    public static class PgConnConf {
        private String host;
        private int port;
        private String database;
        private String schema;
        private String user;
        private String password;
    }

    @Data
    public static class KafkaConf {
        private List<String> addrs;
    }

    @Data
    public static class EsConf {
        private String addrs;
    }

    @Data
    public static class HBaseConf {
        private String zkquorum;
    }

    @Data
    public static class HiveConf {
        private String host;
        private int port;
        private String user;
        private String password;

        @JSONField(name = "hdfs_address")
        private String hdfsAddress;
        @JSONField(name = "data_dir")
        private String dataDir;
        @JSONField(name = "table_name")
        private String tableName;
        private String partition;

    }

    @Data
    public static class HdfsConf {
        private String address;
        private String file;
    }

    @Data
    public static class Rule {

        private String table;

        private String topic;
        private int partition;

        private List<String> pks;
        private List<String> esid;
        private String index;
        private String type;
        private Map<String, String> fields;


        private String sql;
        private List<String> parameters;

        private String family;
        private String qualifier;
        private List<String> rowKeys;

        private String hbaseTable;
        private List<String> hbaseKey;

        private String hiveTable;
        private List<String> hiveFields;

    }

    /**
     * <pre>
     *     例如在es索引中,一个复合索引 es_idx_t1包含10个字段,其中5个来自tb1,5个来自tb2,
     *     当tb1或者tb2有新增或删除的时候,查询另外一个表的内容,添加到索引中
     *
     *     val: insert into tb1(id1,name1,age1) values('id1','name1','age1');
     *
     *     sql: select id2,name2,age2 from tb2 where id = @id and name= @name
     *
     *     idx: append into es1(id1,id2,name1,name2,age1,age2) values()
     *
     *     假设目前只支持根据主键的join
     *
     * </pre>
     */
    @Data
    public static class Join {

        private String table;
        private String sql;
        private List<String> parameters;

    }

}
