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

import java.util.List;
import java.util.Map;

/**
 * 这个是兼容代码
 *
 * @author machunxiao 2018-11-01
 */
public class ApolloConfig {

    @JSONField(name = "pg_dump_path")
    private String pgDumpPath;
    private List<Subscribe> subscribes;

    public String getPgDumpPath() {
        return pgDumpPath;
    }

    public void setPgDumpPath(String pgDumpPath) {
        this.pgDumpPath = pgDumpPath;
    }

    public List<Subscribe> getSubscribes() {
        return subscribes;
    }

    public void setSubscribes(List<Subscribe> subscribes) {
        this.subscribes = subscribes;
    }

    public static class Subscribe {

        private String slotName;
        private PgConnConf pgConnConf;
        private List<Rule> rules;
        private KafkaConf kafkaConf;
        private EsConf esConf;
        private HBaseConf hbaseConf;
        private HiveConf hiveConf;
        private HdfsConf hdfsConf;


        public String getSlotName() {
            return slotName;
        }

        public void setSlotName(String slotName) {
            this.slotName = slotName;
        }

        public PgConnConf getPgConnConf() {
            return pgConnConf;
        }

        public void setPgConnConf(PgConnConf pgConnConf) {
            this.pgConnConf = pgConnConf;
        }

        public List<Rule> getRules() {
            return rules;
        }

        public void setRules(List<Rule> rules) {
            this.rules = rules;
        }

        public KafkaConf getKafkaConf() {
            return kafkaConf;
        }

        public void setKafkaConf(KafkaConf kafkaConf) {
            this.kafkaConf = kafkaConf;
        }

        public EsConf getEsConf() {
            return esConf;
        }

        public void setEsConf(EsConf esConf) {
            this.esConf = esConf;
        }

        public HBaseConf getHbaseConf() {
            return hbaseConf;
        }

        public void setHbaseConf(HBaseConf hbaseConf) {
            this.hbaseConf = hbaseConf;
        }

        public HiveConf getHiveConf() {
            return hiveConf;
        }

        public void setHiveConf(HiveConf hiveConf) {
            this.hiveConf = hiveConf;
        }

        public HdfsConf getHdfsConf() {
            return hdfsConf;
        }

        public void setHdfsConf(HdfsConf hdfsConf) {
            this.hdfsConf = hdfsConf;
        }
    }

    public static class PgConnConf {
        private String host;
        private int port;
        private String database;
        private String schema;
        private String user;
        private String password;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public static class KafkaConf {
        private List<String> addrs;

        public List<String> getAddrs() {
            return addrs;
        }

        public void setAddrs(List<String> addrs) {
            this.addrs = addrs;
        }
    }

    public static class EsConf {
        private String addrs;

        public String getAddrs() {
            return addrs;
        }

        public void setAddrs(String addrs) {
            this.addrs = addrs;
        }
    }

    public static class HBaseConf {
        private String zkquorum;

        public String getZkquorum() {
            return zkquorum;
        }

        public void setZkquorum(String zkquorum) {
            this.zkquorum = zkquorum;
        }
    }

    public static class HiveConf {
    }

    public static class HdfsConf {
    }

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

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public List<String> getPks() {
            return pks;
        }

        public void setPks(List<String> pks) {
            this.pks = pks;
        }

        public List<String> getEsid() {
            return esid;
        }

        public void setEsid(List<String> esid) {
            this.esid = esid;
        }

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, String> getFields() {
            return fields;
        }

        public void setFields(Map<String, String> fields) {
            this.fields = fields;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public List<String> getParameters() {
            return parameters;
        }

        public void setParameters(List<String> parameters) {
            this.parameters = parameters;
        }

        public String getFamily() {
            return family;
        }

        public void setFamily(String family) {
            this.family = family;
        }

        public String getQualifier() {
            return qualifier;
        }

        public void setQualifier(String qualifier) {
            this.qualifier = qualifier;
        }

        public List<String> getRowKeys() {
            return rowKeys;
        }

        public void setRowKeys(List<String> rowKeys) {
            this.rowKeys = rowKeys;
        }

        public String getHbaseTable() {
            return hbaseTable;
        }

        public void setHbaseTable(String hbaseTable) {
            this.hbaseTable = hbaseTable;
        }

        public List<String> getHbaseKey() {
            return hbaseKey;
        }

        public void setHbaseKey(List<String> hbaseKey) {
            this.hbaseKey = hbaseKey;
        }
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
    public static class Join {

        private String table;
        private String sql;
        private List<String> parameters;

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public List<String> getParameters() {
            return parameters;
        }

        public void setParameters(List<String> parameters) {
            this.parameters = parameters;
        }
    }

}
