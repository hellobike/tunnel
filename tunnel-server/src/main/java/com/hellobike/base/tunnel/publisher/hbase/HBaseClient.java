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

package com.hellobike.base.tunnel.publisher.hbase;

import com.hellobike.base.tunnel.config.HBaseConfig;
import com.hellobike.base.tunnel.model.ColumnData;
import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.monitor.TunnelMonitorFactory;
import com.hellobike.base.tunnel.publisher.BasePublisher;
import com.hellobike.base.tunnel.utils.DefaultObjectPoolFactory;
import com.hellobike.base.tunnel.utils.NamedThreadFactory;
import com.hellobike.base.tunnel.utils.ObjectManager;
import com.hellobike.base.tunnel.utils.ObjectPool;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hellobike.base.tunnel.utils.TimeUtils.sleepOneSecond;


/**
 * @author machunxiao create at 2018-11-27
 */
public class HBaseClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseClient.class);
    private static final int MAX_CACHE = 10240;
    private static final int THREAD_NUM = 4;

    private static volatile HBaseClient instance = null;
    private final Configuration cfg;
    private final ObjectPool<Connection> pool;
    private final ArrayBlockingQueue<InsertHelper> insertQueue = new ArrayBlockingQueue<>(40960);
    private final ArrayBlockingQueue<DeleteHelper> deleteQueue = new ArrayBlockingQueue<>(40960);
    private final ThreadPoolExecutor insertExecutor;
    private final ThreadPoolExecutor deleteExecutor;
    private Map<String, Table> tables = new ConcurrentHashMap<>();

    private volatile boolean started;

    private HBaseClient() {
        this.cfg = HBaseConfiguration.create();
        this.pool = new DefaultObjectPoolFactory().createObjectPool(new ConnManager());
        this.insertExecutor = new ThreadPoolExecutor(THREAD_NUM, THREAD_NUM, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(20480), new NamedThreadFactory("HBaseInsertThread"));
        this.deleteExecutor = new ThreadPoolExecutor(THREAD_NUM, THREAD_NUM, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(20480), new NamedThreadFactory("HBaseDeleteThread"));
        started = true;
        for (int i = 0; i < THREAD_NUM; i++) {
            this.insertExecutor.submit(new InsertTask());
            this.deleteExecutor.submit(new DeleteTask());
        }
    }

    private HBaseClient(String quorum) {
        this.cfg = HBaseConfiguration.create();
        this.cfg.set("hbase.zookeeper.quorum", quorum);
        this.pool = new DefaultObjectPoolFactory().createObjectPool(new ConnManager());
        this.insertExecutor = new ThreadPoolExecutor(THREAD_NUM, THREAD_NUM, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(20480), new NamedThreadFactory("HBaseInsertThread"));
        this.deleteExecutor = new ThreadPoolExecutor(THREAD_NUM, THREAD_NUM, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(20480), new NamedThreadFactory("HBaseDeleteThread"));
        started = true;
        for (int i = 0; i < THREAD_NUM; i++) {
            this.insertExecutor.submit(new InsertTask());
            this.deleteExecutor.submit(new DeleteTask());
        }
    }

    public static HBaseClient getInstance() {
        if (instance == null) {
            synchronized (HBaseClient.class) {
                if (instance == null) {
                    instance = new HBaseClient();
                }
            }
        }
        return instance;
    }

    public static HBaseClient getInstance(String quorum) {
        if (instance == null) {
            synchronized (HBaseClient.class) {
                if (instance == null) {
                    instance = new HBaseClient(quorum);
                }
            }
        }
        return instance;
    }

    public void insert(HBaseConfig config, Event event) {
        String rowKey = generateRowKey(config, event.getDataList());
        if (StringUtils.isBlank(rowKey)) {
            return;
        }
        Put put = new Put(Bytes.toBytes(rowKey));
        for (ColumnData cd : event.getDataList()) {
            put.addColumn(Bytes.toBytes(config.getFamily()), Bytes.toBytes(cd.getName()), Bytes.toBytes(cd.getValue()));
        }
        InsertHelper ih = createInsertHelper(config, event, put);
        try {
            insertQueue.put(ih);
        } catch (Exception ignore) {
            //
        }
        if (insertQueue.size() >= MAX_CACHE) {
            doInsert();
        }
    }

    public void update(HBaseConfig config, Event event) {
        insert(config, event);
    }

    public void delete(HBaseConfig config, Event event) {

        String rowKey = generateRowKey(config, event.getDataList());
        if (StringUtils.isBlank(rowKey)) {
            return;
        }
        DeleteHelper dh = createDeleteHelper(config, event, rowKey);
        try {
            deleteQueue.put(dh);
        } catch (Exception ignore) {
            //
        }
        if (deleteQueue.size() >= MAX_CACHE) {
            doDelete();
        }
    }

    public void close() {
        this.started = true;
        this.doDelete();
        this.doInsert();
        this.insertExecutor.shutdown();
        this.deleteExecutor.shutdown();
        this.pool.close();
    }

    private DeleteHelper createDeleteHelper(HBaseConfig config, Event event, String rowKey) {
        DeleteHelper dh = new DeleteHelper();
        dh.table = config.getHbaseTable();
        dh.schema = event.getSchema();
        dh.slotName = event.getSlotName();
        dh.delete = new Delete(Bytes.toBytes(rowKey));
        return dh;
    }

    private InsertHelper createInsertHelper(HBaseConfig config, Event event, Put put) {
        InsertHelper ih = new InsertHelper();
        ih.table = config.getHbaseTable();
        ih.schema = event.getSchema();
        ih.slotName = event.getSlotName();
        ih.put = put;
        return ih;
    }

    private void doDelete() {
        List<DeleteHelper> deletes = pollFromQueue(deleteQueue);
        if (deletes.isEmpty()) {
            return;
        }
        long s = System.currentTimeMillis();
        Map<String, List<Delete>> data = deletes.stream()
                .collect(Collectors.groupingBy(dh -> dh.table, Collectors.mapping(dh -> dh.delete, Collectors.toList())));

        Connection conn = pool.borrowObject();

        try {
            data.forEach((tableName, deleteList) -> {
                try {
                    Table table = getTable(tableName, conn);
                    table.delete(deleteList);
                } catch (Exception e) {
                    logError(e.getMessage(), e);
                }
            });
            long e = System.currentTimeMillis();
            LOGGER.info("delete msg success.queue:{},data:{},thread:{},cost:{}ms", deleteQueue.size(), deletes.size(), Thread.currentThread().getName(), (e - s));
        } finally {
            pool.returnObject(conn);
            monitor(deletes);
        }
    }

    private void doInsert() {
        List<InsertHelper> inserts = pollFromQueue(insertQueue);
        if (inserts.isEmpty()) {
            return;
        }

        long s = System.currentTimeMillis();

        Map<String, List<Put>> data = inserts.stream()
                .collect(Collectors.groupingBy(ih -> ih.table, Collectors.mapping(ih -> ih.put, Collectors.toList())));

        Connection conn = pool.borrowObject();

        try {
            data.forEach((tableName, insertList) -> {
                try {
                    Table table = getTable(tableName, conn);
                    table.put(insertList);
                } catch (Exception e) {
                    logError(e.getMessage(), e);
                }
            });
            long e = System.currentTimeMillis();
            LOGGER.info("insert msg success.queue:{},data:{},thread:{},cost:{}ms", insertQueue.size(), inserts.size(), Thread.currentThread().getName(), (e - s));
        } finally {
            pool.returnObject(conn);
            monitor(inserts);
        }
    }

    private void monitor(List<? extends BaseHelper> helpers) {
        Map<String, Long> map = helpers.stream().collect(Collectors.groupingBy(helper -> helper.schema + "@@" + helper.slotName + "@@" + helper.table + "@@hbase", Collectors.counting()));
        BasePublisher.mapToStatics(map).forEach(statics ->
                TunnelMonitorFactory.getTunnelMonitor().collect(statics)
        );
    }

    private <T> List<T> pollFromQueue(ArrayBlockingQueue<T> queue) {
        int capacity = Math.min(MAX_CACHE, queue.size());
        List<T> list = new ArrayList<>(capacity);
        queue.drainTo(list, capacity);
        return list;
    }

    private Table getTable(String tableName, Connection conn) throws IOException {
        Table table = tables.get(tableName);
        if (table == null) {
            tables.putIfAbsent(tableName, getOrCreateTable(tableName, conn));
            table = tables.get(tableName);
        }
        return table;
    }

    private String generateRowKey(HBaseConfig config, List<ColumnData> data) {
        Map<String, String> columnKeyVal = data.stream().collect(Collectors.toMap(ColumnData::getName, ColumnData::getValue));
        return config.getHbaseKey().stream()
                .map(columnKeyVal::get)
                .filter(Objects::nonNull)
                .reduce(((s1, s2) -> s1 + "_" + s2))
                .orElse(null);
    }

    private synchronized Table getOrCreateTable(String tableName, Connection conn) throws IOException {
        TableName tb = TableName.valueOf(tableName);
        return conn.getTable(tb);
    }

    private void logError(String msg, Throwable t) {
        LOGGER.error(msg, t);
    }

    private Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection(cfg);
    }

    private class ConnManager implements ObjectManager<Connection> {

        @Override
        public Connection newInstance() throws Exception {
            return getConnection();
        }

        @Override
        public void releaseInstance(Connection connection) {
            try {
                connection.close();
            } catch (IOException e) {
                //
            }
        }

        @Override
        public boolean validateInstance(Connection connection) {
            return !connection.isClosed();
        }

    }

    private abstract class BaseHelper {
        String table;
        String schema;
        String slotName;
    }

    private class InsertHelper extends BaseHelper {
        Put put;
    }

    private class DeleteHelper extends BaseHelper {
        Delete delete;
    }

    private class InsertTask implements Runnable {
        @Override
        public void run() {
            while (started) {
                long s = System.currentTimeMillis();
                try {
                    doInsert();
                } catch (Throwable e) {
                    logError("InsertTask Occurred Error", e);
                } finally {
                    sleepOneSecond(s, System.currentTimeMillis());
                }
            }
        }
    }

    private class DeleteTask implements Runnable {
        @Override
        public void run() {
            while (started) {
                long s = System.currentTimeMillis();
                try {
                    doDelete();
                } catch (Throwable e) {
                    logError("DeleteTask Occurred Error", e);
                } finally {
                    sleepOneSecond(s, System.currentTimeMillis());
                }
            }
        }
    }

}
