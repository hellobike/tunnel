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

package com.hellobike.base.tunnel.publisher.es;

import com.hellobike.base.tunnel.config.EsConfig;
import com.hellobike.base.tunnel.model.ColumnData;
import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.model.InvokeContext;
import com.hellobike.base.tunnel.monitor.TunnelMonitorFactory;
import com.hellobike.base.tunnel.publisher.BasePublisher;
import com.hellobike.base.tunnel.publisher.IPublisher;
import com.hellobike.base.tunnel.utils.NamedThreadFactory;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.hellobike.base.tunnel.utils.TimeUtils.sleepOneSecond;

/**
 * @author machunxiao 2018-10-25
 */
public class EsPublisher extends BasePublisher implements IPublisher {

    private static final Logger                         /**/ LOG = LoggerFactory.getLogger(EsPublisher.class);
    private static final int                            /**/ MAX_CACHED = 10240;

    private final List<EsConfig>                        /**/ esConfigs;
    private final ThreadPoolExecutor                    /**/ executor;

    private final LinkedBlockingQueue<Helper>           /**/ requestHelperQueue;
    private final RestHighLevelClient[]                 /**/ restClients;


    private volatile boolean                            /**/ started;

    public EsPublisher(List<EsConfig> esConfigs) {
        this.esConfigs = esConfigs;
        int total = 8;
        this.executor = new ThreadPoolExecutor(total, total, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(5000), new NamedThreadFactory("EsSendThread"));

        this.restClients = new RestHighLevelClient[total];
        for (int i = 0; i < total; i++) {
            this.restClients[i] = newRestEsHighLevelClient();
        }
        this.requestHelperQueue = new LinkedBlockingQueue<>(81920);

        started = true;
        for (int i = 0; i < total; i++) {
            this.executor.submit(new Sender(i));
        }

    }

    @Override
    public void publish(Event event, Callback callback) {
        throw new UnsupportedOperationException();

    }

    @Override
    public void publish(InvokeContext context, Callback callback) {
        this.esConfigs.forEach(
                esConfig -> internalPublish(context, callback, esConfig)
        );
    }

    @Override
    public void close() {
        started = false;
        this.executor.shutdown();
        Arrays.stream(this.restClients).forEach(this::closeClosable);
        LOG.info("EsPublisher Closed");
    }

    private void closeClosable(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                //
            }
        }
    }

    private void internalPublish(InvokeContext context, Callback callback, EsConfig esConfig) {
        if (esConfig.getFilters() == null
                || esConfig.getFilters().isEmpty()
                || esConfig.getFilters().stream().allMatch(filter -> filter.filter(context.getEvent()))) {
            sendToEs(esConfig, context, callback);
        }
    }

    private void sendToEs(EsConfig esConfig, InvokeContext context, Callback callback) {

        try {

            requestHelperQueue.put(new Helper(context, esConfig, callback));

            onSuccess(callback);
        } catch (Exception e) {
            //
            LOG.error("Put Data To MemQueue Failure", e);
            onFailure(callback, e);
        }

        if (requestHelperQueue.size() >= MAX_CACHED) {
            forceFlushMemQueue(0);
        }

    }

    private DocWriteRequest eventToRequest(EsConfig esConfig, InvokeContext context) {

        DocWriteRequest req = null;

        Map<String, String> values = context.getEvent().getDataList()
                .stream()
                .collect(Collectors.toMap(ColumnData::getName, ColumnData::getValue));

        // column_name,column_name
        String id = esConfig.getEsIdFieldNames()
                .stream()
                .map(esId -> String.valueOf(values.get(esId)))
                .reduce((s1, s2) -> s1 + esConfig.getSeparator() + s2)
                .orElse("");

        if (StringUtils.isBlank(id)) {
            return null;
        }
        String type = esConfig.getType();
        String index = esConfig.getIndex();


        switch (context.getEvent().getEventType()) {
            case INSERT:
                IndexRequest ir = new IndexRequest(index, type, id);
                ir.opType(DocWriteRequest.OpType.CREATE);

                execSQL(esConfig, context, values);

                ir.source(values);
                req = ir;
                break;
            case UPDATE:
                UpdateRequest ur = new UpdateRequest(index, type, id);

                execSQL(esConfig, context, values);

                ur.doc(values);
                req = ur;
                break;
            case DELETE:
                DeleteRequest dr = new DeleteRequest(index, type, id);
                dr.id(id);
                req = dr;
                break;
            default:
                break;
        }
        return req;
    }

    private void execSQL(EsConfig esConfig, InvokeContext context, Map<String, String> values) {
        String sql = esConfig.getSql();
        List<String> parameters = esConfig.getParameters();
        if (sql != null && parameters != null) {
            // select * from tb1 where id=@id,name=@name
            for (String k : parameters) {
                sql = sql.replace("?", getValue(values.get(k)));
            }

            // 执行sql,获得结果集
            Map<String, Object> result = executeQuery(sql, context);
            Map<String, String> newValues = new LinkedHashMap<>();
            result.entrySet().stream()
                    .filter(e -> e.getValue() != null)
                    .forEach(e -> newValues.put(e.getKey(), String.valueOf(e.getValue())));
            if (!newValues.isEmpty()) {
                values.clear();
                values.putAll(newValues);
            }
        }
    }

    private String getValue(Object val) {
        if (val instanceof Number) {
            return val.toString();
        }
        return '\'' + val.toString() + '\'';
    }

    private void syncSend(RestHighLevelClient restClient, List<DocWriteRequest> doc) {

        long s = System.currentTimeMillis();
        BulkRequest br = createBulkRequest(doc);
        RequestOptions requestOptions = createRequestOptions();
        int retry = 10;

        while (retry > 0) {
            try {

                BulkResponse response = restClient.bulk(br, requestOptions);

                long e = System.currentTimeMillis();
                LOG.info("indexed doc:{},cost:{}ms,result:{}", doc.size(), e - s, response.hasFailures());
                return;
            } catch (Exception e) {
                //
                retry--;
                LOG.error("Send Data To Es Occurred Error,retry:" + (10 - retry), e);
            }
        }
    }

    private void asyncSend(RestHighLevelClient restClient, List<DocWriteRequest> doc) {
        BulkRequest br = createBulkRequest(doc);
        RequestOptions requestOptions = createRequestOptions();
        restClient.bulkAsync(br, requestOptions, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse responses) {
            }

            @Override
            public void onFailure(Exception e) {
            }
        });
    }

    private BulkRequest createBulkRequest(List<DocWriteRequest> doc) {
        BulkRequest br = new BulkRequest();
        br.add(doc);
        br.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        br.waitForActiveShards(ActiveShardCount.ONE);
        return br;
    }

    private RequestOptions createRequestOptions() {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader("Connection", "Keep-Alive");
        return builder.build();
    }

    private List<Helper> pollHelperFromQueue(BlockingQueue<Helper> queue) {
        int len = queue.size();
        int capacity = Math.min(MAX_CACHED / 2, len);
        List<Helper> helpers = new ArrayList<>(capacity);
        queue.drainTo(helpers, capacity);
        return helpers;
    }

    private Event findMaxEvent(List<Helper> helpers) {
        if (helpers == null || helpers.isEmpty()) {
            return null;
        }
        return helpers.stream()
                .max(Comparator.comparingLong(h1 -> h1.context.getLsn()))
                .map(helper -> helper.context.getEvent())
                .orElse(null);
    }

    private List<DocWriteRequest> transferToReq(List<Helper> helpers) {

        return helpers.stream()
                .map(helper -> eventToRequest(helper.config, helper.context))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private RestHighLevelClient newRestEsHighLevelClient() {
        return new RestHighLevelClient(RestClient.builder(
                this.esConfigs
                        .stream()
                        .map(esConfig -> HttpHost.create(esConfig.getServer()))
                        .toArray(HttpHost[]::new)
        ));
    }

    private void forceFlushMemQueue(int idx) {
        List<Helper> helpers = pollHelperFromQueue(requestHelperQueue);
        try {

            if (helpers.isEmpty()) {
                return;
            }

            syncSend(restClients[idx], transferToReq(helpers));

            Event maxEvent = findMaxEvent(helpers);
            if (maxEvent != null) {
                LOG.info("flush queue success,lsn:{}", maxEvent.getLsn());
            }
        } finally {
            if (!helpers.isEmpty()) {
                Map<String, Long> data = getMonitorData(helpers);
                mapToStatics(data).forEach(statics ->
                        TunnelMonitorFactory.getTunnelMonitor().collect(statics)
                );
            }
        }
    }

    private Map<String, Long> getMonitorData(List<Helper> helpers) {
        return helpers.stream()
                .map(helper -> {
                    helper.context.getEvent().setSlotName(helper.context.getSlotName());
                    return helper.context.getEvent();
                })
                .collect(Collectors.groupingBy(event ->
                        event.getSchema() + "@@" + event.getSlotName() + "@@" + event.getTable() + "@@es", Collectors.counting()));
    }

    private Map<String, Object> executeQuery(String sql, InvokeContext context) {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(context.getJdbcUrl(), context.getJdbcUser(), context.getJdbcPass());
            QueryRunner qr = new QueryRunner();
            Map<String, Object> query = qr.query(connection, sql, new MapHandler());
            if (query == null || query.isEmpty()) {
                query = new LinkedHashMap<>();
                LOG.warn("Select Nothing By SQL:{}", sql);
            }
            return query;
        } catch (Exception e) {
            //
        } finally {
            closeClosable(connection);
        }
        return new LinkedHashMap<>();
    }

    private static class Helper {

        final InvokeContext context;
        final EsConfig config;
        final Callback callback;

        private Helper(InvokeContext context, EsConfig config, Callback callback) {
            this.context = context;
            this.config = config;
            this.callback = callback;
        }
    }

    private class Sender implements Runnable {

        private final int idx;

        public Sender(int idx) {
            this.idx = idx;
        }

        @Override
        public void run() {
            while (started) {
                long s = System.currentTimeMillis();
                try {
                    forceFlushMemQueue(this.idx);
                } catch (Exception e) {
                    LOG.warn("flush data to es failure", e);
                } finally {
                    sleepOneSecond(s, System.currentTimeMillis());
                }
            }
        }

    }

}
