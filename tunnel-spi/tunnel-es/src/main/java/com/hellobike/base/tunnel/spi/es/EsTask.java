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

import com.hellobike.base.tunnel.spi.api.CellData;
import com.hellobike.base.tunnel.spi.api.CollectionUtils;
import com.hellobike.base.tunnel.spi.api.Constants;
import com.hellobike.base.tunnel.spi.api.Invocation;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * @author machunxiao create at 2019-01-03
 */
final class EsTask implements Runnable {

    private final EsClient esClient;
    private final List<Invocation> invocations;

    public EsTask(EsClient esClient, List<Invocation> invocations) {
        this.esClient = esClient;
        this.invocations = invocations;
    }

    @Override
    public void run() {
        if (CollectionUtils.isEmpty(this.invocations)) {
            return;
        }
        Map<Boolean, List<Invocation>> data = invocations.stream().collect(groupingBy(this::isJoin));
        List<Invocation> joinInvocations = data.get(Boolean.TRUE);
        List<Invocation> normalInvocations = data.get(Boolean.FALSE);

        if (CollectionUtils.isNotEmpty(joinInvocations)) {

            Map<String, List<Invocation>> groupData = joinInvocations.stream()
                    .collect(groupingBy(this::getGroupingByKey, LinkedHashMap::new, toList()));

            groupData.forEach((k, list) -> {

                String sql = "";
                String url = "";
                String slotName = "";
                String username = "";
                String password = "";

                List<Map<String, Object>> result = EsJdbcManager.query(sql, slotName, url, username, password);
                List<DocWriteRequest> doc = mapListToRequests(result);

                this.esClient.bulk(doc);
            });
        }

        if (CollectionUtils.isNotEmpty(normalInvocations)) {
            List<DocWriteRequest> doc = toRequests(normalInvocations);
            this.esClient.bulk(doc);
        }

    }

    private List<DocWriteRequest> mapListToRequests(List<Map<String, Object>> data) {
        return null;
    }

    private List<DocWriteRequest> toRequests(List<Invocation> invocations) {
        return invocations.stream()
                .map(this::toRequest)
                .filter(Objects::nonNull)
                .collect(toList());
    }

    private DocWriteRequest toRequest(Invocation invocation) {
        DocWriteRequest req = null;

        EsConfig esConfig = invocation.getParameter(Constants.CONFIG_NAME);

        Map<String, String> values = invocation.getEvent().getDataList()
                .stream()
                .collect(Collectors.toMap(CellData::getName, CellData::getValue));

        // column_name,column_name
        String id = esConfig.getEsId()
                .stream()
                .map(esId -> String.valueOf(values.get(esId)))
                .reduce((s1, s2) -> s1 + esConfig.getSeparator() + s2)
                .orElse("");

        if (StringUtils.isBlank(id)) {
            return null;
        }

        String type = esConfig.getType();
        String index = esConfig.getIndex();

        switch (invocation.getEvent().getWalType()) {
            case INSERT:
            case UPDATE:
                UpdateRequest ur = new UpdateRequest(index, type, id);
                ur.doc(values);
                ur.docAsUpsert(true);
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

    private boolean isJoin(Invocation invocation) {
        EsConfig config = invocation.getParameter(Constants.CONFIG_NAME);
        return config != null
                && StringUtils.isNotBlank(config.getJoinSql())
                && CollectionUtils.isNotEmpty(config.getParameters());
    }

    private String getGroupingByKey(Invocation invocation) {
        return invocation.getSlotName() + "_" + invocation.getEvent().getTable();
    }


}
