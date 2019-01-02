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

import com.hellobike.base.tunnel.spi.api.CollectionUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;

/**
 * @author machunxiao create at 2019-01-03
 */
final class EsClient {

    private final RequestOptions requestOptions;
    private final RestHighLevelClient restClient;

    public EsClient(EsClientConfig esClientConfig) {
        this.requestOptions = createRequestOptions();
        this.restClient = new RestHighLevelClient(RestClient.builder(HttpHost.create(esClientConfig.getServer())));
    }

    public boolean bulk(List<DocWriteRequest> doc) {
        return bulk(doc, 10);
    }

    public boolean bulk(List<DocWriteRequest> doc, int retry) {
        if (CollectionUtils.isEmpty(doc)) {
            return true;
        }
        while (retry > 0) {
            try {
                this.restClient.bulk(createBulkRequest(doc), requestOptions);
                return true;
            } catch (Exception e) {
                //
            } finally {
                retry--;
            }
        }
        return false;
    }

    private BulkRequest createBulkRequest(List<DocWriteRequest> doc) {
        BulkRequest br = new BulkRequest();
        br.add(doc);
        br.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        return br;
    }

    private static RequestOptions createRequestOptions() {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader("Connection", "Keep-Alive");
        return builder.build();
    }

    public void close() {
        try {
            this.restClient.close();
        } catch (Exception e) {
            //
        }
    }

}
