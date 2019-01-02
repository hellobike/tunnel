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

package com.hellobike.base.tunnel.publisher;

import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.model.InvokeContext;
import com.hellobike.base.tunnel.spi.api.CollectionUtils;

import java.util.List;

/**
 * @author machunxiao 2018-10-25
 */
public interface IPublisher extends AutoCloseable {

    /**
     * 消费消息
     *
     * @param event    事件
     * @param callback 回调
     */
    void publish(Event event, Callback callback);


    /**
     * 消费消息
     *
     * @param context  上下文
     * @param callback 回调
     */
    default void publish(InvokeContext context, Callback callback) {
        context.getEvent().setLsn(context.getLsn());
        context.getEvent().setServerId(context.getServerId());
        context.getEvent().setSlotName(context.getSlotName());
        this.publish(context.getEvent(), callback);
    }

    /**
     * 关闭发布器
     */
    @Override
    default void close() {
    }

    /**
     * 批量发布
     *
     * @param contexts 上下文
     */
    default void publish(List<InvokeContext> contexts) {
        if (CollectionUtils.isEmpty(contexts)) {
            return;
        }
        contexts.forEach(ctx -> publish(ctx, null));
    }

    interface Callback {

        void onSuccess();

        void onFailure(Throwable t);

    }
}
