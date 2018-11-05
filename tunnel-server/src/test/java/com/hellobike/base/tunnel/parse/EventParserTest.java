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
package com.hellobike.base.tunnel.parse;

import com.hellobike.base.tunnel.model.InvokeContext;
import com.hellobike.base.tunnel.store.MemStore;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @author machunxiao 2018-11-07
 */
public class EventParserTest {

    @Test
    public void test_parse() {

        EventParser parser = new EventParser();
        String serverId = "server-id1";
        InvokeContext ctx = new InvokeContext();
        ctx.setSlotName("test_slot");
        ctx.setServerId(serverId);
        ctx.setLsn(1000L);

        String msg = "begin 12345";

        ctx.setMessage(msg);
        parser.parse(ctx);

        msg = "commit 12345";
        ctx.setMessage(msg);
        parser.parse(ctx);

        msg = "table public.test_logic_table: INSERT: pk[integer]:1 name[character varying]:'previous value'";
        ctx.setMessage(msg);
        MemStore mock = Mockito.mock(MemStore.class);
        Mockito.doNothing().when(mock).store(Mockito.any());
        parser.setMemStore(mock);

        parser.parse(ctx);

        msg = "table public.test_logic_table: DELETE: (no-tuple-data)";
        ctx.setMessage(msg);
        parser.parse(ctx);

    }

}
