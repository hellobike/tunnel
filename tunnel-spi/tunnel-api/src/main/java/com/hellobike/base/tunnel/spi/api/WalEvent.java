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

package com.hellobike.base.tunnel.spi.api;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author machunxiao create at 2019-01-03
 */
@Data
public final class WalEvent implements Serializable {

    private static final long serialVersionUID = -3762460431364658184L;
    private String schema;
    private String table;
    private WalType walType;
    private List<CellData> dataList = new ArrayList<>();

    public enum WalType {
        // begin
        BEGIN,
        COMMIT,
        INSERT,
        UPDATE,
        DELETE,
        UNKNOWN;
    }

}
