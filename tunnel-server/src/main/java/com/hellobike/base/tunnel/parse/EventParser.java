package com.hellobike.base.tunnel.parse;

import com.hellobike.base.tunnel.model.ColumnData;
import com.hellobike.base.tunnel.model.Event;
import com.hellobike.base.tunnel.model.EventType;
import com.hellobike.base.tunnel.model.InvokeContext;
import com.hellobike.base.tunnel.store.MemStore;

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

/**
 * @author machunxiao 2018-10-25
 */
public class EventParser implements IEventParser {

    private MemStore memStore;

    public EventParser() {
        this.memStore = new MemStore();
    }

    private static boolean isBegin(String msg) {
        return msg != null
                && msg.length() > 5
                && (msg.charAt(0) == 'B' || msg.charAt(0) == 'b')
                && (msg.charAt(1) == 'E' || msg.charAt(1) == 'e')
                && (msg.charAt(2) == 'G' || msg.charAt(2) == 'g')
                && (msg.charAt(3) == 'I' || msg.charAt(3) == 'i')
                && (msg.charAt(4) == 'N' || msg.charAt(4) == 'n');
    }

    private static boolean isCommit(String msg) {
        return msg != null
                && msg.length() > 6
                && (msg.charAt(0) == 'C' || msg.charAt(0) == 'c')
                && (msg.charAt(1) == 'O' || msg.charAt(1) == 'o')
                && (msg.charAt(2) == 'M' || msg.charAt(2) == 'm')
                && (msg.charAt(3) == 'M' || msg.charAt(3) == 'm')
                && (msg.charAt(4) == 'I' || msg.charAt(4) == 'i')
                && (msg.charAt(5) == 'T' || msg.charAt(5) == 't');
    }

    @Override
    public void parse(InvokeContext context) {
        if (isBegin(context.getMessage()) || isCommit(context.getMessage())) {
            return;
        }
        Event event = parseEvent(context.getMessage());
        if (event == null) {
            return;
        }
        context.setEvent(event);

        memStore.store(context);
    }

    private Event parseEvent(String message) {
        Event event = new Event();
        Lexer lexer = new Lexer(message);

        // "table"
        lexer.nextToken(' ');
        // schema_name
        lexer.nextToken('.');
        String schema = lexer.token();
        // table_name
        String table = lexer.nextToken(':');
        lexer.skip(1);
        // event_type
        String eventType = lexer.nextToken(':');

        event.setSchema(schema);
        event.setTable(table);
        event.setEventType(EventType.getEventType(eventType));
        lexer.skip(1);

        while (lexer.hasNext()) {
            ColumnData data = new ColumnData();
            String name = parseName(lexer);
            if ("(no-tuple-data)".equals(name)) {
                // 删除时,无主键,不能同步
                return null;
            }
            String type = parseType(lexer);
            lexer.skip(1);
            String value = parseValue(lexer);


            // 去除多余的符号 "'"
            if (value.length() > 0 && value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
                value = value.substring(1, value.length() - 1);
            }
            data.setName(name);
            data.setDataType(type);
            data.setValue(value);
            event.getDataList().add(data);
        }
        return event;
    }

    private String parseName(Lexer lexer) {
        if (lexer.current() == ' ') {
            lexer.skip(1);
        }
        lexer.nextToken('[');
        return lexer.token();
    }

    private String parseType(Lexer lexer) {
        lexer.nextToken(']');
        return lexer.token();
    }

    private String parseValue(Lexer lexer) {
        if (lexer.current() == '\'') {
            lexer.skip(1);
            lexer.nextToken('\'');
            return lexer.token();
        }
        lexer.nextToken(' ');
        return lexer.token();
    }


    public void setMemStore(MemStore memStore) {
        this.memStore = memStore;
    }

    private static class Lexer {
        private final String input;
        private final char[] array;
        private final int length;
        private int pos = 0;
        private String token;

        public Lexer(String input) {
            this.input = input;
            this.array = input.toCharArray();
            this.length = this.array.length;
        }

        public String token() {
            return token;
        }

        public String nextToken(char comma) {
            if (pos < length) {
                StringBuilder out = new StringBuilder(16);
                while (pos < length && array[pos] != comma) {
                    out.append(array[pos]);
                    pos++;
                }
                pos++;
                return token = out.toString();
            }
            return token = null;
        }

        public void skip(int skip) {
            this.pos += skip;
        }

        public char current() {
            return array[pos];
        }

        public boolean hasNext() {
            return pos < length;
        }
    }

}
