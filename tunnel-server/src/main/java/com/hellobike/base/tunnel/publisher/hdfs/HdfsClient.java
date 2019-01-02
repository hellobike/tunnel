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

package com.hellobike.base.tunnel.publisher.hdfs;

import com.alibaba.fastjson.JSON;
import com.hellobike.base.tunnel.model.Event;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author machunxiao create at 2018-11-30
 */
public class HdfsClient {

    private Configuration configuration;

    private String fileName;

    public void append(HdfsConfig config, Event event) {
        try {
            Configuration hadoopConfig = new Configuration();
            FileSystem fileSystem = FileSystem.get(hadoopConfig);
            Path hdfsPath = new Path(fileName);
            FSDataOutputStream fileOutputStream = null;
            try {
                if (fileSystem.exists(hdfsPath)) {
                    fileOutputStream = fileSystem.append(hdfsPath);
                } else {
                    fileOutputStream = fileSystem.create(hdfsPath);
                }
                fileOutputStream.writeUTF(JSON.toJSONString(event));

            } finally {
                if (fileSystem != null) {
                    fileSystem.close();
                }
                if (fileOutputStream != null) {
                    fileOutputStream.close();
                }
            }
        } catch (IOException e) {

        }

    }

    public void delete(HdfsConfig config, Event event) {
    }

    public void update(HdfsConfig config, Event event) {
    }

}
