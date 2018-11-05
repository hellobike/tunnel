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
package com.hellobike.base.tunnel.utils;

import com.hellobike.base.tunnel.TunnelLauncher;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Paths;

/**
 * @author machunxiao create at 2018-12-26
 */
public class FileUtils {

    private FileUtils() {
    }

    public static InputStream load(String file) {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
        if (is == null) {
            is = TunnelLauncher.class.getResourceAsStream(file);
        }
        if (is == null) {
            is = loadFromFileSystem(file);
        }
        return is;
    }

    private static InputStream loadFromFileSystem(String configPath) {
        if (Paths.get(configPath).toFile().exists()) {
            try {
                return new FileInputStream(Paths.get(configPath).toFile());
            } catch (Exception ignore) {
                //
            }
        }
        return null;
    }
}
