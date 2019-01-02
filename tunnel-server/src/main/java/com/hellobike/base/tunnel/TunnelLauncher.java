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

package com.hellobike.base.tunnel;

import com.alibaba.fastjson.JSON;
import com.hellobike.base.tunnel.apollo.ApolloConfig;
import com.hellobike.base.tunnel.config.*;
import com.hellobike.base.tunnel.filter.TableNameFilter;
import com.hellobike.base.tunnel.monitor.ExporterConfig;
import com.hellobike.base.tunnel.monitor.TunnelExporter;
import com.hellobike.base.tunnel.monitor.TunnelMonitorFactory;
import com.hellobike.base.tunnel.publisher.PublisherManager;
import com.hellobike.base.tunnel.publisher.es.EsPublisher;
import com.hellobike.base.tunnel.publisher.hbase.HBasePublisher;
import com.hellobike.base.tunnel.publisher.kafka.KafkaPublisher;
import com.hellobike.base.tunnel.spi.api.CollectionUtils;
import com.hellobike.base.tunnel.utils.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author machunxiao 2018-11-07
 */
public class TunnelLauncher {

    private static final Logger                         /**/ LOGGER         /**/ = LoggerFactory.getLogger(TunnelLauncher.class);
    private static final String                         /**/ TUNNEL_KEY     /**/ = "tunnel_subscribe_config";
    private static final String                         /**/ TUNNEL_ZK_KEY  /**/ = "tunnel_zookeeper_address";
    private static final String                         /**/ CONFIG_PATH    /**/ = "conf/tunnel.properties";
    private static final String                         /**/ APP_ID         /**/ = "AppTunnelService";

    private static final TunnelConfig                   /**/ TUNNEL_CONFIG  /**/ = new TunnelConfig();

    public static void main(String[] args) {

        Map<String, String> cmdArgs = toMap(args);
        initTunnelConfig(cmdArgs);
        initTunnelMonitor(cmdArgs);

        ConfigLoader config = ConfigLoaderFactory.getConfigLoader(TUNNEL_CONFIG);

        String configValue = config.getProperty(TUNNEL_KEY, "");
        if ("".equals(configValue)) {
            LOGGER.warn("config is null at first setup");
            System.exit(0);
        }

        LOGGER.info("config value:{}", configValue);
        String zkAddress = config.getProperty(TUNNEL_ZK_KEY, "");
        if ("".equals(zkAddress)) {
            LOGGER.warn("zk address is null");
            System.exit(0);
        }
        ZkConfig zkConfig = new ZkConfig();
        zkConfig.setAddress(zkAddress);
        LOGGER.info("zk address:{}", zkAddress);

        startSubscribe(zkConfig, configValue);
        config.addChangeListener(((key, oldValue, newValue) -> {
            if (!TUNNEL_KEY.equals(key)) {
                return;
            }
            startSubscribe(zkConfig, newValue);
        }));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            TunnelContext.close();
            LogManager.shutdown();
            LOGGER.info("TunnelServer Stopped");
        }));

        LOGGER.info("TunnelServer Started at:{}", TUNNEL_CONFIG.getProcessId());

    }

    public static TunnelConfig getTunnelConfig() {
        return TUNNEL_CONFIG;
    }

    /**
     * 初始化Tunnel 配置
     * <pre>
     *     -d domain
     *     -a app id
     *     -u use apollo
     *     -c config file
     *     -y use yukon
     * </pre>
     *
     * @param cfg 参数
     */
    private static void initTunnelConfig(Map<String, String> cfg) {
        TUNNEL_CONFIG.setProcessId(getPid());
        TUNNEL_CONFIG.setAppId(cfg.getOrDefault("-a", APP_ID));
        TUNNEL_CONFIG.setMetaDomain(cfg.getOrDefault("-d", getMetaDomain()));
        TUNNEL_CONFIG.setUseApollo("true".equalsIgnoreCase(cfg.getOrDefault("-u", "true")));
        TUNNEL_CONFIG.setUseYukon("true".equalsIgnoreCase(cfg.getOrDefault("-y", "false")));
        TUNNEL_CONFIG.setConfigFile(cfg.get("-c"));
    }

    /**
     * <pre>
     *     -p port
     *     -m metric name
     *     -n logger name
     *     -l labels
     * </pre>
     *
     * @param cfg 参数
     */
    private static void initTunnelMonitor(Map<String, String> cfg) {
        ExporterConfig config = new ExporterConfig();
        String port = cfg.get("-p");
        String metricName = cfg.get("-m");
        String loggerName = cfg.get("-n");
        String labelNames = cfg.get("-l");
        if (StringUtils.isNotBlank(port)) {
            try {
                config.setExportPort(Integer.parseInt(port));
            } catch (Exception e) {

            }
        }
        if (StringUtils.isNotBlank(metricName)) {
            config.setMetricName(metricName);
        }
        if (StringUtils.isNotBlank(loggerName)) {
            config.setLoggerName(loggerName);
        }
        if (StringUtils.isNotBlank(labelNames)) {
            config.setLabelNames(labelNames.split(","));
        }
        boolean useYukon = "true".equalsIgnoreCase(cfg.getOrDefault("-y", "true"));
        TunnelExporter exporter = TunnelMonitorFactory.initializeExporter(useYukon, config);

        exporter.startup();
        Runtime.getRuntime().addShutdownHook(new Thread(exporter::destroy));

    }

    private static Map<String, String> toMap(String[] args) {
        Map<String, String> cfg = new LinkedHashMap<>();
        if (args == null || args.length == 0) {
            return cfg;
        }
        for (int i = 0; i < args.length; i += 2) {
            try {
                cfg.put(args[i], args[i + 1]);
            } catch (Exception e) {
                //
            }
        }
        return cfg;
    }

    private static int getPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        return Integer.parseInt(name.split("@")[0]);
    }

    private static void startSubscribe(ZkConfig zkConfig, String value) {
        if (StringUtils.isBlank(value)) {
            return;
        }
        ApolloConfig apolloConfig = JSON.parseObject(value, ApolloConfig.class);

        List<ApolloConfig.Subscribe> subscribes = apolloConfig.getSubscribes();
        for (ApolloConfig.Subscribe subscribe : subscribes) {
            TunnelServer newServer = null;
            TunnelServer oldServer = null;
            try {
                SubscribeConfig subscribeConfig = toTunnelConfig(subscribe);
                subscribeConfig.setZkConfig(zkConfig);
                newServer = new TunnelServer(subscribeConfig);
                oldServer = TunnelContext.findServer(newServer.getServerId());
                if (oldServer != null) {
                    oldServer.shutdown();
                }
                TunnelContext.putServer(newServer);
                newServer.start();
            } catch (Exception e) {
                LOGGER.warn("setup :'" + subscribe.getSlotName() + "' failure", e);
                //
                if (oldServer != null) {
                    oldServer.shutdown();
                    TunnelContext.remove(oldServer.getServerId());
                }
                if (newServer != null) {
                    newServer.shutdown();
                    TunnelContext.remove(newServer.getServerId());
                }
            }
        }
    }

    private static SubscribeConfig toTunnelConfig(ApolloConfig.Subscribe subscribe) {

        String slotName = subscribe.getSlotName();
        ApolloConfig.EsConf esConf = subscribe.getEsConf();
        ApolloConfig.KafkaConf kafkaConf = subscribe.getKafkaConf();
        ApolloConfig.HBaseConf hbaseConf = subscribe.getHbaseConf();
        ApolloConfig.HiveConf hiveConf = subscribe.getHiveConf();
        ApolloConfig.HdfsConf hdfsConf = subscribe.getHdfsConf();

        ApolloConfig.PgConnConf pgConnConf = subscribe.getPgConnConf();
        List<ApolloConfig.Rule> rules = subscribe.getRules();

        parseEsConfig(slotName, esConf, rules);
        parseKafkaConfig(slotName, kafkaConf, rules);
        parseHBaseConfig(slotName, hbaseConf, rules);
        parseHiveConfig(slotName, hiveConf, rules);
        parseHdfsConfig(slotName, hdfsConf, rules);

        JdbcConfig jdbcConfig = getJdbcConfig(slotName, pgConnConf);
        SubscribeConfig subscribeConfig = new SubscribeConfig();
        subscribeConfig.setJdbcConfig(jdbcConfig);
        subscribeConfig.setServerId(generateServerId(pgConnConf.getHost(), pgConnConf.getPort(), jdbcConfig.getSlotName()));
        return subscribeConfig;
    }

    /**
     * generate CONFIG_NAME new serverId
     *
     * @param host host
     * @param port port
     * @param slot slot
     * @return serverId
     */
    private static String generateServerId(String host, int port, String slot) {
        return slot + "@" + host + ":" + port;
    }

    private static void parseKafkaConfig(String slotName, ApolloConfig.KafkaConf kafkaConf, List<ApolloConfig.Rule> rules) {
        if (kafkaConf == null || CollectionUtils.isEmpty(kafkaConf.getAddrs())) {
            return;
        }

        List<KafkaConfig> kafkaConfigs = rules.stream()
                .map(TunnelLauncher::toKafkaConfig)
                .filter(Objects::nonNull)
                .peek(cfg -> cfg.setServer(StringUtils.join(kafkaConf.getAddrs(), ",")))
                .collect(Collectors.toList());

        PublisherManager.getInstance().putPublisher(slotName, new KafkaPublisher(kafkaConfigs));
    }

    private static void parseEsConfig(String slotName, ApolloConfig.EsConf esConf, List<ApolloConfig.Rule> rules) {
        if (esConf == null || esConf.getAddrs() == null || esConf.getAddrs().isEmpty()) {
            return;
        }
        List<EsConfig> esConfigs = rules.stream()
                .map(TunnelLauncher::toEsConfig)
                .filter(Objects::nonNull)
                .peek(esConfig -> esConfig.setServer(esConf.getAddrs()))
                .collect(Collectors.toList());

        PublisherManager.getInstance().putPublisher(slotName, new EsPublisher(esConfigs));

    }

    private static void parseHBaseConfig(String slotName, ApolloConfig.HBaseConf hbaseConf, List<ApolloConfig.Rule> rules) {

        if (hbaseConf == null || StringUtils.isBlank(hbaseConf.getZkquorum())) {
            return;
        }

        List<HBaseConfig> configs = rules.stream()
                .map(TunnelLauncher::toHBaseConfig)
                .filter(Objects::nonNull)
                .peek(config -> config.setQuorum(hbaseConf.getZkquorum()))
                .collect(Collectors.toList());
        PublisherManager.getInstance().putPublisher(slotName, new HBasePublisher(configs));
    }

    private static void parseHiveConfig(String slotName, ApolloConfig.HiveConf hiveConf, List<ApolloConfig.Rule> rules) {
    }

    private static void parseHdfsConfig(String slotName, ApolloConfig.HdfsConf hdfsConf, List<ApolloConfig.Rule> rules) {
    }

    private static KafkaConfig toKafkaConfig(ApolloConfig.Rule rule) {
        if (StringUtils.isBlank(rule.getTopic())) {
            return null;
        }
        KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.setTopic(rule.getTopic());
        kafkaConfig.setPartition(rule.getPartition());
        kafkaConfig.setFilters(Collections.singletonList(new TableNameFilter(rule.getTable())));
        kafkaConfig.setPkNames(new ArrayList<>(rule.getPks()));

        return kafkaConfig;
    }

    private static EsConfig toEsConfig(ApolloConfig.Rule rule) {
        if (StringUtils.isBlank(rule.getTable())
                || StringUtils.isBlank(rule.getIndex())
                || StringUtils.isBlank(rule.getType())
                || rule.getPks() == null
                || rule.getEsid() == null
        ) {
            return null;
        }
        EsConfig esConfig = new EsConfig();
        esConfig.setTable(rule.getTable());
        esConfig.setIndex(rule.getIndex());
        esConfig.setType(rule.getType());
        esConfig.setPkFieldNames(new ArrayList<>(rule.getPks()));
        esConfig.setEsIdFieldNames(new ArrayList<>(rule.getEsid()));
        esConfig.setFieldMappings(rule.getFields() == null ? new HashMap<>() : new HashMap<>(rule.getFields()));
        esConfig.setFilters(Collections.singletonList(new TableNameFilter(esConfig.getTable())));

        esConfig.setSql(rule.getSql());
        esConfig.setParameters(rule.getParameters());

        return esConfig;
    }

    private static HBaseConfig toHBaseConfig(ApolloConfig.Rule rule) {
        HBaseConfig config = new HBaseConfig();
        config.setPks(rule.getPks());
        config.setHbaseKey(rule.getHbaseKey());
        config.setHbaseTable(rule.getHbaseTable());
        config.setTable(rule.getTable());
        config.setFamily(StringUtils.isBlank(rule.getFamily()) ? "data" : rule.getFamily());
        config.setQualifier(StringUtils.isBlank(rule.getQualifier()) ? "bytes" : rule.getQualifier());
        config.setFilters(Collections.singletonList(new TableNameFilter(rule.getTable())));
        return config;
    }

    private static JdbcConfig getJdbcConfig(String slotName, ApolloConfig.PgConnConf pgConnConf) {
        String jdbcUrl = "jdbc:postgresql://" + pgConnConf.getHost() + ":" + pgConnConf.getPort() + "/" + pgConnConf.getDatabase();
        JdbcConfig jdbcConfig = new JdbcConfig();
        jdbcConfig.setSlotName(slotName);
        jdbcConfig.setUrl(jdbcUrl);
        jdbcConfig.setUsername(pgConnConf.getUser());
        jdbcConfig.setPassword(pgConnConf.getPassword());
        jdbcConfig.setHost(pgConnConf.getHost());
        jdbcConfig.setPort(pgConnConf.getPort());
        jdbcConfig.setSchema(pgConnConf.getDatabase());
        return jdbcConfig;
    }

    private static String getMetaDomain() {
        InputStream is = FileUtils.load(CONFIG_PATH);
        if (is == null) {
            return "";
        }
        Properties prop = new Properties();
        try {
            prop.load(is);
        } catch (IOException e) {
            //
        }
        return prop.getProperty("tunnel.apollo.meta.domain", "");
    }


}
