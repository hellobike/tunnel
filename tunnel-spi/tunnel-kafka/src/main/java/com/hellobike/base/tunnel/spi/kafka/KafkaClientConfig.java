package com.hellobike.base.tunnel.spi.kafka;

import lombok.Data;

/**
 * @author machunxiao create at 2019-01-04
 */
@Data
public class KafkaClientConfig {

    private String server;
    private String ackConfig;
    private String keySerializer;
    private String valSerializer;

}
