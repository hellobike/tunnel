# prometheus 监控配置

## Tunnel prometheus抓取地址配置

在Tunnel的配置文件中

``` java
// PrometheusAddress prometheus
// 启动参数中添加配置
java -server -classpath conf/*:lib/* com.hellobike.base.tunnel.TunnelLauncher -u false -c cfg.properties -p 7788
```

可以通过 localhost:7788/metrics 可以获取到相关监控项