# tunnel配置

支持两种格式配置文件：
* properties

默认使用properties格式配置文件。
```properties
tunnel_subscribe_config={"pg_dump_path":"","subscribes":[{"slotName":"slot_for_test","pgConnConf":{"host":"localhost","port":5432,"database":"test1","user":"test1","password":"test1"},"rules":[{"table":"t_department_info","fields":null,"pks":["id"],"esid":["id"],"index":"t_department_info","type":"logs"}],"esConf":{"addrs":"http://localhost:9200"}}]}
tunnel_zookeeper_address=localhost:2181
```
* yaml / yml

配置文件扩展名应为 ```yml``` 或者 ```yaml```。
```yaml
tunnel_subscribe_config:
  pg_dump_path: ''
  subscribes:
  - slotName: slot_for_test
    pgConnConf:
      host: localhost
      port: 5432
      database: test1
      user: test1
      password: test1
    rules:
    - {table: test_1, pks: ['id'], topic: test_1_logs}
    - {table: test_2, pks: ['id'], topic: test_2_logs}
    kafkaConf:
      addrs:
      - localhost:9092
tunnel_zookeeper_address: localhost:2181
```

