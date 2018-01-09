# postgres-logical-replication by java

### The application to be a producer,send logical log to kafka.. Spark or other applications can be the consumer
![image](http://chuantu.biz/t6/201/1515515409x-1404795475.png)
---

- 通过postgres-logical-replication对数据库变更事实处理,目前PG只支持处理DML。
- 通过ZK来做HA+LB，保证任务数量平均分配到各自执行节点，当某个节点宕机可以顺利Failover。
- 通过Kafka来做消息转发，本程序为消息生产者。
- 配置文件application.properties可以设置系统任务以及其他参数
- 通过http://localhost:8080/task, http://localhost:8080/doing ,http://localhost:8080/machine
查看当前任务以及每台机器执行情况。

### 目前该中间件支持HelloBike每天大概5亿次数据库变更。
---

## install wal2json decoding plugin
- git clone https://github.com/eulerto/wal2json.git
- cd wal2json
- USE_PGXS=1 sudo make
- USE_PGXS=1 sudo make install

**pg_hba.conf**

- local   replication     pg-jdbc-logical-decoding                                peer
- host    replication     pg-jdbc-logical-decoding        127.0.0.1/32            md5
- host    replication     pg-jdbc-logical-decoding        ::1/128                 md5

**postgres.conf**

- wal_level = logical
- max_wal_senders = 8
- wal_keep_segments = 4
- wal_sender_timeout = 60s
- max_replication_slots = 4

**PG操作**

- Slot 查询
select * from pg_replication_slots

- 删除Slot
select pg_drop_replication_slot($$hp_test_slot$$)

- 创建Slot
select pg_create_logical_replication_slot($$hp_test_slot$$, $$wal2json$$)