# uldra-replicator

1. MySQL replicator that supports various target form.
2. Parallel processing on a specific column basis
3. Data change processing according to binlog event.

## build & run

    mvn install
    java -jar target/uldra-replicator-0.0.1.jar --config-file="uldra-config.yml"

## uldra-config.yml
```yaml
workerCount: 16
wokerQueueSize: 500

binlogServer: 127.0.0.1:3306
binlogServerID: 5
binlogServerUsername: repl
binlogServerPassword: repl
binlogInfoFile: "binlog.info"

binlogPolicies:
- name: origin.s_user
  groupKey: userid
  replicatPolicies:
  - name: "t_user"
- name: origin.s_user_status
  groupKey: userid

targetDataSource: !!org.apache.commons.dbcp2.BasicDataSource
  driverClassName: com.mysql.jdbc.Driver
  url: jdbc:mysql://127.0.0.1:3306/target?autoReconnect=true&useSSL=false&sessionVariables=SQL_MODE='NO_AUTO_VALUE_ON_ZERO'
  username: target
  password: target
  maxTotal: 30
  maxWaitMillis: 100
  validationQuery: SELECT 1
  testOnBorrow: false
  testOnReturn: false
  testWhileIdle: true
  timeBetweenEvictionRunsMillis: 60000
  minEvictableIdleTimeMillis : 1200000
  numTestsPerEvictionRun : 10
```



### 1. binlogPolicies

- name : source table name ex) {{database}}.{{table_name}}
- groupKey: data grouping column for parallel processing, included in pk is recommended.
- rowHandlerClassName: custom handler to modify row from binlog, must implement net.gywn.binlog.handler.RowHandler
- rowHandlerParams: parameters for row handler
- replicatPolicies: replicate policies for target table
    - name : target table name
    - softDelete: update null except pk if delete event income
    - upsertMode: convert insert to upsert query 
    - colums:  target columns to replicate, default is origin columns
    
    
### 2. targetDataSource
- If there is more than one in targetDataSource, the data source is managed as a shardingsphere.
    -  default sharding algorithm: net.gywn.binlog.shardingsphere.PreciseShardingCRC32

### 2. targetHandlerClassName
Currently, only a single mysql target is implemented.


Still in development. Monitoring and logging implementation is required.

gywndi@gmail.com
