# bingo2sql
MySQL Binlog 解析工具

从MySQL binlog解析出原始SQL,对应的回滚SQL等.

### 限制和要求

- MySQL必须开启binlog
- binlog_format = row
- binlog_row_image = full

## 支持模式

### 1. 本地解析
```sh
bingo2sql --start-file=~/db_cmdb/blog/mysql-bin.000001 -t table.sql
```
其中`-t`参数指定的是建表语句文件,内容类似:
```sql
-- 需要解析哪个表,提供哪个表的建表语句
CREATE TABLE `tt` (
  id int auto_increment primary key,
  `TABLE_NAME` varchar(64) NOT NULL DEFAULT ''
) ;
```


### 2. 远程解析

远程解析的参数及使用均与binlog2sql类似

```
bingo2sql -h=127.0.0.1 -P 3306 -u test -p test -d db1_3306_test_inc \
  --start-time="2006-01-02 15:04:05" -t t1 -B
```

### 3. 解析服务

bingo2sql 支持以服务方式运行,提供解析的HTTP接口支持

```sh
bingo2sql --server --config=config.ini
```


## 支持选项

**解析模式**

- --stop-never 持续解析binlog。可选。默认False，同步至执行命令时最新的binlog位置。

- -K, --no-primary-key 对INSERT语句去除主键。可选。默认False

- -B, --flashback 生成回滚SQL，可解析大文件，不受内存限制。可选。默认False。与stop-never或no-primary-key不能同时添加。

- -M, --minimal-update 最小化update语句. 可选. (default false)

**解析范围控制**

- --start-file 起始解析文件，只需文件名，无需全路径 。可选，如果指定了起止时间，可以忽略该参数。

- --start-pos 起始解析位置。可选。默认为start-file的起始位置。

- --stop-file 终止解析文件。可选。若解析模式为stop-never，此选项失效。

- --stop-pos 终止解析位置。可选。默认为stop-file的最末位置；若解析模式为stop-never，此选项失效。

- --start-time 起始解析时间，格式'%Y-%m-%d %H:%M:%S'等。可选。默认不过滤。

- --stop-time 终止解析时间，格式'%Y-%m-%d %H:%M:%S'等。可选。默认不过滤。

- -C, --connection-id 线程号，可解析指定线程的SQL。

- -g, --gtid GTID范围.格式为uuid:编号[-编号],多个时以逗号分隔，例如：6573bb29-9d94-11e9-9e0c-0242ac130002:1-100

- --max 解析的最大行数,设置为0则不限制，以避免解析范围过大 (default 100000)

**对象过滤**

-d, --databases 只解析目标db的sql，多个库用逗号隔开，如-d db1,db2。可选。默认为空。

-t, --tables 只解析目标table的sql，多张表用逗号隔开，如-t tbl1,tbl2。可选。默认为空。

--ddl 解析ddl，仅支持正向解析。可选。默认False。

--sql-type 只解析指定类型，支持 insert,update,delete。多个类型用逗号隔开，如--sql-type=insert,delete。可选。默认为增删改都解析。


**附加信息**

- --show-gtid            显示gtid (default true)

- --show-time            显示执行时间,同一时间仅显示首次 (default true)

- --show-all-time        显示每条SQL的执行时间 (default false)

- --show-thread          显示线程号,便于区别同一进程操作 (default false)

- -o, --output          本地或远程解析时，可输出到指定文件(置空则输出到控制台，可通过 > file重定向)

**mysql连接配置** (仅远程解析需要)

```
 -h host
 -P port
 -u user
 -p password
```
