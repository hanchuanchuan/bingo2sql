# 效率测试

测试结果

mysqlbinlog > bingo2sql > binlog_rollback > binlog2sql

**说明：**

该测试结果可能存在的偏差如下：

- 输出格式略有差异
- 测试所在环境的状态变化
- binlog_rollback 输出了binlog分析等信息等


### 测试数据准备

- 准备100w行基础数据
- 更新
- 删除
- 获取binlog位置


#### 创建测试表

```sql

use test;

reset master;

drop table if exists tt;

CREATE TABLE `tt` (
  ID bigint unsigned auto_increment primary key,
  `TABLE_CATALOG` varchar(512) NOT NULL DEFAULT '',
  `TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT '',
  `TABLE_NAME` varchar(64) NOT NULL DEFAULT '',
  `COLUMN_NAME` varchar(64) NOT NULL DEFAULT '',
  `ORDINAL_POSITION` bigint(21) unsigned NOT NULL DEFAULT '0',
  `COLUMN_DEFAULT` longtext,
  `IS_NULLABLE` varchar(3) NOT NULL DEFAULT '',
  `DATA_TYPE` varchar(64) NOT NULL DEFAULT '',
  `CHARACTER_MAXIMUM_LENGTH` bigint(21) unsigned DEFAULT NULL,
  `CHARACTER_OCTET_LENGTH` bigint(21) unsigned DEFAULT NULL,
  `NUMERIC_PRECISION` bigint(21) unsigned DEFAULT NULL,
  `NUMERIC_SCALE` bigint(21) unsigned DEFAULT NULL,
  `DATETIME_PRECISION` bigint(21) unsigned DEFAULT NULL,
  `CHARACTER_SET_NAME` varchar(32) DEFAULT NULL,
  `COLLATION_NAME` varchar(32) DEFAULT NULL,
  `COLUMN_TYPE` longtext NOT NULL,
  `COLUMN_KEY` varchar(3) NOT NULL DEFAULT '',
  `EXTRA` varchar(30) NOT NULL DEFAULT '',
  `PRIVILEGES` varchar(80) NOT NULL DEFAULT '',
  `COLUMN_COMMENT` varchar(1024) NOT NULL DEFAULT '',
  `GENERATION_EXPRESSION` longtext NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

```


#### 搭建测试数据



```sql

insert into tt(TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY,EXTRA,PRIVILEGES,COLUMN_COMMENT,GENERATION_EXPRESSION)
select * from information_schema.columns limit 1000;

# 准备100w行基础数据
insert into tt(TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION,CHARACTER_SET_NAME,COLLATION_NAME,COLUMN_TYPE,COLUMN_KEY,EXTRA,PRIVILEGES,COLUMN_COMMENT,GENERATION_EXPRESSION)
select tt.TABLE_CATALOG,tt.TABLE_SCHEMA,tt.TABLE_NAME,tt.COLUMN_NAME,tt.ORDINAL_POSITION,tt.COLUMN_DEFAULT,tt.IS_NULLABLE,tt.DATA_TYPE,tt.CHARACTER_MAXIMUM_LENGTH,tt.CHARACTER_OCTET_LENGTH,tt.NUMERIC_PRECISION,tt.NUMERIC_SCALE,tt.DATETIME_PRECISION,tt.CHARACTER_SET_NAME,tt.COLLATION_NAME,tt.COLUMN_TYPE,tt.COLUMN_KEY,tt.EXTRA,tt.PRIVILEGES,tt.COLUMN_COMMENT,tt.GENERATION_EXPRESSION from tt,tt t1;


update tt set ORDINAL_POSITION=ORDINAL_POSITION+10 where id%5=0;

update tt set TABLE_NAME=concat(TABLE_NAME,"1"),COLLATION_NAME=concat(COLLATION_NAME,"2"),
COLUMN_COMMENT = TABLE_NAME where id%4=0;

delete from tt where id%2=0;

# 获取binlog位置以便设置解析参数
show BINARY LOGS;
```

Log_name         | File_size
-----------| -----------
 mysql-bin.000001 | 357491554


#### bin2sql 测试脚本

```sh

for i in {1..10}; do
echo $i;
/usr/bin/time -f "real %E  |  user %U  |  sys %S  |  cpu %P" -a -o out.txt bin/bingo2sql -h 127.0.0.1 -P 3306 -u test -p test --start-file=mysql-bin.000001 --start-pos=4 --stop-file=mysql-bin.000001 --stop-pos=357490000 -o /tmp/binlog2/out.sql --max=0 --show-gtid=false --show-time=false;
done
cat out.txt;

```

执行结果

real  |  user  |  sys | cpu
---- | ---- | ---- | ----
real 0:13.49  |  user 33.65  |  sys 2.68  |  cpu 269%
real 0:13.61  |  user 34.21  |  sys 2.75  |  cpu 271%
real 0:13.42  |  user 33.26  |  sys 2.61  |  cpu 267%
real 0:13.61  |  user 33.99  |  sys 2.64  |  cpu 269%
real 0:13.85  |  user 34.77  |  sys 2.65  |  cpu 270%
real 0:13.58  |  user 33.58  |  sys 2.62  |  cpu 266%
real 0:13.61  |  user 34.06  |  sys 2.59  |  cpu 269%
real 0:13.47  |  user 33.39  |  sys 2.65  |  cpu 267%
real 0:11.68  |  user 28.75  |  sys 2.29  |  cpu 265%
real 0:11.45  |  user 28.68  |  sys 2.11  |  cpu 268%


#### binlog_rollback 测试脚本
```sh

rm -f out.txt;
for i in {1..10}; do
echo $i;
/usr/bin/time -f "real %E  |  user %U  |  sys %S  |  cpu %P" -a -o out.txt ./binlog_rollback -m repl -w 2sql -M mysql -t 4 -H 127.0.0.1 -P 3306 -u test -p test -sbin mysql-bin.000001 -spos 4 -ebin mysql-bin.000001 -epos 357490000 -o /tmp/binlog -dj=""
done
cat out.txt;

```

执行结果

real  |  user  |  sys | cpu
---- | ---- | ---- | ----
real 0:28.79  |  user 68.74  |  sys 8.46  |  cpu 268%
real 0:27.35  |  user 66.22  |  sys 7.86  |  cpu 270%
real 0:28.58  |  user 69.29  |  sys 8.31  |  cpu 271%
real 0:29.19  |  user 70.78  |  sys 8.47  |  cpu 271%
real 0:28.31  |  user 68.91  |  sys 8.06  |  cpu 271%
real 0:28.67  |  user 68.67  |  sys 8.33  |  cpu 268%
real 0:28.40  |  user 68.92  |  sys 7.73  |  cpu 269%
real 0:28.44  |  user 69.04  |  sys 8.05  |  cpu 271%
real 0:28.43  |  user 68.97  |  sys 7.94  |  cpu 270%
real 0:28.87  |  user 69.27  |  sys 7.99  |  cpu 267%



#### bin2sql测试脚本
```sh

rm -f out.txt;
for i in {1..10}; do
echo $i;

/usr/bin/time -f "real %E  |  user %U  |  sys %S  |  cpu %P" -a -o out.txt python binlog2sql.py -h127.0.0.1 -P3306 -utest -p'test' --start-file='mysql-bin.000001' --start-position=4 --stop-file='mysql-bin.000001' --stop-position=357490000 --only-dml > /tmp/binlog3/out.sql

done
cat out.txt;

```

执行结果

**无**
感兴趣的同学可以自行测试。


#### mysqlbinlog 测试脚本
```sh

rm -f out.txt;
for i in {1..10}; do
echo $i;
/usr/bin/time -f "real %E  |  user %U  |  sys %S  |  cpu %P" -a -o out.txt mysqlbinlog -h127.0.0.1 -P3306 -utest -p'test' --start-position=4 --stop-position=357490000 --base64-output=DECODE-ROWS -v  -r /tmp/binlog4/out.sql /data/mysql/db_cmdb/blog/mysql-bin.000001
done
cat out.txt;

```

执行结果

real  |  user  |  sys | cpu
---- | ---- | ---- | ----
real 0:13.42  |  user 11.43  |  sys 1.96  |  cpu 99%
real 0:13.29  |  user 11.37  |  sys 1.88  |  cpu 99%
real 0:13.59  |  user 11.54  |  sys 1.94  |  cpu 99%
real 0:13.17  |  user 11.20  |  sys 1.94  |  cpu 99%
real 0:13.27  |  user 11.37  |  sys 1.83  |  cpu 99%
real 0:13.76  |  user 11.62  |  sys 2.04  |  cpu 99%
real 0:13.98  |  user 12.01  |  sys 1.93  |  cpu 99%
real 0:14.05  |  user 11.86  |  sys 1.95  |  cpu 98%
real 0:13.76  |  user 11.67  |  sys 1.96  |  cpu 99%
real 0:13.79  |  user 11.72  |  sys 2.01  |  cpu 99%


