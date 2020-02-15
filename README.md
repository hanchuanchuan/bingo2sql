# bingo2sql
MySQL Binlog 解析

```sh
bingo2sql -h=127.0.0.1 -P 3306 -u test -p test \
  -start-file=mysql-bin.000003 -db db1_3306_test_inc -o 1.txt
```
