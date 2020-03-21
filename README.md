### JdbcStorageHandler
方便数据组从关系型数据库到hive之间数据迁移,实现了hive通过jdbc读写mysql、pg关系型数据库

- 可读mysql表,pg表,分区表(多个相同的表)

- 可写,支持作业重跑,insert overwrite hive_my_tb select可以通过set配置删除待覆盖数据.

#### prepare 
```hiveql
use test;
add jar viewfs://hadoopbeta/user/share/hive/jdbc/jdbc-handler.jar;
```

#### case 1, read from mysql

```hiveql
drop table tb_my_read_1;
create external table tb_my_read_1(day date, dt timestamp, busi string, pv int, uv int, info string)
STORED BY 'com.wdw.hive.jdbchandler.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "com.mysql.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mysql://hostname:port/database",
    "hive.sql.jdbc.read-write" = "read",
    "hive.sql.jdbc.username" = "username",
    "hive.sql.jdbc.password" = "password",
    "hive.sql.jdbc.table" = "order_1"
);
select * from tb_my_read_1 limit 1;
+-------------------+------------------------+--------------------+------------------+------------------+--------------------+
| 2020-01-01        | 2020-01-01 00:00:00.0  | feiji             | 100              | 10               | {city:bj}          |
+-------------------+------------------------+--------------------+------------------+------------------+--------------------+
```

#### case 2, write to mysql table, when rerun, truncate mysql table

```hiveql
drop table tb_my_write_1;
create external table tb_my_write_1(day date, dt timestamp, busi string, pv int, uv int, info string)
STORED BY 'com.wdw.hive.jdbchandler.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "com.mysql.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mysql://hostname:port/database",
    "hive.sql.jdbc.read-write" = "read,write",
    "hive.sql.jdbc.username" = "username",
    "hive.sql.jdbc.password" = "password",
    "hive.sql.jdbc.table" = "order_1",
    "hive.sql.jdbc.delete.type" = "table"
);
insert into tb_my_write_1 values('2020-01-03', '2020-01-01 00:00:00', 'feiji', 100, 10, '{city:bj}' );
select * from tb_my_write_1;
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+
| 2020-01-01         | 2020-01-01 00:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 01:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 02:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 03:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 12:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 12:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 00:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 00:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 13:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 13:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 00:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-03         | 2020-01-01 00:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+

insert overwrite table tb_my_write_1 values('2020-01-03', '2020-01-01 00:00:00', 'feiji', 100, 10, '{city:bj}' );
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+
| 2020-01-03         | 2020-01-01 00:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+
```

#### case 3, write to mysql table, when rerun, delete from table where col1=v1 and col2>=v2 and col2<v3

```hiveql
drop table tb_my_write_2;
create external table tb_my_write_2(day date, dt timestamp, busi string, pv int, uv int, info string)
STORED BY 'com.wdw.hive.jdbchandler.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "com.mysql.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mysql://hostname:port/database",
    "hive.sql.jdbc.read-write" = "read,write",
    "hive.sql.jdbc.username" = "username",
    "hive.sql.jdbc.password" = "password",
    "hive.sql.jdbc.table" = "order_1",
    "hive.sql.jdbc.delete.type" = "column",
    "hive.sql.jdbc.delete.by.columns" = "day,dt,busi"
);

insert into tb_my_write_2 values('2020-01-01', '2020-01-01 00:00:00', 'feiji', 200, 20, '{city:bj}' );
select * from tb_my_write_2;
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+
| 2020-01-01         | 2020-01-01 00:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 01:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 02:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 03:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 12:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 12:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 00:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 00:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 13:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 13:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 00:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 00:00:00.0  | feiji              | 200               | 20                | {city:bj}           |
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+

set hivevar:hive.jdbc.delete.order_1.day.interval="2020-01-01";
set hivevar:hive.jdbc.delete.order_1.dt.interval="2020-01-01 00:00:00,2020-01-01 12:00:00";
set hivevar:hive.jdbc.delete.order_1.busi.interval="feiji";
insert overwrite table tb_my_write_2 values('2020-01-01', '2020-01-01 00:00:00', 'feiji', 300, 30, '{city:bj}' );
select * from tb_my_write_2;
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+
| 2020-01-01         | 2020-01-01 12:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 12:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 00:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 00:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 13:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 13:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 00:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 00:00:00.0  | feiji              | 300               | 30                | {city:bj}           |
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+

set hivevar:hive.jdbc.delete.order_1.day.interval="2020-01-01,2020-01-02";
set hivevar:hive.jdbc.delete.order_1.dt.interval="2020-01-01 00:00:00,2020-01-01 12:00:00";
set hivevar:hive.jdbc.delete.order_1.busi.interval="feiji,zzz";
insert overwrite table tb_my_write_2 values('2020-01-01', '2020-01-01 00:00:00', 'feiji', 400, 40, '{city:bj}' ),('2020-01-01', '2020-01-01 00:00:00', 'hotel', 400, 40, '{city:bj}');
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+
| 2020-01-01         | 2020-01-01 12:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 12:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 00:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 00:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-02         | 2020-01-02 13:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 13:00:00.0  | hotel               | 100               | 10                | {city:bj}           |
| 2020-01-01         | 2020-01-01 00:00:00.0  | feiji              | 400               | 40                | {city:bj}           |
| 2020-01-01         | 2020-01-01 00:00:00.0  | hotel               | 400               | 40                | {city:bj}           |
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+

```

#### postgresql prepare 

```postgresql
insert into order_1 values ('2020-01-01', '2020-01-01 00:00:00', 'feiji', 100, 10, 'city=bj');
insert into order_1 values ('2020-01-01', '2020-01-01 12:00:00', 'feiji', 100, 10, 'city=bj');
insert into order_1 values ('2020-01-02', '2020-01-01 00:00:00', 'feiji', 100, 10, 'city=bj');
insert into order_1 values ('2020-01-01', '2020-01-01 00:00:00', 'hotel', 100, 10, 'city=bj');
```

#### case 4, read from postgres

```hiveql
create external table tb_pg_read_4(day date, dt timestamp, busi string, pv int, uv int, info string)
STORED BY 'com.wdw.hive.jdbchandler.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://hostname:port/database",
    "hive.sql.jdbc.read-write" = "read",
    "hive.sql.jdbc.username" = "username",
    "hive.sql.jdbc.password" = "password",
    "hive.sql.jdbc.table" = "order_1"
);
select * from tb_pg_read_4 limit 1;
+-------------------+------------------------+--------------------+------------------+------------------+--------------------+
| 2020-01-01        | 2020-01-01 00:00:00.0  | feiji             | 100              | 10               | {city:bj}          |
+-------------------+------------------------+--------------------+------------------+------------------+--------------------+
```

#### case 5, write to postgres table, when rerun, truncate postgres table

```hiveql
drop table tb_pg_write_1;

create external table tb_pg_write_1(day date, dt timestamp, busi string, pv int, uv int, info string)
STORED BY 'com.wdw.hive.jdbchandler.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://hostname:port/database",
    "hive.sql.jdbc.read-write" = "read,write",
    "hive.sql.jdbc.username" = "username",
    "hive.sql.jdbc.password" = "password",
    "hive.sql.jdbc.table" = "order_1",
    "hive.sql.jdbc.delete.type" = "table"
);

insert into tb_pg_write_1 values('2020-01-03', '2020-01-01 00:00:00', 'feiji', 100, 10, '{city:bj}' );

select * from tb_pg_write_1;

insert overwrite table tb_pg_write_1 values('2020-01-03', '2020-01-01 00:00:00', 'feiji', 100, 10, '{city:bj}' );
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+
| 2020-01-03         | 2020-01-01 00:00:00.0  | feiji              | 100               | 10                | {city:bj}           |
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+
```

#### case 6, write to postgres table, when rerun, delete from table where col1=v1 and col2>=v21 and col2<v22
```hiveql
drop table tb_pg_write_2;
create external table tb_pg_write_2(day date, dt timestamp, busi string, pv int, uv int, info string)
STORED BY 'com.wdw.hive.jdbchandler.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://hostname:port/database",
    "hive.sql.jdbc.read-write" = "read,write",
    "hive.sql.jdbc.username" = "username",
    "hive.sql.jdbc.password" = "password",
    "hive.sql.jdbc.table" = "order_1",
    "hive.sql.jdbc.delete.type" = "column",
    "hive.sql.jdbc.delete.by.columns" = "day,dt,busi"
);

set hivevar:hive.jdbc.delete.order_1.day.interval="2020-01-01";
set hivevar:hive.jdbc.delete.order_1.dt.interval="2020-01-01 00:00:00,2020-01-01 12:00:00";
set hivevar:hive.jdbc.delete.order_1.busi.interval="feiji";
insert overwrite table tb_pg_write_2 values('2020-01-01', '2020-01-01 00:00:00', 'feiji', 300, 30, '{city:bj}' );
select * from tb_pg_write_2;
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+
| 2020-01-01         | 2020-01-01 12:00:00.0  | feiji              | 100               | 10                | city=bj             |
| 2020-01-02         | 2020-01-01 00:00:00.0  | feiji              | 100               | 10                | city=bj             |
| 2020-01-01         | 2020-01-01 00:00:00.0  | hotel               | 100               | 10                | city=bj             |
| 2020-01-01         | 2020-01-01 00:00:00.0  | feiji              | 300               | 30                | {city:bj}           |
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+

set hivevar:hive.jdbc.delete.order_1.day.interval="2020-01-01,2020-01-02";
set hivevar:hive.jdbc.delete.order_1.dt.interval="2020-01-01 00:00:00,2020-01-01 12:00:00";
set hivevar:hive.jdbc.delete.order_1.busi.interval="feiji,zzz";
insert overwrite table tb_pg_write_2 values('2020-01-01', '2020-01-01 00:00:00', 'feiji', 400, 40, '{city:bj}' ),('2020-01-01', '2020-01-01 00:00:00', 'hotel', 400, 40, '{city:bj}');
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+
| 2020-01-01         | 2020-01-01 12:00:00.0  | feiji              | 100               | 10                | city=bj             |
| 2020-01-02         | 2020-01-01 00:00:00.0  | feiji              | 100               | 10                | city=bj             |
| 2020-01-01         | 2020-01-01 00:00:00.0  | feiji              | 400               | 40                | {city:bj}           |
| 2020-01-01         | 2020-01-01 00:00:00.0  | hotel               | 400               | 40                | {city:bj}           |
+--------------------+------------------------+---------------------+-------------------+-------------------+---------------------+



```

#### case 7, read from multi partitioned mysql tables

```hiveql
use db;
drop table tb_part_my_read_1;
create external table tb_part_my_read_1(day date, dt timestamp, busi string, pv int, uv int, info string)
STORED BY 'com.wdw.hive.jdbchandler.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "com.mysql.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mysql://hostname:port/database",
    "hive.sql.jdbc.read-write" = "read",
    "hive.sql.jdbc.username" = "username",
    "hive.sql.jdbc.password" = "password",
    "hive.sql.jdbc.table" = "order_1",
    "hive.sql.jdbc.part.tables" = "order_[1-4]"
);
create external table tb_part_my_read_2(day date, dt timestamp, busi string, pv int, uv int, info string)
STORED BY 'com.wdw.hive.jdbchandler.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "com.mysql.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mysql://hostname:port/database",
    "hive.sql.jdbc.read-write" = "read,write",
    "hive.sql.jdbc.username" = "username",
    "hive.sql.jdbc.password" = "password",
    "hive.sql.jdbc.table" = "order_1",
    "hive.sql.jdbc.delete.type" = "table",
    "hive.sql.jdbc.part.tables" = "order_[1-4]"
); -- this sql fails
select * from tb_part_my_read_1;
select * from tb_part_my_read_1 limit 1;
+-------------------+------------------------+--------------------+------------------+------------------+--------------------+
| 2020-01-01        | 2020-01-01 00:00:00.0  | feiji             | 100              | 10               | {city:bj}          |
+-------------------+------------------------+--------------------+------------------+------------------+--------------------+
```

#### case 8, read from postgres

```hiveql
add jar viewfs://hadoopbeta/user/share/hive/jdbc/jdbc-handler.jar;
drop table tb_part_pg_read_4;
create external table tb_part_pg_read_4(day date, dt timestamp, busi string, pv int, uv int, info string)
STORED BY 'com.wdw.hive.jdbchandler.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://hostname:port/database",
    "hive.sql.jdbc.read-write" = "read",
    "hive.sql.jdbc.username" = "username",
    "hive.sql.jdbc.password" = "password",
    "hive.sql.jdbc.table" = "order_1",
    "hive.sql.jdbc.part.tables" = "order_[1-4]"
);
create external table tb_part_pg_read_5(day date, dt timestamp, busi string, pv int, uv int, info string)
STORED BY 'com.wdw.hive.jdbchandler.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://hostname:port/database",
    "hive.sql.jdbc.read-write" = "read",
    "hive.sql.jdbc.username" = "username",
    "hive.sql.jdbc.password" = "password",
    "hive.sql.jdbc.table" = "order_1",
    "hive.sql.jdbc.part.tables" = "order_1,order_2,order_3,order_4"
);
drop table tb_part_pg_read_6;
add jar viewfs://wdwbeta/user/share/common/hive/jdbc/jdbc-handler-1.0.jar;
create external table tb_part_pg_read_6(day date, dt timestamp, busi string, pv int, uv int, info string)
STORED BY 'com.wdw.hive.jdbchandler.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.jdbc.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://hostname:port/database",
    "hive.sql.jdbc.read-write" = "read",
    "hive.sql.jdbc.username" = "username",
    "hive.sql.jdbc.password" = "password",
    "hive.sql.jdbc.table" = "order_1",
    "hive.sql.jdbc.part.tables" = "/user/corphive/hive/conf/corphive/tb_part_pg_read_6.txt"
);
select * from tb_part_pg_read_6 limit 1;
+-------------------+------------------------+--------------------+------------------+------------------+--------------------+
| 2020-01-01        | 2020-01-01 00:00:00.0  | feiji             | 100              | 10               | {city:bj}          |
+-------------------+------------------------+--------------------+------------------+------------------+--------------------+
```

