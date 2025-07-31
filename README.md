# 一.软件简介

本软件用于从PostgreSQL数据库中将地震波数据和标准答案进行对比以输出统计结果，支持将统计结果与对比明细导出到excel。

# 二.运行环境

•	编译环境为 Windows 11 x64，QT6.8.1，MSVC2022_64。

•	需提供权限开放的PostgreSQL数据库。

# 三.文件说明

- lib：QT和postgres数据库的动态依赖库dll
- scripts：导入和导出功能的python脚本源码及打包exe
- sql：实现统计查询的sql
- test_tables.sql 导入测试地震波数据
- StandardAnswersFile.csv 与测试地震波匹配的标准答案文件，供测试

# 四.软件界面及使用说明

![image-20250708135939720](https://github.com/KaiBai03/StatFromDB/blob/master/界面截图.png)

•	输入完整配置信息可进行自定义连接，全部置空则会根据exe同目录下config.ini进行配置连接，config.ini中应存在database字段以及完整配置信息，config,ini示例如下

```
[database]
host = localhost
port = 5432
dbname = TestDB
user = postgres
password =12345
```

•	在成功连接数据库后会读取库中所有模式填充在下拉栏供选择，请确保选择的模式中存在表station_p_wave_alarm和station_s_wave_alarm。



•	标准答案文件应为csv格式，请确保标准答案与数据库中的地震波信息匹配。

•	为了确保数据库与查询结果为最新，不被残留信息干扰，每次操作需按连接数据库->导入标准答案->查询->导出查询结果顺序，否则会进行弹窗提示。

•	导出查询结果后会在提示目录下生成数据明细和统计结果，统计结果中应存在14个sheet存放不同条目，同时会在目录中生成导入和导出日志，在发生意外状况（导入失败，导出数据不全）时请检查日志。

# 五.数据库结构说明

•	新建的数据库将默认存在public模式，在程序运行途中将在public模式下更新或新建以下表：

details，standanswer_p_wave_alarm，stand_answer，

p_compare_result，p_matched_details，p_epicenter_deviation，p_judge_time，p_mag_deviation，p_send_time，p_warning_miss，

s_compare_result，s_matched_details，s_peak_deviation，s_warning_miss，s_40gal_send_time，s_40gal_judge_time，s_80gal_send_time，s_80gal_judge_time，s_120gal_send_time，s120gal_judge_time，

s_alarm_before_p

id_matched_p_filtered_by_time，id_matched_p_filtered_by_time，id_matched_s_wave_info，id_matched_s_filtered_by_time，

time_filtered_p_send_time，time_filtered_s_80gal_send_time，time_filtered_s_40gal_send_time，time_filtered_s_120gal_send_time

共21张，若public模式下原本存在相同表名，请注意备份以免意外丢失。

•	界面中选择的模式中应存在表station_p_wave_alarm和station_s_wave_alarm，这个模式可以是public，它们的结构应至少包含以下字段，可以存在多余字段但不会被统计。

station_p_wave_alarm:

```sql
station_p_wave_alarm_id varchar NOT NULL,
earth_time timestamp without time zone,
p_arrive_time timestamp without time zone,
jk_time timestamp without time zone,
rcv_jktime timestamp without time zone,
earth_id varchar,
receive_time timestamp without time zone,
sta_code varchar,
device_code varchar,
source_longitude double precision,
source_latitude double precision,
source_deep double precision,
alarm_time timestamp without time zone,
s_time timestamp without time zone,
x_acc_value double precision,
y_acc_value double precision,
z_acc_value double precision,
epi_dist double precision,
alarm_level smallint,
azi_angle double precision,
earthquake_level double precision,
report_num integer,
PRIMARY KEY(station_p_wave_alarm_id)
```

station_s_wave_alarm:

```sql
station_s_wave_alarm_id varchar NOT NULL,
alarm_level smallint,
earth_id varchar,
sta_code varchar,
device_code varchar,
alarm_time timestamp without time zone,
receive_time timestamp without time zone,
jk_time timestamp without time zone,
rcv_jktime timestamp without time zone,
x_acc_value double precision,
y_acc_value double precision,
z_acc_value double precision,
earthquake_level double precision,
source_longitude double precision,
source_latitude double precision,
source_deep double precision,
report_num integer,
PRIMARY KEY(station_s_wave_alarm_id)
```

# 六,数据统计方法(SQL逻辑)

## 计算对象

​	为了保证实时性，对于每一次地震波测试，针对P波预警首报信息，S波分别达到阈值40gal，80gal，120gal的首报信息进行数据统计计算，将以上关注上报信息筛选出并分别存入表**public.p_matched_details**，简记**pmd**，和表**public.s_matched_details**，简记**smd**，它们的表结构和被统计计算的字段意义如下：

p_matched_details:

```sql
CREATE TABLE p_matched_details(
    id integer,
    send_time timestamp without time zone,  
    next_send_time timestamp without time zone,  
    first_jk_time timestamp without time zone,   首报台站发出时间
    first_rcv_jktime timestamp without time zone,  首报前端接收时间
    sta_code varchar, 
    device_code varchar,  
    source_longitude numeric,  首报经度
    source_latitude numeric,  首报纬度 
    epi_dist numeric,  首报震中距
    azi_angle numeric,  首报方位角
    earthquake_level numeric  首报震级
);
```

s_matched_details:

```sql
CREATE TABLE s_matched_details(
    id integer,
    send_time timestamp without time zone,  地震波发送时间
    next_send_time timestamp without time zone,  下一次地震波发送时间
    first_jk_time timestamp without time zone,  首次达到40gal阈值台站发出时间
    first_rcv_jktime timestamp without time zone,  首次达到40gal阈值前端接收时间
    gal80_jk_time timestamp without time zone,  首次达到80gal阈值台站发出时间
    gal80_rcv_jktime timestamp without time zone,  首次达到80gal阈值前端接收时间
    gal120_jk_time timestamp without time zone,  首次达到120gal阈值台站发出时间
    gal120_rcv_jktime timestamp without time zone,  首次达到120gal阈值前端接收时间
    sta_code varchar,
    device_code varchar,
    wave_peak numeric  加速度峰值
);
```

标准答案**standanswer_p_wave_alarm**简记为**stand**。



## P波预警统计项目

P波预警传输时延：

统计对象为台站首报发出至中心接收耗时:

**pmd.first_receive_time  -  pmd.jk_time**

该项目的统计结果包括：总数，合格数，合格率，最大值，最小值，平均值。传输时延≤0.1s时合格，合格率要求为≥95%。



P波预警首报震中位置偏差：

统计对象为：监控单元预警经纬度和中心系统的警报记录经纬度采用Haversine公式计算出的球面距离，用于公式计算的字段有：

pmd.source_longitude(预警经度)，pmd.source_latitude(预警纬度)，stand.actual_longitude(实际经度)，stand.actual_latitude(实际纬度)

该项目的统计结果包括：总数，偏差≤60km组数，偏差≤60km占比，偏差≤100km组数，偏差≤100km占比，偏差最大值，对于偏差≤60km和100km的占比标准为≥60%和≥80，偏差最大值应不大于300km。



P波预警首报判别时间：

统计对象为台站首报发出时间与标准答案P波初至时间之差：

**stand.p_wave_time  -  pmd.first_jk_time**

该项目的统计结果包括：总数，合格数，合格率，平均值。判别时间≤3s时合格，合格率要求为≥90%。



P波首报震级偏差：

统计对象为台站首报预警震级与标准答案实际震级之差：

**ABS(pmd.eathquake_level-stand.actual_magnitude)**

该项目的统计结果包括：总数，合格数，合格率，平均值。偏差≤1时合格，合格率要求为≥75%。



P波预警漏报率：

统计对象为监控单元未发出P波预警的次数：

**COUNT(pmd.first_jk_time IS NULL)**

该项目的统计结果包括：总数，漏报数，漏报率。漏报率应≤5%。



## 阈值报警

阈值报警传输时间：

统计对象为台站对应阈值首报发出至中心接收耗时:

**smd.first_rcv_jktime - smd.first_jk_time**  40gal阈值

 **smd.gal80_rcv_jktime - smd.gal80_jk_time**  80gal阈值

 **smd.gal120_rcv_jktime - smd.gal120_jk_time**  120gal阈值

每一阈值统计结果都包含：总数，合格数，合格率，最大值，最小值，平均值。传输时间≤0.1s时合格，三个阈值合格率要求均为≥95%。



阈值报警漏报率：

统计对象为监控单元在达到阈值时未发出报警的次数：

**COUNT(smd.first_jk_time IS NULL)**

该项目的统计结果包括：总数，漏报数，漏报率。阈值报警漏报率应=0%。



阈值报警判别时间：

统计对象为不同阈值下台站首报发出时间分别与标准答案一报/二报/三报时间之差

**smd.first_jk_time - stand.s_wave_first_time** 40gal阈值

**smd.gal80_jk_time - stand.s_wave_second_time**  80gal阈值

**smd.gal120_jk_time - stand.s_wave_second_time** 120gal阈值

每一阈值统计结果都包含：总数，合格数，合格率，最大值，最小值，平均值。判别时间≤0.5s时合格，三个阈值合格率要求均为≥70%。



阈值报警最大偏差：

统计对象为台站接收加速度峰值与标准答案峰值之差除以标准答案峰值：

**ABS(stand.actual_peak - smd.wave_peak)/stand.actual_peak**

该项目的统计结果包括：总数，合格数，合格率，最大偏差百分比。偏差百分比≤5%时合格，合格率要求≥95%。



## 先报警后预警

同一地震波下先发出阈值报警后发出P波预警的次数：

**COUNT(pmd.first_jk_time>smd.first_jk_time)**









