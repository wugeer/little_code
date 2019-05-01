---------------------------------------------------
--   File Name：     dm_spark_ddl
--   Description :  dms层建表语句
--   Author :       yangming
--   date：          2018/9/6
---------------------------------------------------
--   Change Activity:
--                   2018/9/6:
---------------------------------------------------

--------------------------------------------- **** SKU-天-店  start ****------------------------------------------------
drop table if exists dms.dm_sku_store_day_road_stock;
create table dms.dm_sku_store_day_road_stock(
  product_code string comment '产品编码',
  color_code string comment '颜色编码',
  size_code string comment '尺码编码',
  store_code string comment '门店编码',
  -- day_date string comment '库存日期',
  road_stock_qty int comment '在途库存',
  etl_time timestamp comment 'etl时间')
  partitioned by (day_date string comment '库存日期')
stored as parquet
location
  '/dw_pb/dms.db/dm_sku_store_day_road_stock';

drop table if exists dms.dm_sku_store_day_available_stock;
create table dms.dm_sku_store_day_available_stock(
  product_code string comment '产品编码',
  color_code string comment '颜色编码',
  size_code string comment '尺码编码',
  store_code string comment '门店编码',
  -- day_date string comment '库存日期',
  available_stock_qty int comment '可用库存',
  etl_time timestamp)
partitioned by (day_date string comment '库存日期')
stored as parquet
location
  '/dw_pb/dms.db/dm_sku_store_day_available_stock';

drop table if exists dms.dm_sku_store_day_stock;
create table dms.dm_sku_store_day_stock(
  product_code string comment '产品编码',
  color_code string comment '颜色编码',
  size_code string comment '尺码编码',
  store_code string comment '门店编码',
  -- day_date string comment '库存日期',
  stock_qty int comment '库存数量',
  etl_time timestamp comment 'etl时间')
partitioned by (day_date string comment '库存日期')
stored as parquet
location
  '/dw_pb/dms.db/dm_sku_store_day_stock';

drop table if exists dms.dm_sku_store_day_sales;
create table dms.dm_sku_store_day_sales(
  product_code string comment '产品编码',
  color_code string comment '颜色编码',
  size_code string comment '尺码编码',
  store_code string comment '门店编码',
  -- day_date string comment '库存日期',
  sales_qty int comment '销售数量',
  sales_amt decimal(30,8) comment '销售额',
  last_seven_days_sales_qty int comment '前7天累计销量',
  last_seven_days_sales_amt decimal(30,8) comment '前7天累计销售额',
  last2_seven_days_sales_qty int comment '上上7天累计销量',
  last_thirty_days_sales_qty int COMMENT '过去30天销量',
  total_sales_qty int comment '累计销量',
  total_sales_amt decimal(30,8) comment '累计销售额',
  etl_time timestamp comment 'etl时间')
partitioned by (day_date string comment '日期')
stored as parquet
location
  '/dw_pb/dms.db/dm_sku_store_day_sales';

drop table if exists dms.dm_sku_store_day_replenish_allot_stock;
create table dms.dm_sku_store_day_replenish_allot_stock(
  product_code string comment '产品编码',
  color_code string comment '颜色编码',
  size_code string comment '尺码编码',
  store_code string comment '门店编码',
  replenish_qty int comment '补货数量',
  allot_out_qty int comment '调出数量',
  allot_in_qty int comment '调入数量',
  available_stock_qty int comment '加在途后库存数量',
  road_stock_qty int comment '在途库存数量',
  after_replenish_allot_qty int comment '补调后加在途加日末库存之和',
  -- day_date string comment '库存日期',
  etl_time timestamp comment 'etl时间')
partitioned by (day_date string comment '日期')
stored as parquet
location
  '/dw_pb/dms.db/dm_sku_store_day_replenish_allot_stock';

--------------------------------------------- **** SKU-天-店  end ****------------------------------------------------

--------------------------------------------- **** SKC-天-店  start ****------------------------------------------------
drop table if exists dms.dm_store_day_expection_focused_skc;
create table dms.dm_store_day_expection_focused_skc(
  product_code string comment '货号',
  color_code string comment '颜色',
  store_code string comment '门店',
  store_expection int comment '门店预期',
  -- day_date string comment '日期',
  is_fullsize string comment '是否断码',
  last_two_week_sales_qty int comment '过去两周销量',
  etl_time timestamp comment 'etl 时间')
partitioned by (day_date string comment '日期')
stored as parquet
location
  '/dw_pb/dms.db/dm_store_day_expection_focused_skc';

drop table if exists dms.dm_skc_store_day_stock;
create table dms.dm_skc_store_day_stock(
  product_code string comment '产品编码',
  color_code string comment '颜色编码',
  store_code string comment '门店编码',
  -- day_date string comment '库存日期',
  stock_qty int comment '库存数量',
  etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location
  '/dw_pb/dms.db/dm_skc_store_day_stock';

drop table if exists dms.dm_skc_store_day_available_stock;
create table dms.dm_skc_store_day_available_stock(
  product_code string comment '产品编码',
  color_code string comment '颜色编码',
  store_code string comment '门店编码',
  -- day_date string comment '库存日期',
  available_stock_qty int comment '可用库存',
  etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location
  '/dw_pb/dms.db/dm_skc_store_day_available_stock';

drop table if exists dms.dm_skc_store_day_available_fullsize;
create table dms.dm_skc_store_day_available_fullsize(
  product_code string comment '产品编码',
  color_code string comment '颜色编码',
  store_code string comment '门店编码',
  -- day_date string comment '统计日期',
  available_is_fullsize string comment '是否齐码',
  etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location
  '/dw_pb/dms.db/dm_skc_store_day_available_fullsize';

drop table if exists dms.dm_skc_store_day_sales;
create table dms.dm_skc_store_day_sales(
  product_code string comment '产品编码',
  color_code string comment '颜色编码',
  store_code string comment '门店编码',
  -- day_date string comment '销售日期',
  sales_qty int comment '销售数量',
  sales_amt decimal(30,8) comment '销售额',
  last_seven_days_sales_qty int comment '前7天累计销量',
  last_seven_days_sales_amt decimal(30,8) comment '前7天累计销售额',
  last_fourteen_days_sales_qty int comment '前14天累计销量',
  last_fourteen_days_sales_amt decimal(30,8) comment '前14天累计销量',
  his_sales_qty int comment '过去14天+本周已过去天数的销量',
  his_sales_amt decimal(30,8) comment '过去14天+本周已过去天数的销售额',
  last_week_sales_qty int comment '上一周累计销量',
  last_week_sales_amt int comment '上一周累计销售额',
  week_sales_qty int comment '本周销量',
  week_sales_amt decimal(30,8) comment '本周销售额',
  total_sales_qty int comment '累计销量',
  total_sales_amt decimal(30,8) comment '累计销售额',
  etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location '/dw_pb/dms.db/dm_skc_store_day_sales';

drop table if exists dms.dm_skc_store_day_road_stock;
create table dms.dm_skc_store_day_road_stock(
  product_code string comment '产品编码',
  color_code string comment '颜色编码',
  store_code string comment '门店编码',
  -- day_date string comment '库存日期',
  road_stock_qty int comment '在途库存',
  etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location
  '/dw_pb/dms.db/dm_skc_store_day_road_stock';

drop table if exists dms.dm_skc_store_day_fullsize;
create table dms.dm_skc_store_day_fullsize(
  product_code string comment '产品编码',
  color_code string comment '颜色编码',
  store_code string comment '门店编码',
  -- day_date string comment '库存日期',
  is_fullsize string comment '是否齐码',
  etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location
  '/dw_pb/dms.db/dm_skc_store_day_fullsize';

drop table if exists dms.dm_skc_store_day_fullprice;
create table dms.dm_skc_store_day_fullprice(
  product_code string comment '产品编码',
  color_code string comment '颜色编码',
  store_code string comment '门店编码',
  -- day_date string comment '销售日期',
  fullprice_qty int comment '正价销量',
  total_fullprice_qty int comment '累计正价销量',
  etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location
  '/dw_pb/dms.db/dm_skc_store_day_fullprice';

drop table if exists dms.dm_skc_store_day_after_replenish_allot_fullsize;
create table dms.dm_skc_store_day_after_replenish_allot_fullsize(
  product_code string comment '产品编码',
  color_code string comment '颜色编码',
  store_code string comment '门店编码',
  -- day_date string comment '库存日期',
  is_fullsize string comment '是否齐码',
  etl_time timestamp comment 'etl时间')
partitioned by (day_date string comment '日期')
stored as parquet
location
  '/dw_pb/dms.db/dm_skc_store_day_after_replenish_allot_fullsize';

drop table if exists dms.dm_skc_store_distributed_info;
create table dms.dm_skc_store_distributed_info(
  product_id string comment '商品id',
  product_code string comment '商品code',
  color_id string comment '颜色id',
  color_code string comment '颜色code',
  store_id string comment '门店id',
  store_code string comment '门店code',
  distri_send_day_date string comment '首铺发出日期',
  distri_receive_day_date string comment '首铺收货日期',
  etl_time timestamp)
stored as parquet
location '/dw_pb/dms.db/dm_skc_store_distributed_info';

--------------------------------------------- **** SKC-天-店  end ****------------------------------------------------
--------------------------------------------- **** 天-组织  start ****------------------------------------------------
drop table if exists dms.dm_org_day_mon_section_fct_sales_amt;
CREATE TABLE dms.dm_org_day_mon_section_fct_sales_amt(
  org_code string COMMENT '组织编码',
  org_long_code string COMMENT '组织长编码',
  org_type string COMMENT '组织类型',
  fct_mon_section_sales_amt decimal(30,8) COMMENT '实际累积销售额',
  etl_time timestamp)
  PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_org_day_mon_section_fct_sales_amt'
;

drop table if exists dms.dm_org_day_mon_section_target_sales_amt;
CREATE TABLE dms.dm_org_day_mon_section_target_sales_amt(
  org_code string COMMENT '组织编码',
  org_long_code string COMMENT '组织长编码',
  org_type string COMMENT '组织类型',
  target_mon_section_sales_amt decimal(30,8) COMMENT '目标累积销售额',
  etl_time timestamp)
  PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_org_day_mon_section_target_sales_amt'
;
--------------------------------------------- **** 天-组织  end ****------------------------------------------------
--------------------------------------------- **** skc-天-城市-toc  start ****-----------------------------------------
drop table if exists dms.dm_skc_city_day_analysis_toc;
CREATE TABLE dms.dm_skc_city_day_analysis_toc(
  product_code string COMMENT '产品编码',
  color_code string COMMENT '颜色编码',
  city_code string COMMENT '城市编码',
  city_long_code string COMMENT '城市长编码',
  is_toc string COMMENT '是否toc',
  no_sales_no_available_stock_store_count int COMMENT '无销量无库存(含在途)门店数',
  has_sales_no_available_stock_store_count int COMMENT '有销量无库存(含在途)门店数',
  no_sales_available_fullsize_store_count int COMMENT '无销量齐码(含在途)门店数',
  no_sales_available_brokensize_store_count int COMMENT '无销量断码(含在途)门店数',
  has_sales_available_brokensize_store_count int COMMENT '有销量断码(含在途)门店数',
  has_sales_available_fullsize_store_count int COMMENT '有销量齐码(含在途)店数',
  has_sales_fullsize_store_count int COMMENT '有销量齐码(不含在途)门店数',
  no_sales_has_available_stock_store_count int COMMENT '无销量有库存门店数(加在途) 用于无销售门店比例分子',
  has_available_stock_store_count int COMMENT '加在途后有库存门店数（用于无销售门店比例分母和销售断码率分母）',
  has_stock_store_count int COMMENT '有库存门店数（无在途）',
  available_brokensize_store_count int COMMENT '断码门店数（加在途）',
  brokensize_store_count int COMMENT '断码店铺数（不加在途）',
  etl_time timestamp)
  PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_skc_city_day_analysis_toc';

drop table if exists dms.dm_skc_city_day_sales_toc;
CREATE TABLE dms.dm_skc_city_day_sales_toc(
  product_code string COMMENT '产品编码',
  color_code string COMMENT '颜色编码',
  city_code string COMMENT '城市编码',
  city_long_code string COMMENT '城市长编码',
  is_toc string COMMENT '是否toc',
  sales_qty int COMMENT '销售数量',
  sales_amt decimal(30,8) COMMENT '销售额',
  last_seven_days_sales_qty int COMMENT '过去七天销量',
  last_seven_days_sales_amt decimal(30,8) COMMENT '过去七天销售额',
  last_fourteen_days_sales_qty int COMMENT '过去十四天销量',
  last_fourteen_days_sales_amt decimal(30,8) COMMENT '过去十四天销售额',
  his_sales_qty int COMMENT '过去十四天+本周已过去天数的销量',
  his_sales_amt decimal(30,8) COMMENT '过去十四天+本周已过去天数的销售额',
  last_week_sales_qty int COMMENT '上周销量',
  last_week_sales_amt decimal(30,8) COMMENT '上周销售额',
  week_sales_qty int COMMENT '本周销量',
  week_sales_amt decimal(30,8) COMMENT '本周销售额',
  total_sales_qty int COMMENT '累计销量',
  total_sales_amt decimal(30,8) COMMENT '累计销售额',
  total_tag_price decimal(30,8) COMMENT '累计吊牌价之和',
  etl_time timestamp)
  PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_skc_city_day_sales_toc';

drop table if exists dms.dm_skc_city_day_available_stock_toc;
create table dms.dm_skc_city_day_available_stock_toc(
    product_code string COMMENT '产品code',
    color_code string COMMENT '颜色code',
    city_code string COMMENT '城市编码',
    city_long_code string COMMENT '城市长编码',
    is_toc string COMMENT '是否toc',
    available_stock_qty int COMMENT '库存数量',
    etl_time timestamp)
    PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION '/dw_pb/dms.db/dm_skc_city_day_available_stock_toc';

drop table if exists dms.dm_skc_city_day_available_distributed;
create table dms.dm_skc_city_day_available_distributed(
product_code string    comment '产品编码',
color_code string    comment '颜色编码',
city_code string    comment '城市编码',
city_long_code string    comment '城市长编码',
available_distributed_store_count int    comment '加在途后有库存门店数',
available_brokensize_store_count int    comment '加在途后断码门店数',
etl_time timestamp)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
location '/dw_pb/dms.db/dm_skc_city_day_available_distributed';

--------------------------------------------- **** skc-天-城市-toc  end ****-----------------------------------------
------------------------------------------------- **** 天-门店  start ****--------------------------------------------
drop table if exists dms.dm_store_day_mon_section_comple_rate;
CREATE TABLE dms.dm_store_day_mon_section_comple_rate(
  store_code string COMMENT '门店编码',
  store_expection int COMMENT '门店分类',
  act_comple_rate decimal(30,8) COMMENT '实际完成率',
  pla_comple_rate decimal(30,8) COMMENT '计划完成率',
  etl_time timestamp)
  PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_store_day_mon_section_comple_rate';

drop table if exists dms.dm_store_day_stock;
CREATE TABLE dms.dm_store_day_stock(
  store_code string COMMENT '门店编码',
  stock_qty int COMMENT '库存',
  etl_time timestamp)
  PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_store_day_stock';\

drop table if exists dms.dm_store_day_brokensize_rate;
CREATE TABLE dms.dm_store_day_brokensize_rate(
  store_code string COMMENT '门店编码',
  brokensize_rate decimal(30,8) COMMENT '断码率',
  etl_time timestamp)
  PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_store_day_brokensize_rate';
 
drop table if exists dms.dm_store_day_sell_well_skc;    
CREATE TABLE dms.dm_store_day_sell_well_skc(
  store_code string COMMENT '门店 code', 
  product_code string COMMENT '产品code', 
  color_code string COMMENT '颜色code', 
  order_num int COMMENT '在门店销售排名', 
  etl_time timestamp)
  PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_store_day_sell_well_skc';


drop table if exists dms.dm_store_day_sell_well_brokensize_rate;
CREATE TABLE dms.dm_store_day_sell_well_brokensize_rate(
  store_code string COMMENT '门店 code',
  sell_well_brokensize_rate decimal(30,8) COMMENT '畅销款断码率',
  etl_time timestamp)
  PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_store_day_sell_well_brokensize_rate';
 
drop table if exists dms.dm_store_day_sales_analysis;    
CREATE TABLE dms.dm_store_day_sales_analysis(
  store_code string COMMENT '门店编码', 
  last_fourteen_days_sales_qty int COMMENT '过去十四天销量', 
  last_week_sales_qty int COMMENT '上周销量', 
  etl_time timestamp)
  PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_store_day_sales_analysis';

drop table if exists dms.dm_store_day_expection;    
CREATE TABLE dms.dm_store_day_expection(
  store_code string COMMENT '门店编码', 
  store_expection int COMMENT '门店分类', 
  etl_time timestamp)
  PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet  
LOCATION
  '/dw_pb/dms.db/dm_store_day_expection';


-------------------------------------------------- **** 天-门店  end ****---------------------------------------------

--------------------------------------------- **** SKC-天-全国  start ****----------------------------------------------
drop table if exists dms.dm_skc_country_day_road_stock;
create table dms.dm_skc_country_day_road_stock (
  product_code   string comment '产品编码',
  color_code     string comment '颜色编码',
  -- day_date       string comment '库存日期',
  road_stock_qty int comment '在途库存',
  etl_time       timestamp
)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_skc_country_day_road_stock';


drop table if exists dms.dm_skc_country_day_stock;
create table dms.dm_skc_country_day_stock (
  product_code              string,
  color_code                string,
  -- day_date                  string,
  warehouse_total_stock_qty int,
  store_total_stock_qty     int,
  etl_time                  timestamp
)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_skc_country_day_stock';

drop table if exists dms.dm_skc_country_day_sales;
create table dms.dm_skc_country_day_sales (
  product_code              string comment '产品编码',
  color_code                string comment '颜色编码',
  first_sale_date           string comment '首销日期',
  -- day_date                  string comment '销售日期',
  sales_qty                 int comment '销售数量',
  sales_amt                 decimal(30, 8) comment '销售额',
  total_sales_qty           int comment '累计销量',
  total_sales_amt           decimal(30, 8) comment '累计销售额',
  fullprice_total_amt       decimal(30, 8) comment '正价累计销售额',
  total_tag_price           decimal(30, 8) comment '累计吊牌价之和',
  last_seven_days_sales_qty int comment '前7天累计销量',
  week_sales_qty            int comment '本周(截止到当前)销量',
  last_week_sales_qty       int comment '上周销量',
  last_two_week_sales_qty   int comment '上两周销量',
  last_three_week_sales_qty int comment '上三周销量',
  last_four_week_sales_qty  int comment '上四周销量',
  last_five_week_sales_qty  int comment '上五周销量',
  etl_time                  timestamp
)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_skc_country_day_sales';

drop table if exists dms.dm_skc_country_day_life_cycle;
create table dms.dm_skc_country_day_life_cycle (
  product_code     string comment '产品编码',
  color_code       string comment '颜色编码',
  -- day_date         string comment '计算日期',
  life_cycle_week  int comment '生命周期',
  distributed_days int,
  etl_time         timestamp
)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_skc_country_day_life_cycle';

drop table if exists dms.dm_skc_country_day_pre_classify;
create table dms.dm_skc_country_day_pre_classify (
  product_code              string comment '产品编码',
  color_code                string comment '颜色编码',
  distributed_days          int comment '日期间隔',
  stage_lifetime            int comment '生命周期',
  -- day_date                  string comment '分类日期',
  last_seven_days_sales_qty int comment '前7天累计销量',
  total_sales_qty           int comment '累计销量',
  warehouse_total_stock_qty int comment '总仓库存',
  store_total_stock_qty     int comment '门店总库存',
  road_stock_qty            int comment '在途库存',
  sales_out_rate            decimal(30, 8) comment '售罄率',
  pre_class                 string comment '预期分类',
  etl_time                  timestamp
)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_skc_country_day_pre_classify';

drop table if exists dms.dm_skc_country_day_sales_weeks;
create table dms.dm_skc_country_day_sales_weeks (
  product_code string comment '产品编码',
  color_code   string comment '颜色编码',
  -- day_date     string comment '计算日期',
  sales_weeks  int comment '已售卖周数',
  etl_time     timestamp
)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_skc_country_day_sales_weeks';

drop table if exists dms.dm_skc_country_day_brokensize_rate;
create table dms.dm_skc_country_day_brokensize_rate (
  product_code    string comment '产品编码',
  color_code      string comment '颜色编码',
  -- day_date        string comment '日期',
  brokensize_rate decimal(30, 8) comment '某skc断码率',
  etl_time        timestamp comment 'etl时间'
)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_skc_country_day_brokensize_rate';
--------------------------------------------- **** SKC-天-全国  end ****------------------------------------------------

--------------------------------------------- **** SKU-天-全国  start ****----------------------------------------------
drop table if exists dms.dm_sku_country_day_stock;    
CREATE TABLE dms.dm_sku_country_day_stock(
  product_code string COMMENT '产品编码', 
  color_code string COMMENT '颜色编码', 
  size_code string COMMENT '尺码编码', 
  warehouse_total_stock_qty int COMMENT '总仓库存', 
  store_total_stock_qty int COMMENT '门店总库存', 
  etl_time timestamp COMMENT 'etl时间')
  PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_sku_country_day_stock';
--------------------------------------------- **** SKU-天-全国  end ****------------------------------------------------

--------------------------------------------- **** SKC-全国  start ****------------------------------------------------
drop table if exists dms.dm_skc_country_order;  
CREATE TABLE dms.dm_skc_country_order(
  product_code string, 
  color_code string, 
  total_otb_order_qty int, 
  total_otb_order_rank int, 
  total_otb_order_in_tinyclass_rank int, 
  etl_time timestamp)
LOCATION
  '/dw_pb/dms.db/dm_skc_country_order';
 
drop table if exists dms.dm_skc_country_add_order;
CREATE TABLE dms.dm_skc_country_add_order(
  product_code string COMMENT '产品编码', 
  color_code string COMMENT '颜色编码', 
  add_order_date string COMMENT '追单日期', 
  add_order_qty int COMMENT '追单数量', 
  arrived_qty int COMMENT '已到货数量', 
  not_arrived_qty int COMMENT '未到货数量', 
  etl_time timestamp)
LOCATION
  '/dw_pb/dms.db/dm_skc_country_add_order';
 
drop table if exists dms.dm_skc_country_add_order_expected_arrival;  
CREATE TABLE dms.dm_skc_country_add_order_expected_arrival(
  product_code string COMMENT '产品编码', 
  color_code string COMMENT '颜色编码', 
  expected_arrival_date string COMMENT '预计到货日期', 
  expected_arrival_qty int COMMENT '预计到货数量', 
  etl_time timestamp)
LOCATION
  '/dw_pb/dms.db/dm_skc_country_add_order_expected_arrival';

---------------------------------------------  **** SKC-全国  end ****-------------------------------------------------
--------------------------------------------- **** 小类-天-全国  start ****------------------------------------------------
drop table if exists dms.dm_tiny_class_country_day_sales;
CREATE TABLE dms.dm_tiny_class_country_day_sales(
  year_id int COMMENT '年份', 
  quarter_id string COMMENT '季节', 
  tiny_class string COMMENT '小类', 
  total_sales_qty int COMMENT '累计销量', 
  fullprice_total_amt decimal(30,8) COMMENT '正价累计销售额', 
  total_sales_amt decimal(30,8) COMMENT '累计销售额', 
  total_tag_price decimal(30,8) COMMENT '累计吊牌价之和', 
  etl_time timestamp)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_tiny_class_country_day_sales'
;
 
drop table if exists dms.dm_tiny_class_country_day_stock;    
CREATE TABLE dms.dm_tiny_class_country_day_stock(
  year_id int COMMENT '年份', 
  quarter_id string COMMENT '季节', 
  tiny_class string COMMENT '小类', 
  warehouse_total_stock_qty int COMMENT '总仓总库存', 
  store_total_stock_qty int COMMENT '门店总库存', 
  road_total_stock_qty int COMMENT '在途库存', 
  etl_time timestamp)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_tiny_class_country_day_stock';
--------------------------------------------- **** 小类-天-全国  end ****------------------------------------------------
--------------------------------------------- **** skc分类-天-全国  start ****-------------------------------------------
drop table if exists dms.dm_skc_classify_day_analysis;
CREATE TABLE dms.dm_skc_classify_day_analysis(
  year_id int COMMENT '年份',
  quarter_id string COMMENT '季节',
  prod_class string COMMENT '产品分类', 
  stage_lifetime string COMMENT '生命周期阶段', 
  total_sales_qty int COMMENT '累计销量', 
  sales_rate decimal(30,8) COMMENT '销量占比', 
  etl_time timestamp)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_skc_classify_day_analysis';
--------------------------------------------- **** skc分类-天-全国  end ****---------------------------------------------
------------------------------------------- **** skc分类波段-天-全国  start ****------------------------------------------
drop table if exists dms.dm_skc_classify_band_country_day_has_sales;  
CREATE TABLE dms.dm_skc_classify_band_country_day_has_sales(
  prod_class string COMMENT '产品分类', 
  stage_lifetime string COMMENT '生命周期阶段', 
  year_id int COMMENT '年份', 
  quarter_id string COMMENT '季节', 
  band string COMMENT '波段', 
  has_sales_store_qty int COMMENT '七天有销量门店数', 
  etl_time timestamp)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_skc_classify_band_country_day_has_sales';
  
drop table if exists dms.dm_skc_classify_band_country_day_sales;
CREATE TABLE dms.dm_skc_classify_band_country_day_sales(
  prod_class string COMMENT '产品分类', 
  stage_lifetime string COMMENT '生命周期阶段', 
  year_id int COMMENT '年份', 
  quarter_id string COMMENT '季节', 
  band string COMMENT '波段', 
  total_sales_qty int COMMENT '累计销量', 
  last_seven_days_sales_qty int COMMENT '过去七天销量', 
  etl_time timestamp)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_skc_classify_band_country_day_sales';
   
drop table if exists dms.dm_skc_classify_band_country_day_stock;
CREATE TABLE dms.dm_skc_classify_band_country_day_stock(
  prod_class string COMMENT '产品分类', 
  stage_lifetime string COMMENT '生命周期阶段', 
  year_id int COMMENT '年份', 
  quarter_id string COMMENT '季节', 
  band string COMMENT '波段', 
  warehouse_total_stock_qty int COMMENT '总仓总库存', 
  store_total_stock_qty int COMMENT '门店总库存', 
  road_total_stock_qty int COMMENT '在途库存', 
  etl_time timestamp)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_skc_classify_band_country_day_stock';
  
drop table if exists dms.dm_skc_classify_band_country_on_sales_days;
CREATE TABLE dms.dm_skc_classify_band_country_on_sales_days(
  prod_class string COMMENT '产品分类', 
  stage_lifetime string COMMENT '生命周期阶段', 
  year_id int COMMENT '年份', 
  quarter_id string COMMENT '季节', 
  band string COMMENT '波段', 
  on_sales_days int COMMENT '上市天数',  
  etl_time timestamp)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_skc_classify_band_country_on_sales_days';
 
drop table if exists dms.dm_skc_classify_band_country_day_available_distributed;
CREATE TABLE dms.dm_skc_classify_band_country_day_available_distributed(
  prod_class string COMMENT '产品分类', 
  stage_lifetime string COMMENT '生命周期阶段', 
  year_id int COMMENT '年份', 
  quarter_id string COMMENT '季节', 
  band string COMMENT '波段', 
  brokensize_count string COMMENT '断码商品数', 
  distributed_store_count string COMMENT '品铺货门店组合数', 
  etl_time timestamp)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION '/dw_pb/dms.db/dm_skc_classify_band_country_day_available_distributed';
--------------------------------------------- **** skc分类波段-天-全国  end ****-------------------------------------------
-------------------------------------------------- **** 天-全国  start ****---------------------------------------------
drop table if exists dms.dm_comp_store_sales_analysis;
CREATE TABLE dms.dm_comp_store_sales_analysis(
  last_total_sales_amt decimal(30,8) COMMENT '上一时间间隔内的销售总额', 
  total_sales_amt decimal(30,8) COMMENT '时间间隔内的销售总额', 
  etl_time timestamp)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_comp_store_sales_analysis';
 
drop table if exists dms.dm_country_day_analysis;
CREATE TABLE dms.dm_country_day_analysis(
  year_id int COMMENT '年份id', 
  quarter_id string COMMENT '季节id', 
  total_sales_qty int COMMENT '累计销量', 
  road_stock_qty int COMMENT '在途库存', 
  warehouse_total_stock_qty int COMMENT '总仓库存', 
  store_total_stock_qty int COMMENT '门店总库存', 
  store_count int COMMENT '店铺数', 
  brokensize_store_count int COMMENT '断码店铺数', 
  etl_time timestamp)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_country_day_analysis';
--------------------------------------------------- **** 天-全国  end **** ---------------------------------------------
------------------------------------------------ **** 细分小类-天-门店  start **** ----------------------------------------
drop table if exists dms.dm_mictiny_class_store_day_stock;
CREATE TABLE dms.dm_mictiny_class_store_day_stock(
  mictiny_class string COMMENT '细分小类', 
  store_code string COMMENT '门店编码', 
  stock_qty int COMMENT '库存', 
  stock_amt decimal(30,8) COMMENT '库存金额', 
  etl_time timestamp COMMENT 'etl时间', 
  year_id int COMMENT '年份', 
  quarter_id string COMMENT '季节')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_mictiny_class_store_day_stock';
  
drop table if exists dms.dm_mictiny_class_store_day_total_sales;
CREATE TABLE dms.dm_mictiny_class_store_day_total_sales(
  mictiny_class string COMMENT '细分小类', 
  store_code string COMMENT '门店编码', 
  total_sales_qty int COMMENT '累计销量', 
  total_sales_amt decimal(30,8) COMMENT '累计销售额', 
  etl_time timestamp COMMENT 'etl时间', 
  year_id int COMMENT '年份', 
  quarter_id string COMMENT '季节')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_mictiny_class_store_day_total_sales';
 
drop table if exists dms.dm_store_mictiny_class_replenish_allot_comparison;
CREATE TABLE dms.dm_store_mictiny_class_replenish_allot_comparison(
  store_code string COMMENT '门店编码', 
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  big_class string COMMENT '大类', 
  tiny_class string COMMENT '小类', 
  mictiny_class string COMMENT '细分品类', 
  last_seven_days_sales_qty int COMMENT '过去七天销量', 
  replenish_qty int COMMENT '补货量', 
  receive_qty int COMMENT '调入量', 
  send_qty int COMMENT '调出量', 
  available_sell_well_skc_count int COMMENT '补调前畅销款有库存SKC数', 
  available_all_skc_count int COMMENT '补调前全款有库存SKC数', 
  available_sell_well_stock_qty int COMMENT '补调前畅销款库存数量', 
  available_all_skc_stock_qty int COMMENT '补调前全款库存数量', 
  available_sell_well_brokensize_numerator int COMMENT '补调前畅销款断码率分子', 
  available_sell_well_brokensize_denominator int COMMENT '补调前畅销款断码率分母', 
  available_all_brokensize_numerator int COMMENT '补调前全款断码率分子', 
  available_all_brokensize_denominator int COMMENT '补调前全款断码率分母',
  after_sell_well_skc_count int COMMENT '补调后畅销款有库存SKC数', 
  after_all_skc_count int COMMENT '补调后全款有库存SKC数', 
  after_sell_well_stock_qty int COMMENT '补调后畅销款库存数量', 
  after_all_skc_stock_qty int COMMENT '补调后全款库存数量', 
  after_sell_well_brokensize_numerator int COMMENT '补调后畅销款断码率分子', 
  after_sell_well_brokensize_denominator int COMMENT '补调后畅销款断码率分母', 
  after_all_brokensize_numerator int COMMENT '补调后全款断码率分子', 
  after_all_brokensize_denominator int COMMENT '补调后全款断码率分母', 
  etl_time timestamp COMMENT 'etl时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_store_mictiny_class_replenish_allot_comparison'
;
------------------------------------------------- **** 细分小类-天-门店  end **** ------------------------------------------
------------------------------------------------- **** 数据量校验  start **** ------------------------------------------
drop table if exists dms.dm_table_day_data_count;
CREATE TABLE dms.dm_table_day_data_count(
  table_name string COMMENT '表名:schema+表名', 
  data_count int COMMENT '数据量', 
  etl_time timestamp COMMENT 'etl时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/dms.db/dm_table_day_data_count';
------------------------------------------------- **** 数据量校验  start **** ------------------------------------------