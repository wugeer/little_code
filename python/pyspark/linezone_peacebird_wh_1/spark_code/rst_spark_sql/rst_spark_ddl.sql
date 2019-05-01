---------------------------------------------------
--   File Name：     rst_spark_ddl
--   Description :
--   Author :       yangming
--   date：          2018/9/12
---------------------------------------------------
--   Change Activity:
--                   2018/9/12:
---------------------------------------------------

-----------------------------------------------   区域决策页面   start    -------------------------------------------------
drop table if exists rsts.rst_store_day_expection;
CREATE TABLE rsts.rst_store_day_expection(
  store_code string COMMENT '门店', 
  store_name string COMMENT '门店名称', 
  store_expection int COMMENT '门店分类', 
  pla_comple_rate decimal(30,8) COMMENT '计划完成率', 
  act_comple_rate decimal(30,8) COMMENT '实际完成率', 
  temperate_zone string COMMENT '温带', 
  region_code string COMMENT '大区', 
  region_long_code string COMMENT '大区长编码', 
  zone_code string COMMENT '区域', 
  city_code string COMMENT '城市', 
  city_long_code string COMMENT '城市', 
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_store_day_expection';

drop table if exists rsts.rst_skc_country_day_expection_analysis;
CREATE TABLE rsts.rst_skc_country_day_expection_analysis(
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  product_code string COMMENT '货号', 
  product_name string COMMENT '货号名称', 
  color_code string COMMENT '颜色', 
  color_name string COMMENT '颜色名字', 
  pre_class int COMMENT 'skc超低内预期', 
  sale_out_rate decimal(30,8) COMMENT '售罄率', 
  brokensize_rate decimal(30,8) COMMENT '断码率', 
  sale_inventory_rate decimal(30,8) COMMENT '发销率', 
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_skc_country_day_expection_analysis';
 
drop table if exists rsts.rst_store_day_focused_skc_in_store;
CREATE TABLE rsts.rst_store_day_focused_skc_in_store(
  product_code string COMMENT '货号', 
  product_name string COMMENT '货号名称', 
  color_code string COMMENT '颜色', 
  color_name string COMMENT '颜色名字', 
  store_code string COMMENT '门店编码', 
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  last_two_week_sales_qty int COMMENT '过去两周销量', 
  stock_qty int COMMENT '库存', 
  is_fullsize string COMMENT '是否断码', 
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_store_day_focused_skc_in_store';
 
drop table if exists rsts.rst_store_day_focused_skc_in_country; 
CREATE TABLE rsts.rst_store_day_focused_skc_in_country(
  product_code string COMMENT '货号', 
  product_name string COMMENT '货号名称', 
  color_code string COMMENT '颜色', 
  color_name string COMMENT '颜色名字', 
  store_code string COMMENT '门店编码', 
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  sale_out_rate decimal(30,8) COMMENT '售罄率', 
  brokensize_rate decimal(30,8) COMMENT '断码率', 
  sale_inventory_rate decimal(30,8) COMMENT '发销率', 
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_store_day_focused_skc_in_country';
  
rsts.rst_skc_city_week_sales_change_comp;   -- 该表也用在单品策略分析页面

drop table if exists rsts.rst_skc_city_week_suitability;
create table rsts.rst_skc_city_week_suitability(
year int    comment '年份',
quarter string    comment '季节',
region_code string    comment '大区',
region_long_code string    comment '大区长编码',
zone_long_code string    comment '区域经理长编码',
zone_name string    comment '区域经理名称',
city_code string    comment '城市',
city_long_code string    comment '城市长编码',
send_suit_numerator int    comment '发货匹配度分子',
send_suit_denominator int    comment '发货匹配度分母',
receive_suit_numerator int    comment '收货匹配度分子',
receive_suit_denominator int    comment '收货匹配度分母',
composite_suit_numerator int    comment '综合匹配度分子',
composite_suit_denominator int    comment '综合匹配度分母',
--week_date string    comment '周日期',
etl_time timestamp    comment 'etl时间'
)
PARTITIONED BY (week_date string COMMENT '日期')
STORED AS parquet   
location '/dw_pb/rsts.db/rst_skc_city_week_suitability';
 
-----------------------------------------------   区域决策页面   end   ---------------------------------------------------
-----------------------------------------------   门店销售分析页面   start  -----------------------------------------------
drop table if exists rsts.rst_store_day_overall; 
CREATE TABLE rsts.rst_store_day_overall(
  store_code string COMMENT '门店', 
  store_name string COMMENT '门店名称', 
  store_expection int COMMENT '门店分类', 
  stock_qty int COMMENT '商品库存', 
  brokensize_rate decimal(30,8) COMMENT '商品断码率', 
  sell_well_brokensize_rate decimal(30,8) COMMENT '畅销款断码率', 
  two_week_sales_qty int COMMENT '两周销', 
  turnover_weeks decimal(30,8) COMMENT '平均两周库存周转', 
  temperate_code string COMMENT '温带', 
  region_code string COMMENT '大区', 
  region_long_code string COMMENT '大区长编码', 
  region_name string COMMENT '大区名称', 
  zone_code string COMMENT '区域', 
  city_code string COMMENT '城市', 
  city_long_code string COMMENT '城市', 
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_store_day_overall'; 
 
drop table if exists rsts.rst_org_day_mon_section_fct_sales_amt;
CREATE TABLE rsts.rst_org_day_mon_section_fct_sales_amt(
  org_code string COMMENT '组织编码', 
  org_long_code string COMMENT '组织长编码', 
  org_type string COMMENT '组织类型', 
  fct_mon_section_sales_amt decimal(30,8) COMMENT '月区间实际销售额', 
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_org_day_mon_section_fct_sales_amt'; 

drop table if exists rsts.rst_org_day_mon_section_target_sales_amt;
CREATE TABLE rsts.rst_org_day_mon_section_target_sales_amt(
  org_code string COMMENT '组织编码', 
  org_long_code string COMMENT '组织长编码', 
  org_type string COMMENT '组织类型', 
  target_mon_section_sales_amt decimal(30,8) COMMENT '月区间计划销售额', 
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_org_day_mon_section_target_sales_amt'; 

-----------------------------------------------   门店销售分析页面   end  ------------------------------------------------
-----------------------------------------------   门店细分品类页面   start  -----------------------------------------------
drop table if exists rsts.rst_mictiny_class_store_day;
CREATE TABLE rsts.rst_mictiny_class_store_day(
  store_code string COMMENT '门店', 
  store_name string COMMENT '门店名称', 
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  mictiny_class string COMMENT '细分品类', 
  stock_qty int COMMENT '库存数量', 
  sales_qty int COMMENT '销量', 
  stock_amt decimal(30,8) COMMENT '库存金额', 
  sales_amt decimal(30,8) COMMENT '销售金额', 
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_mictiny_class_store_day';
-----------------------------------------------   门店细分品类页面   end  -----------------------------------------------
-----------------------------------------------   销售总览页面   start  -----------------------------------------------
drop table if exists rsts.rst_skc_tiny_class_country_day_pre_classify;
CREATE TABLE rsts.rst_skc_tiny_class_pre_classify(
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  tiny_class string COMMENT '小类', 
  product_code string COMMENT '货号', 
  product_name string COMMENT '商品名称', 
  color_code string COMMENT '颜色编码', 
  color_name string COMMENT '颜色名称', 
  distributed_days string COMMENT '售卖天数', 
  sales_out_rate decimal(30,8) COMMENT '售罄率', 
  pre_class string COMMENT '预期分类', 
  class_celling_slope decimal(30,8) COMMENT '分类上限斜率', 
  class_floor_slope decimal(30,8) COMMENT '分类下限斜率', 
  etl_time timestamp)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_skc_tiny_class_country_day_pre_classify';
 
drop table if exists rsts.rst_overall_tag;
CREATE TABLE rsts.rst_overall_tag(
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  brokensize_rate decimal(30,8) COMMENT '断码率', 
  sales_out_rate decimal(30,8) COMMENT '售罄率', 
  comp_store_perf_growth decimal(30,8) COMMENT '同比门店业绩增长', 
  etl_time timestamp)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_overall_tag';
 
drop table if exists rsts.rst_skc_overall;
CREATE TABLE rsts.rst_skc_overall(
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  tiny_class string COMMENT '小类', 
  product_code string COMMENT '货号', 
  product_name string COMMENT '商品名称', 
  color_code string COMMENT '颜色', 
  color_name string COMMENT '颜色', 
  pre_class string COMMENT '预期分类', 
  stage_lifetime int COMMENT '生命周期', 
  sales_out_rate decimal(30,8) COMMENT '售罄率', 
  brokensize_rate decimal(30,8) COMMENT '断码率', 
  distributed_rate decimal(30,8) COMMENT '铺货率', 
  sales string COMMENT '销量：上七天销量/累计销量', 
  add_order_info string COMMENT '追单信息：追单数量/追单未到货数量', 
  warehouse_total_stock_qty int COMMENT '总仓库存', 
  two_week_sales_qty_pre string COMMENT '预计两周销量上下限', 
  four_week_sales_qty_pre string COMMENT '预计四周销量上下限', 
  residue_sales_pre string COMMENT '预计剩余生命周期销量上下限', 
  sug_add_order_qty string COMMENT '建议追单量', 
  not_add_order_sales_loss string COMMENT '不追单销售损失', 
  etl_time timestamp,
  store_total_stock_qty int COMMENT '门店总库存',
  total_sales_qty int COMMENT '累计销量')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_skc_overall';
 
drop table if exists rsts.rst_skc_tiny_class_count;
CREATE TABLE rsts.rst_skc_tiny_class_count(
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  tiny_class string COMMENT '小类', 
  pre_class string COMMENT '预期分类', 
  counts int COMMENT '预期分类skc个数', 
  etl_time timestamp)
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_skc_tiny_class_count';
-----------------------------------------------   销售总览页面   end  -----------------------------------------------
-----------------------------------------------   补货调拨后整体情况查看页面   start  -----------------------------------------------
drop table if exists rsts.rst_store_mictiny_class_replenish_allot_comparison;
CREATE TABLE rsts.rst_store_mictiny_class_replenish_allot_comparison(
  store_code string COMMENT '门店编码', 
  city_long_code string COMMENT '城市长编码', 
  zone_long_code string COMMENT '区域长编码', 
  dq_long_code string COMMENT '大区长编码', 
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
  available_sell_well_brokensize_rate decimal(30,8) COMMENT '补调前畅销款断码率', 
  available_all_brokensize_rate decimal(30,8) COMMENT '补调前全款断码率', 
  after_sell_well_skc_count int COMMENT '补调后畅销款有库存SKC数', 
  after_all_skc_count int COMMENT '补调后全款有库存SKC数', 
  after_sell_well_stock_qty int COMMENT '补调后畅销款库存数量', 
  after_all_skc_stock_qty int COMMENT '补调后全款库存数量', 
  after_sell_well_brokensize_rate decimal(30,8) COMMENT '补调后畅销款断码率', 
  after_all_brokensize_rate decimal(30,8) COMMENT '补调后全款断码率', 
  order_num int COMMENT '排序字段', 
  etl_time timestamp COMMENT 'etl时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_store_mictiny_class_replenish_allot_comparison'
;

drop table if exists rsts.rst_store_tiny_class_replenish_allot_comparison;
CREATE TABLE rsts.rst_store_tiny_class_replenish_allot_comparison(
  store_code string COMMENT '门店编码', 
  city_long_code string COMMENT '城市长编码', 
  zone_long_code string COMMENT '区域长编码', 
  dq_long_code string COMMENT '大区长编码', 
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
  available_sell_well_brokensize_rate decimal(30,8) COMMENT '补调前畅销款断码率', 
  available_all_brokensize_rate decimal(30,8) COMMENT '补调前全款断码率', 
  after_sell_well_skc_count int COMMENT '补调后畅销款有库存SKC数', 
  after_all_skc_count int COMMENT '补调后全款有库存SKC数', 
  after_sell_well_stock_qty int COMMENT '补调后畅销款库存数量', 
  after_all_skc_stock_qty int COMMENT '补调后全款库存数量', 
  after_sell_well_brokensize_rate decimal(30,8) COMMENT '补调后畅销款断码率', 
  after_all_brokensize_rate decimal(30,8) COMMENT '补调后全款断码率', 
  order_num int COMMENT '排序字段',  
  etl_time timestamp COMMENT 'etl时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_store_tiny_class_replenish_allot_comparison'
;

drop table if exists rsts.rst_store_big_class_replenish_allot_comparison;
CREATE TABLE rsts.rst_store_big_class_replenish_allot_comparison(
  store_code string COMMENT '门店编码', 
  city_long_code string COMMENT '城市长编码', 
  zone_long_code string COMMENT '区域长编码', 
  dq_long_code string COMMENT '大区长编码', 
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
  available_sell_well_brokensize_rate decimal(30,8) COMMENT '补调前畅销款断码率', 
  available_all_brokensize_rate decimal(30,8) COMMENT '补调前全款断码率', 
  after_sell_well_skc_count int COMMENT '补调后畅销款有库存SKC数', 
  after_all_skc_count int COMMENT '补调后全款有库存SKC数', 
  after_sell_well_stock_qty int COMMENT '补调后畅销款库存数量', 
  after_all_skc_stock_qty int COMMENT '补调后全款库存数量', 
  after_sell_well_brokensize_rate decimal(30,8) COMMENT '补调后畅销款断码率', 
  after_all_brokensize_rate decimal(30,8) COMMENT '补调后全款断码率', 
  order_num int COMMENT '排序字段', 
  etl_time timestamp COMMENT 'etl时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_store_big_class_replenish_allot_comparison'
;

drop table if exists rsts.rst_store_replenish_allot_comparison;
CREATE TABLE rsts.rst_store_replenish_allot_comparison(
  store_code string COMMENT '门店编码', 
  city_long_code string COMMENT '城市长编码', 
  zone_long_code string COMMENT '区域长编码', 
  dq_long_code string COMMENT '大区长编码', 
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
  available_sell_well_brokensize_rate decimal(30,8) COMMENT '补调前畅销款断码率', 
  available_all_brokensize_rate decimal(30,8) COMMENT '补调前全款断码率', 
  after_sell_well_skc_count int COMMENT '补调后畅销款有库存SKC数', 
  after_all_skc_count int COMMENT '补调后全款有库存SKC数', 
  after_sell_well_stock_qty int COMMENT '补调后畅销款库存数量', 
  after_all_skc_stock_qty int COMMENT '补调后全款库存数量', 
  after_sell_well_brokensize_rate decimal(30,8) COMMENT '补调后畅销款断码率', 
  after_all_brokensize_rate decimal(30,8) COMMENT '补调后全款断码率', 
  order_num int COMMENT '排序字段', 
  etl_time timestamp COMMENT 'etl时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_store_replenish_allot_comparison'
;

-----------------------------------------------   补货调拨后整体情况查看页面   end  -------------------------------------------------
---------------------------------------------------   补货调拨页面   start  ----------------------------------------------------
drop table if exists rsts.rst_skc_detail_info; 
CREATE TABLE rsts.rst_skc_detail_info(
  product_code string COMMENT '产品编码', 
  product_name string COMMENT '产品名称', 
  color_code string COMMENT '颜色编码', 
  color_name string COMMENT '颜色名称', 
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  band string COMMENT '波段', 
  tiny_class string COMMENT '小类', 
  product_class string COMMENT '产品分类', 
  material string COMMENT '材料', 
  stock_qty string COMMENT '总仓库存', 
  stock_qty_int int COMMENT '总仓库存数量', 
  store_stock_qty string COMMENT '门店总库存', 
  store_stock_qty_int int COMMENT '门店总库存数量', 
  distributed_days int COMMENT '已售卖天数', 
  etl_time timestamp COMMENT 'etl时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_skc_detail_info';

drop table if exists rsts.rst_skc_city_brokensize_count;
CREATE TABLE rsts.rst_skc_city_brokensize_count(
  city_long_code string COMMENT '城市长编码', 
  product_code string COMMENT '产品编码', 
  product_name string COMMENT '产品名称', 
  color_code string COMMENT '颜色编码', 
  color_name string COMMENT '颜色名称', 
  before_rep_allot_skc_count int COMMENT '补调前商品数', 
  before_rep_allot_brokensize_count int COMMENT '补调前断码商品数', 
  after_rep_allot_skc_count int COMMENT '补调后商品数', 
  after_rep_allot_brokensize_count int COMMENT '补调后断码商品数', 
  etl_time timestamp COMMENT 'etl时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_skc_city_brokensize_count';

drop table if exists rsts.rst_skc_store_ra_model;
CREATE TABLE rsts.rst_skc_store_ra_model(
  product_code string COMMENT '货号', 
  product_name string COMMENT '货号名称', 
  color_code string COMMENT '颜色编码', 
  color_name string COMMENT '颜色编码', 
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  band string COMMENT '波段', 
  tiny_class string COMMENT '小类', 
  product_class string COMMENT '产品类别', 
  send_org_code string COMMENT '调出组织', 
  send_org_name string COMMENT '调出组织名', 
  send_org_long_code string COMMENT '调出组织长编码', 
  stock_qty string COMMENT '库存数量', 
  road_stock_qty string COMMENT '在途数量', 
  week_sales_qty string COMMENT '近一周销量', 
  week_sales_qty_int int COMMENT '近一周销量', 
  total_sales_qty int COMMENT '累计销量', 
  total_sales_qty_str string COMMENT '累计销量', 
  on_order_qty string COMMENT '在单数量', 
  target_inventory_qty string COMMENT '目标库存', 
  suggest_replenish_qty string COMMENT '建议补货量', 
  suggest_replenish_qty_int string COMMENT '建议补货数量', 
  suggest_allot_qty string COMMENT '建议调拨量', 
  suggest_allot_qty_int string COMMENT '建议调拨数量',
  suggest_return_qty string COMMENT '建议返仓数量',
  replenish_allot_qty string COMMENT '补调量', 
  replenish_allot_after_stock string COMMENT '补调后库存', 
  receive_org_code string COMMENT '调入组织', 
  receive_org_name string COMMENT '调入组织名', 
  receive_org_long_code string COMMENT '调入组织长编码', 
  city_code string COMMENT '城市', 
  city_long_code string COMMENT '城市长编码',  
  suggest_target string COMMENT '策略目的', 
  modify_reason string COMMENT '手工修改原因', 
  zone_id string COMMENT '大区id', 
  zone_name string COMMENT '大区名称',
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_skc_store_ra_model';
  
drop table if exists rsts.rst_skc_store_ra_model_export;
CREATE TABLE rsts.rst_skc_store_ra_model_export(
  product_code string COMMENT '货号', 
  product_name string COMMENT '货号名称', 
  color_code string COMMENT '颜色编码', 
  color_name string COMMENT '颜色编码', 
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  band string COMMENT '波段', 
  tiny_class string COMMENT '小类', 
  product_class string COMMENT '产品类别', 
  send_org_code string COMMENT '调出组织', 
  send_org_name string COMMENT '调出组织名', 
  send_org_long_code string COMMENT '调出组织长编码', 
  stock_qty string COMMENT '库存数量', 
  road_stock_qty string COMMENT '在途数量', 
  week_sales_qty string COMMENT '近一周销量', 
  week_sales_qty_int int COMMENT '近一周销量', 
  last2_seven_days_sales_qty string COMMENT '上上周销量',
  last_thirty_days_sales_qty string COMMENT '过去30天销量',
  total_sales_qty int COMMENT '累计销量', 
  total_sales_qty_str string COMMENT '累计销量', 
  on_order_qty string COMMENT '在单数量', 
  target_inventory_qty string COMMENT '目标库存', 
  suggest_replenish_qty string COMMENT '建议补货量', 
  suggest_replenish_qty_int string COMMENT '建议补货数量',   
  suggest_allot_qty string COMMENT '建议调拨量', 
  suggest_allot_qty_int string COMMENT '建议调拨数量',
  suggest_return_qty string COMMENT '建议返仓数量',  
  replenish_allot_qty string COMMENT '补调量', 
  replenish_allot_after_stock string COMMENT '补调后库存',
  move_type string comment '移动类型',                     -- 目前只考虑返仓 
  receive_org_code string COMMENT '调入组织', 
  receive_org_name string COMMENT '调入组织名', 
  receive_org_long_code string COMMENT '调入组织长编码', 
  city_code string COMMENT '城市', 
  city_long_code string COMMENT '城市长编码',  
  suggest_target string COMMENT '策略目的', 
  modify_reason string COMMENT '手工修改原因', 
  zone_id string COMMENT '大区id', 
  zone_name string COMMENT '大区名称',
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_skc_store_ra_model_export';
  
drop table if exists rsts.rst_skc_store_ra_model_export_temp;
CREATE TABLE rsts.rst_skc_store_ra_model_export_temp(
  product_code string COMMENT '货号', 
  product_name string COMMENT '货号名称', 
  color_code string COMMENT '颜色编码', 
  color_name string COMMENT '颜色编码', 
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  band string COMMENT '波段', 
  tiny_class string COMMENT '小类', 
  product_class string COMMENT '产品类别', 
  send_org_code string COMMENT '调出组织', 
  send_org_name string COMMENT '调出组织名', 
  send_org_long_code string COMMENT '调出组织长编码', 
  stock_qty string COMMENT '库存数量', 
  road_stock_qty string COMMENT '在途数量', 
  week_sales_qty string COMMENT '近一周销量', 
  week_sales_qty_int int COMMENT '近一周销量', 
  last2_seven_days_sales_qty string COMMENT '上上周销量',
  last_thirty_days_sales_qty string COMMENT '过去30天销量',
  total_sales_qty int COMMENT '累计销量', 
  total_sales_qty_str string COMMENT '累计销量', 
  on_order_qty string COMMENT '在单数量', 
  target_inventory_qty string COMMENT '目标库存', 
  suggest_replenish_qty string COMMENT '建议补货量', 
  suggest_replenish_qty_int string COMMENT '建议补货数量',   
  suggest_allot_qty string COMMENT '建议调拨量', 
  suggest_allot_qty_int string COMMENT '建议调拨数量',
  suggest_return_qty string COMMENT '建议返仓数量',  
  replenish_allot_qty string COMMENT '补调量', 
  replenish_allot_after_stock string COMMENT '补调后库存', 
  move_type string comment '移动类型',                     -- 目前只考虑返仓
  receive_org_code string COMMENT '调入组织', 
  receive_org_name string COMMENT '调入组织名', 
  receive_org_long_code string COMMENT '调入组织长编码', 
  city_code string COMMENT '城市', 
  city_long_code string COMMENT '城市长编码',  
  suggest_target string COMMENT '策略目的', 
  modify_reason string COMMENT '手工修改原因', 
  zone_id string COMMENT '大区id', 
  zone_name string COMMENT '大区名称',
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_skc_store_ra_model_export_temp';
 
drop table if exists rsts.rst_skc_store_ra_model_temp;
CREATE TABLE rsts.rst_skc_store_ra_model_temp(
  product_code string COMMENT '货号', 
  product_name string COMMENT '货号名称', 
  color_code string COMMENT '颜色编码', 
  color_name string COMMENT '颜色编码', 
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  band string COMMENT '波段', 
  tiny_class string COMMENT '小类', 
  product_class string COMMENT '产品类别', 
  send_org_code string COMMENT '调出组织', 
  send_org_name string COMMENT '调出组织名', 
  send_org_long_code string COMMENT '调出组织长编码', 
  stock_qty string COMMENT '库存数量', 
  road_stock_qty string COMMENT '在途数量', 
  week_sales_qty string COMMENT '近一周销量', 
  week_sales_qty_int int COMMENT '近一周销量', 
  total_sales_qty int COMMENT '累计销量', 
  total_sales_qty_str string COMMENT '累计销量', 
  on_order_qty string COMMENT '在单数量', 
  target_inventory_qty string COMMENT '目标库存', 
  suggest_replenish_qty string COMMENT '建议补货量', 
  suggest_replenish_qty_int string COMMENT '建议补货数量', 
  suggest_allot_qty string COMMENT '建议调拨量', 
  suggest_allot_qty_int string COMMENT '建议调拨数量', 
  replenish_allot_qty string COMMENT '补调量', 
  replenish_allot_after_stock string COMMENT '补调后库存', 
  receive_org_code string COMMENT '调入组织', 
  receive_org_name string COMMENT '调入组织名', 
  receive_org_long_code string COMMENT '调入组织长编码', 
  city_code string COMMENT '城市', 
  city_long_code string COMMENT '城市长编码',  
  suggest_target string COMMENT '策略目的', 
  modify_reason string COMMENT '手工修改原因', 
  zone_id string COMMENT '大区id', 
  zone_name string COMMENT '大区名称',
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_skc_store_ra_model_temp';

drop table if exists rsts.rst_city_brokensize_count;
CREATE TABLE rsts.rst_city_brokensize_count(
  city_long_code string COMMENT '城市长编码', 
  before_rep_allot_skc_count int COMMENT '补调前商品数', 
  before_rep_allot_brokensize_count int COMMENT '补调前断码商品数', 
  after_rep_allot_skc_count int COMMENT '补调后商品数', 
  after_rep_allot_brokensize_count int COMMENT '补调后断码商品数', 
  etl_time timestamp COMMENT 'etl时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_city_brokensize_count';

DROP TABLE IF EXISTS rsts.rst_skc_store_ra_real;
CREATE TABLE rsts.rst_skc_store_ra_real(
  product_code string COMMENT '货号', 
  product_name string COMMENT '货号名称', 
  color_code string COMMENT '颜色编码', 
  color_name string COMMENT '颜色编码', 
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  band string COMMENT '波段', 
  tiny_class string COMMENT '小类', 
  product_class string COMMENT '产品类别', 
  send_org_code string COMMENT '调出组织', 
  send_org_name string COMMENT '调出组织名', 
  send_org_long_code string COMMENT '调出组织长编码', 
  stock_qty string COMMENT '库存数量', 
  road_stock_qty string COMMENT '在途数量', 
  week_sales_qty string COMMENT '近一周销量', 
  total_sales_qty int COMMENT '累计销量', 
  on_order_qty string COMMENT '在单数量', 
  target_inventory_qty string COMMENT '目标库存', 
  real_replenish_qty string COMMENT '建议补货量', 
  real_replenish_qty_int string COMMENT '建议补货数量', 
  real_allot_qty string COMMENT '建议调拨量', 
  real_allot_qty_int string COMMENT '建议调拨数量', 
  replenish_allot_qty string COMMENT '补调量', 
  replenish_allot_after_stock string COMMENT '补调后库存', 
  receive_org_code string COMMENT '调入组织', 
  receive_org_name string COMMENT '调入组织名', 
  receive_org_long_code string COMMENT '调入组织长编码', 
  city_code string COMMENT '城市', 
  city_longcode string COMMENT '城市长编码', 
  suggest_target string COMMENT '策略目的', 
  modify_reason string COMMENT '手工修改原因', 
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_skc_store_ra_real';
  
  --补调量
drop table if exists rsts.rst_skc_city_replenish_allot_info;
CREATE TABLE rsts.rst_skc_city_replenish_allot_info(
  product_code string COMMENT '货号', 
  product_name string COMMENT '货号名称', 
  color_code string COMMENT '颜色编码', 
  color_name string COMMENT '颜色名称', 
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  band string COMMENT '波段', 
  tiny_class string COMMENT '小类', 
  product_class string COMMENT '产品类别', 
  send_city_long_code string COMMENT '调出城市长编码', 
  send_zone_id string COMMENT '调出区域用户id',
  replenish_allot_qty int COMMENT '补调量',   
  receive_city_long_code string COMMENT '调入城市长编码', 
  receive_zone_id string COMMENT '调入区域用户id',
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_skc_city_replenish_allot_info';
  
--包裹数
drop table if exists rsts.rst_skc_org_replenish_allot_package_info;
CREATE TABLE rsts.rst_skc_org_replenish_allot_package_info(
  product_code string COMMENT '货号', 
  product_name string COMMENT '货号名称', 
  color_code string COMMENT '颜色编码', 
  color_name string COMMENT '颜色名称', 
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  band string COMMENT '波段', 
  tiny_class string COMMENT '小类', 
  product_class string COMMENT '产品类别',
  send_org_code  string COMMENT '调出组织编码',
  send_city_long_code string COMMENT '调出城市长编码', 
  send_zone_id string COMMENT '调出区域用户id',
  receive_org_code  string COMMENT '调入组织编码',   
  receive_city_long_code string COMMENT '调入城市长编码', 
  receive_zone_id string COMMENT '调入区域用户id',
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_skc_org_replenish_allot_package_info';

--  skc城市分类波段断码断码表  替换 rst_city_brokensize_count 表的
 drop table if exists rsts.rst_skc_city_classify_band_day_brokensize_count;
CREATE TABLE rsts.rst_skc_city_classify_band_day_brokensize_count(
  product_code string COMMENT '货号', 
  product_name string COMMENT '货号名称', 
  color_code string COMMENT '颜色编码', 
  color_name string COMMENT '颜色名称', 
  year int COMMENT '年份', 
  quarter string COMMENT '季节', 
  band string COMMENT '波段', 
  tiny_class string COMMENT '小类', 
  product_class string COMMENT '产品类别', 
  city_long_code string COMMENT '城市长编码', 
  zone_id string COMMENT '区域用户id',
  before_rep_allot_skc_count int COMMENT '补调前商品数', 
  before_rep_allot_brokensize_count int COMMENT '补调前断码商品数', 
  after_rep_allot_skc_count int COMMENT '补调后商品数', 
  after_rep_allot_brokensize_count int COMMENT '补调后断码商品数', 
  etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_skc_city_classify_band_day_brokensize_count';

---------------------------------------------------   补货调拨页面   end  ----------------------------------------------------

--------------------------------------------- **** 商品总览  start ****------------------------------------------------
drop table if exists rsts.rst_skc_classify_day_analysis;
create table rsts.rst_skc_classify_day_analysis(
  year int comment '年份',
  quarter string comment '季节',
  prod_class string comment '产品分类',
  stage_lifetime string comment '生命周期阶段',
  -- day_date string comment '统计日期',
  total_sales_qty int comment '累计销量',
  sales_rate decimal(30,8) comment '销量占比',
  etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location '/dw_pb/rsts.db/rst_skc_classify_day_analysis';


drop table if exists rsts.rst_skc_classify_band_day_analysis;
create table rsts.rst_skc_classify_band_day_analysis(
  prod_class string comment '产品分类',
  stage_lifetime string comment '生命周期阶段',
  year int comment '年份',
  quarter string comment '季节',
  band string comment '波段',
  sales_out_rate decimal(30,8) comment '售罄率',
  avg_sales_qty decimal(30,8) comment '平均销量',
  selling_rate decimal(30,8) comment '售卖速度',
  brokensize_rate decimal(30,8) comment '断码率',
  -- day_date string comment '日期',
  etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location '/dw_pb/rsts.db/rst_skc_classify_band_day_analysis';

drop table if exists rsts.rst_skc_city_day_analysis_toc;
create table rsts.rst_skc_city_day_analysis_toc(
  product_code string comment '货号',
  product_name string comment '产品名称',
  color_code string comment '颜色',
  color_name string comment '颜色名称',
  year int comment '年份',
  quarter string comment '季节',
  band string comment '波段',
  tiny_class string comment '小类',
  mictiny_class string comment '细分品类',
  prod_class string comment '产品类别',
  stage_lifetime string comment '生命周期阶段',
  no_sales_no_available_stock_store_count int comment '无销量无库存(含在途)门店数',
  has_sales_no_available_stock_store_count int comment '有销量无库存(含在途)门店数',
  no_sales_available_fullsize_store_count int comment '无销量齐码(含在途)门店数',
  no_sales_available_brokensize_store_count int comment '无销量断码(含在途)门店数',
  has_sales_available_brokensize_store_count int comment '有销量断码(含在途)门店数',
  has_sales_available_fullsize_store_count int comment '有销量齐码(含在途)店数',
  has_sales_fullsize_store_count int comment '有销量齐码(不含在途)门店数',
  is_toc string comment '是否toc',
  city_code string comment '城市编码',
  city_long_code string comment '城市长编码',
  region_code string comment '大区编码',
  region_long_code string comment '大区长编码',
  -- day_date string comment '日期',
  etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location '/dw_pb/rsts.db/rst_skc_city_day_analysis_toc';


drop table if exists rsts.rst_skc_add_order;
 create table rsts.rst_skc_add_order(
  product_code string comment '货号',
  product_name string comment '货号名字',
  color_code string comment '颜色编码',
  color_name string comment '颜色名称',
  add_order_date string comment '追单日期',
  expected_arrival_date string comment '预计到货日期',
  add_order_qty int comment '追单数量',
  actual_arrival_date string comment '实际到仓日期',
  receive_qty int comment '已到货数量',
  etl_time timestamp)
stored as parquet
location '/dw_pb/rsts.db/rst_skc_add_order';

drop table if exists rsts.rst_skc_product_tag_toc;
create table rsts.rst_skc_product_tag_toc(
product_code string    comment '货号',
product_name string    comment '产品名称',
color_code string    comment '颜色编码',
color_name string    comment '颜色名称',
year int    comment '年份',
quarter string    comment '季节',
band string    comment '波段',
tiny_class string    comment '小类',
mictiny_class string    comment '细分品类',
is_toc string comment '是否toc',
city_code string    comment '城市编码',
city_long_code string    comment '城市长编码',
region_code string    comment '大区编码',
region_long_code string    comment '大区长编码',
brokensize_rate_numerator int    comment '断码率分子',
brokensize_rate_denominator int    comment '断码率分母',
delivery_sales_rate_numerator int    comment '发销率分子',
delivery_sales_rate_denominator int    comment '发销率分母',
sales_brokensize_rate_numerator int    comment '销售断码率分子',
sales_brokensize_rate_denominator int    comment '销售断码率分母',
store_turnover_weeks_numerator int    comment '周转周数',
store_turnover_weeks_denominator int    comment '周转周数',
no_sales_store_rate_numerator int    comment '无销售门店比例分子',
no_sales_store_rate_denominator int    comment '无销售门店比例分母',
distribute_rate_numerator int    comment '铺货率分子',
distribute_rate_denominator int    comment '铺货率分母',
-- day_date string    comment '日期',
etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location '/dw_pb/rsts.db/rst_skc_product_tag_toc';

drop table if exists rsts.rst_skc_city_day_stock_analysis_toc;
create table rsts.rst_skc_city_day_stock_analysis_toc(
  product_code string comment '货号',
  product_name string comment '产品名称',
  color_code string comment '颜色',
  color_name string comment '颜色名称',
  year int comment '年份',
  quarter string comment '季节',
  total_distributed_store_count int comment '总铺门店数',
  avg_store_stock_qty_numerator int comment '店均库存分子',
  avg_store_stock_qty_denominator int comment '店均库存分母',
  store_stock_qty int comment '店铺库存',
  is_toc string comment '是否toc',
  city_code string comment '城市编码',
  city_long_code string comment '城市长编码',
  region_code string comment '大区编码',
  region_long_code string comment '大区长编码',
  -- day_date string comment '日期',
  etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location '/dw_pb/rsts.db/rst_skc_city_day_stock_analysis_toc';

drop table if exists rsts.rst_skc_city_summary;
create table rsts.rst_skc_city_summary(
 product_code string comment '产品code',
 product_name string comment '产品名称',
 color_code string comment '颜色',
 color_name string comment '颜色名称',
 year int comment '年份',
 quarter string comment '季节',
 tag_price int comment '吊牌价',
 sales_weeks int comment '已售卖周数',
 total_sales_qty int comment '累计销售',
 five_weeks_sales_trend string comment '五周销售趋势',
 turnover_weeks decimal(30,8) comment '周转周数',
 warehouse_total_stock_qty int comment '总仓库存',
 cb11_stock_qty int comment 'cb11库存',
 total_otb_order_qty int comment '总订货量',
 total_otb_order_rank int comment '订货总排名',
 total_otb_order_in_tinyclass_rank int comment '本小类订货排名',
 prod_delivery_sales_rate decimal(30,8) comment '本款发销率',
 prod_discount decimal(30,8) comment '本款折扣',
 prod_fullprice_rate decimal(30,8) comment '本款正价率',
 tinyclass_delivery_sales_rate decimal(30,8) comment '品类发销率',
 tinyclass_discount decimal(30,8) comment '品类折扣',
 tinyclass_fullprice_rate decimal(30,8) comment '品类正价率',
 -- day_date string comment '日期',
 etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location '/dw_pb/rsts.db/rst_skc_city_summary';

drop table if exists rsts.rst_skc_distri_pre;
create table rsts.rst_skc_distri_pre(
  product_code string comment '货号',
  product_name string comment '产品名称',
  color_code string comment '颜色',
  color_name string comment '颜色名称',
  two_week_sales_celling_qty int comment '已铺未来两周预测需求上限',
  two_week_sales_floor_qty int comment '已铺未来两周预测需求下限',
  four_week_sales_celling_qty int comment '已铺未来两周预测需求上限',
  four_week_sales_floor_qty int comment '已铺未来两周预测需求下限',
  residue_sales_celling_qty int comment '已铺剩余生命周期预测需求上限',
  residue_sales_floor_qty int comment '已铺剩余生命周期预测需求下限',
  two_week_all_distri_sales_celling_qty int comment '全铺未来两周预测需求上限',
  two_week_all_distri_sales_floor_qty int comment '全铺未来两周预测需求下限',
  four_week_all_distri_sales_celling_qty int comment '全铺未来四周预测需求上限',
  four_week_all_distri_sales_floor_qty int comment '全铺未来四周预测需求下限',
  residue_all_distri_sales_celling_qty int comment '全铺剩余生命周期预测需求上限',
  residue_all_distri_sales_floor_qty int comment '全铺剩余生命周期预测需求下限',
  last_week_sales_qty int comment '上周销售',
  total_sales_qty int comment '累计销售',
  org_code string comment '组织编码',
  org_long_code string comment '组织长编码',
  -- day_date string comment '日期',
  etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location '/dw_pb/rsts.db/rst_skc_distri_pre';

drop table if exists rsts.rst_skc_country_day_match_info;
  create table rsts.rst_skc_country_day_match_info(
  main_product_code string comment '主款product_code',
  main_product_name string comment '主款product_name',
  main_color_code string comment '主款color_code',
  main_color_name string comment '主款color_name',
  main_tiny_class string comment '主款小类',
  main_tag_price decimal(30,8) comment '主款吊牌价',
  main_year int comment '主款年份',
  main_quarter string comment '主款季节',
  match_product_code string comment '搭配款product_code',
  match_product_name string comment '搭配款product_name',
  match_color_code string comment '搭配款color_code',
  match_color_name string comment '搭配款color_name',
  match_tiny_class string comment '搭配款小类',
  match_tag_price decimal(30,8) comment '搭配款吊牌价',
  match_year int comment '搭配款年份',
  match_quarter string comment '搭配款季节',
  numerator int comment '搭配款出现次数',
  denominator int comment '所有搭配款出现次数',
  -- day_date string comment '日期',
  etl_time timestamp comment 'etl 时间')
partitioned by (day_date string comment '日期')
stored as parquet
location '/dw_pb/rsts.db/rst_skc_country_day_match_info';

drop table if exists rsts.rst_skc_city_day_analysis_for_order_toc;
create table rsts.rst_skc_city_day_analysis_for_order_toc(
product_code string    comment '货号',
product_name string    comment '商品名称',
color_code string    comment '颜色编码',
color_name string    comment '颜色名称',
brokensize_numerator int    comment '断码率分子',
brokensize_denominator int    comment '断码率分母',
last_week_sales_qty int    comment '上周销量',
week_sales_qty int    comment '本周(截止到当前)销量',
total_sales_qty int    comment '累计销量',
-- day_date string    comment '日期',
is_toc string    comment '是否toc',
city_code string    comment '城市编码',
city_long_code string    comment '城市长编码',
etl_time timestamp)
partitioned by (day_date string comment '日期')
stored as parquet
location '/dw_pb/rsts.db/rst_skc_city_day_analysis_for_order_toc';
--------------------------------------------- **** 商品总览  end ****------------------------------------------------

--------------------------------------------- **** 单品策略分析页面  start ****--------------------------------------
drop table if exists rsts.rst_skc_city_week_sales_change_comp;    
create table rsts.rst_skc_city_week_sales_change_comp(    
product_code string    comment '货号',
product_name string    comment '货号名称',
color_code string    comment '颜色编码',
color_name string    comment '颜色名称',
year int    comment '年份',
quarter string    comment '季节',
region_code string    comment '大区',
region_long_code string    comment '大区长编码',
zone_long_code string    comment '区域经理长编码',
zone_name string    comment '区域经理名称',
city_code string    comment '城市',
city_long_code string    comment '城市长编码',
ai_sales_grow int    comment '模型销量增长',
peacebird_sales_grow int    comment '实际销量增长',
ai_sales_amt_grow decimal(30,8)    comment '模型销售额增长',
peacebird_sales_amt_grow decimal(30,8)    comment '实际销售额增长',
ai_sales_loss int    comment '模型销量损失',
peacebird_sales_loss int    comment '实际销量损失',
ai_sales_amt_loss decimal(30,8)    comment '模型销售额损失',
peacebird_sales_amt_loss decimal(30,8)    comment '实际销售额损失',
peacebird_distribute_rate_numerator int    comment '实际铺货率分子',
peacebird_distribute_rate_denominator int    comment '实际铺货率分母',
peacebird_brokensize_numerator int    comment '实际断码率分子',
peacebird_brokensize_denominator int    comment '实际断码率分母',
ai_distribute_rate_numerator int    comment '模型铺货率分子',
ai_distribute_rate_denominator int    comment '模型铺货率分母',
ai_brokensize_numerator int    comment '模型断码率分子',
ai_brokensize_denominator int    comment '模型断码率分母',
--week_date string    comment '周日期',
etl_time timestamp    comment 'etl时间'
)
PARTITIONED BY (week_date string COMMENT '日期')
STORED AS parquet
location '/dw_pb/rsts.db/rst_skc_city_week_sales_change_comp';

--------------------------------------------- **** 单品策略分析页面  end ****----------------------------------------

--------------------------------------------- **** 对接drp系统的表  start ****----------------------------------------
DROP TABLE IF EXISTS rsts.rst_sku_replenish_allot_day_drp;
CREATE TABLE rsts.rst_sku_replenish_allot_day_drp(
    send_org_code string COMMENT '调出组织编码', 
    receive_org_code string COMMENT '调入组织编码',
    refno string COMMENT '调拨编号',   -- 由 send_org_code+receive_org_code+billdate 
    sku_code string COMMENT '条码code',     
    qty int COMMENT '发送数量',  
    etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (billdate string COMMENT '补调单日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_sku_replenish_allot_day_drp';
  
 DROP TABLE IF EXISTS rsts.rst_sku_replenish_day_drp;
CREATE TABLE rsts.rst_sku_replenish_day_drp(
    send_org_code string COMMENT '调出组织编码', 
    receive_org_code string COMMENT '调入组织编码',
    refno string COMMENT '调拨编号',   -- 由 send_org_code+receive_org_code+billdate 
    sku_code string COMMENT '条码code',
    qty int COMMENT '发送数量', 
    receive_org_limt int COMMENT '调入组织数量限制',
    package_qty_sum int COMMENT '每个包裹sku的累计数量',    
    etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (billdate string COMMENT '补调单日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_sku_replenish_day_drp';
 
-- 在模型生成补调表的时候出现错误的情况下，才会用到这张表
 DROP TABLE IF EXISTS rsts.rst_sku_replenish_day_drp_toc;  
CREATE TABLE rsts.rst_sku_replenish_day_drp_toc(
    send_org_code string COMMENT '调出组织编码', 
    receive_org_code string COMMENT '调入组织编码',
    refno string COMMENT '调拨编号',   -- 由 send_org_code+receive_org_code+billdate 
    sku_code string COMMENT '条码code', 
    qty int COMMENT '发送数量', 
    receive_org_limt int COMMENT '调入组织数量限制',
    package_qty_sum int COMMENT '每个包裹sku的累计数量',
    etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (billdate string COMMENT '补调单日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_sku_replenish_day_drp_toc';
  
 DROP TABLE IF EXISTS rsts.rst_sku_allot_day_drp;
CREATE TABLE rsts.rst_sku_allot_day_drp(
    send_org_code string COMMENT '调出组织编码', 
    receive_org_code string COMMENT '调入组织编码',
    refno string COMMENT '调拨编号',   -- 由 send_org_code+receive_org_code+billdate 
    sku_code string COMMENT '条码code',     
    qty int COMMENT '发送数量',  
    receive_org_limt int COMMENT '调入组织数量限制',
    package_qty_sum int COMMENT '每个包裹sku的累计数量',
    etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (billdate string COMMENT '补调单日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_sku_allot_day_drp';

DROP TABLE IF EXISTS rsts.rst_sku_sales_slip_day_drp;
CREATE TABLE rsts.rst_sku_sales_slip_day_drp(
    send_org_code string COMMENT '调出组织编码', 
    receive_org_code string COMMENT '调入组织编码',
    refno string COMMENT '调拨编号',   -- 由 send_org_code+receive_org_code+billdate 
    sku_code string COMMENT '条码code',     
    qty int COMMENT '发送数量',  
    receive_org_limt int COMMENT '调入组织数量限制',
    package_qty_sum int COMMENT '每个包裹sku的累计数量',
    etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (billdate string COMMENT '补调单日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_sku_sales_slip_day_drp';


  
 DROP TABLE IF EXISTS rsts.rst_sku_store_target_inventory_day_drp;
CREATE TABLE rsts.rst_sku_store_target_inventory_day_drp(
    c_store_id string COMMENT '门店id', 
    m_product_id string COMMENT '款号id',
    skuid string COMMENT '条码id',
    target string COMMENT '目标库存', 
    onhand string COMMENT '在店数量',
    onway string COMMENT '在店数量', 
    active string COMMENT '状态',    
    kind string COMMENT '性质',  
    ad_client_id string COMMENT 'ad_client_id',  
    ad_org_id string COMMENT 'ad_org_id',
    isactive string COMMENT '是否可用',
    m_attributesetinstance string COMMENT '颜色尺寸',    
    adjust_reason string COMMENT '调整原因', 
    etl_time timestamp COMMENT 'etl 时间')
PARTITIONED BY (day_date string COMMENT '记录日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_sku_store_target_inventory_day_drp';

-- 加盟商资金预警表
DROP TABLE IF EXISTS rsts.rst_customer_store_alarm_bak_drp;
CREATE TABLE rsts.rst_customer_store_alarm_bak_drp(
    c_customer_id string COMMENT '经销商id', 
    c_store_id string COMMENT '门店id',
    amount_can decimal(38,10) COMMENT '可提货金额', 
    amount_bill decimal(38,10) COMMENT '待下单金额',     
    amount_pass decimal(38,10) COMMENT '及格线',  
    is_bill string COMMENT '下单情况',
    etl_time string COMMENT 'etl 时间')
PARTITIONED BY (billdate string COMMENT '日期')
STORED AS parquet
LOCATION
  '/dw_pb/rsts.db/rst_customer_store_alarm_bak_drp';

--------------------------------------------- **** 对接drp系统的表  end ****----------------------------------------


--------------------------------------------- **** 维度表  start ****------------------------------------------------
drop table if exists rsts.rst_store;
create table rsts.rst_store(
  store_id string comment '门店id',
  store_code string comment '门店编码',
  store_type string comment '门店类型',
  store_kind string comment '类型',
  store_name string comment '店铺名称',
  store_level string comment '店铺级别',
  biz_district string comment '商圈',
  selling_area string comment '营业面积',
  warehouse_area string comment '仓库面积',
  opening_time string comment '开业时间',
  close_time string comment '停业时间',
  status string comment '状态',
  province string comment '省份',
  city string comment '城市',
  city_level string comment '城市等级',
  area string comment '地区',
  clerk_count int comment '店员人数',
  address string comment '详细地址',
  city_code string comment '城市编码',
  city_long_code string comment '城市长编码',
  dq_code string comment '大区编码',
  dq_long_code string comment '大区长编码',
  dq_name string comment '大区名称',
  nanz_code string comment '男装编码',
  nanz_long_code string comment '男装编码',
  zone_id string comment '区域id',
  zone_name string comment '区域名称',
  lat string comment '纬度',
  lng string comment '经度',
  is_store string comment '是否门店',
  is_toc string comment '是否toc',
  etl_time timestamp comment 'etl时间')
stored as parquet
location '/dw_pb/rsts.db/rst_store';

drop table if exists rsts.rst_product;
create table rsts.rst_product(
  product_id string comment '货号',
  product_code string comment '产品编码',
  product_name string comment '名称',
  color_id string comment '颜色 id',
  color_code string comment '颜色编码',
  color_name string comment '颜色名称',
  size_id string comment '尺码 id',
  size_code string comment '尺码 code',
  size_name string comment '尺码名称',
  year int comment '年份',
  quarter string comment '季节',
  big_class string comment '大类',
  mid_class string comment '中类',
  tiny_class string comment '小类',
  mictiny_class string comment '细分小类',
  brand string comment '品牌',
  band string comment '波段',
  cost_price decimal(38,10) comment '成本价',
  tag_price decimal(38,10) comment '吊牌价',
  gender string comment '性别',
  put_on_date string comment '上架日期',
  pull_off_date string comment '下架日期',
  etl_time timestamp)
stored as parquet
location '/dw_pb/rsts.db/rst_product';

drop table if exists rsts.rst_stock_org;
create table rsts.rst_stock_org(
org_name string comment '组织名称',
org_code string comment '组织编码',
org_id string comment '组织id',
org_longname string comment '组织长名称',
org_longcode string comment '组织长编码',
parent_code string comment '父组织编码',
parent_id string comment '父组织 id',
org_type string comment '组织类型',
org_typecode string comment '组织类型编码',
status string comment '状态',
remark string comment '备注',
etl_time timestamp comment 'etl时间')
stored as parquet
location '/dw_pb/rsts.db/rst_stock_org';

--------------------------------------------- **** 维度表  end ****------------------------------------------------
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

------------------------------------------------- **** 预测评估  start **** ------------------------------------------
drop table if exists rsts.rst_skc_brokensize_has_stock_store_count_assess; -- 普通表 textfile
CREATE TABLE rsts.rst_skc_brokensize_has_stock_store_count_assess(
  product_id string COMMENT '产品id',
  product_code string COMMENT '产品编码',
  color_id string COMMENT '颜色id',
  color_code string COMMENT '颜色编码',
  brokensize_rate decimal(30,8) COMMENT '断码率',
  has_stock_store_count int COMMENT '有库存门店数',
  day_date string comment '日期',
  etl_time string COMMENT 'etl时间')
LOCATION
  '/dw_pb/rsts.db/rst_skc_brokensize_has_stock_store_count_assess';

drop table if exists rsts.rst_skc_four_week_sales_assess; -- 普通表 textfile
CREATE TABLE rsts.rst_skc_four_week_sales_assess(
  product_id string COMMENT '产品id',
  product_code string COMMENT '产品编码',
  color_id string COMMENT '颜色id',
  color_code string COMMENT '颜色编码',
  four_week_sales_qty_pre decimal(30,8) COMMENT '四周销量预测',
  four_week_sales_qty_act int COMMENT '四周实际销量',
  four_week_sales_qty_comp_store int comment '可比门店四周销量',
  accuracy decimal(30,8) COMMENT '准确率：1 - abs(销量预测 - 可比门店销量)/可比门店销量',
  distributed_days int COMMENT '已售卖天数',
  avg_brokensize_rate  decimal(30,8) COMMENT '四周平均断码率',
  assess_date string comment '评估日期：传入日期-28天；评估日期+1天=销量预测的决策日期',
  etl_time string COMMENT 'etl时间')
LOCATION
  '/dw_pb/rsts.db/rst_skc_four_week_sales_assess';

  
------------------------------------------------- **** 预测评估  start **** ------------------------------------------
