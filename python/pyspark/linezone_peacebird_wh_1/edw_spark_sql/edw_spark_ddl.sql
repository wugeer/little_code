drop table if exists edw.dim_store_skc_trans_status;
CREATE EXTERNAL TABLE edw.dim_store_skc_trans_status (
  store_id STRING comment '门店id',
  product_id string comment '产品id',
  color_id string comment '颜色id',
  not_in STRING comment '不进',
  not_out STRING comment '不出',
  --date_dec STRING comment '决策日期',
  etl_time string comment 'etl时间'
)
partitioned by (date_dec string comment '决策日期')
stored as parquet
LOCATION '/dw_pb/edw.db/dim_store_skc_trans_status';

-- st参与经销商相关信息
drop table if exists edw.fct_customer_store_toc;
create external table edw.fct_customer_store_toc(
customer_id string comment '经销商id',
store_id string comment '门店id',
discount decimal (30, 8) comment 'TOC折扣',
fund_pass decimal (30, 8) comment '资金及格线',
feecantake decimal (30, 8) comment '当前可提货金额',
day_date string comment '日期',
etl_time string comment 'etl时间'
)
location '/dw_pb/edw.db/fct_customer_store_toc';

-- dim_store  门店表 修改
drop table if exists edw.dim_store;
create external table edw.dim_store (
store_id string comment '门店id',
store_code string comment '门店编码',
store_long_code string comment '门店长编码',
store_type string comment '门店类型',
price_type string comment '价格类型',
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
city_id string comment '城市id',
city_long_code string comment '城市长编码',
dq_code string comment '大区编码',
dq_id string comment '大区id',
dq_long_code string comment '大区长编码',
nanz_code string comment '男装编码',
nanz_id string comment '男装id',
nanz_long_code string comment '男装长编码',
zone_id string comment '区域用户id',
zone_name string comment '区域用户名称',
lat string comment '纬度',
lng string comment '经度',
is_store string comment '是否门店',
is_toc string comment '是否toc门店',
customer_id string comment '所属经销商', -- 新增 2019年1月5日15:12:51
is_gap string comment '是否跨级',       -- 新增 2019年1月5日15:12:51
etl_time timestamp comment 'etl时间')
stored as parquet
location '/dw_pb/edw.db/dim_store';

# 目标门店表 排除掉加盟门店后 门店数据  和 dim_store 表在一个存储过程中
drop table if exists edw.dim_target_store;
create external table edw.dim_target_store (
store_id string comment '门店id',
store_code string comment '门店编码',
store_long_code string comment '门店长编码',
store_type string comment '门店类型',
price_type string comment '价格类型',
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
city_id string comment '城市id',
city_long_code string comment '城市长编码',
dq_code string comment '大区编码',
dq_id string comment '大区id',
dq_long_code string comment '大区长编码',
nanz_code string comment '男装编码',
nanz_id string comment '男装id',
nanz_long_code string comment '男装长编码',
zone_id string comment '区域用户id',
zone_name string comment '区域用户名称',
lat string comment '纬度',
lng string comment '经度',
is_store string comment '是否门店',
is_toc string comment '是否toc门店',
etl_time timestamp comment 'etl时间')
stored as parquet
location '/dw_pb/edw.db/dim_target_store';

--每天补货门店表    小表 采用 普通格式 不分区
drop table if exists edw.mid_store_move_period;
create EXTERNAL table edw.mid_store_move_period(
day_date string comment '日期',
store_id string comment '补货门店id',
etl_time string comment 'etl时间'
)
location '/dw_pb/edw.db/mid_store_move_period';


drop table if exists edw.dim_area;
create external table edw.dim_area(
area string,
region string,
user_id string,
user string)
stored as parquet
location '/dw_pb/edw.db/dim_area';

-- dim_city_location  城市信息表
drop table if exists edw.dim_city_location;
create external table edw.dim_city_location (
areaname string comment '城市',
parent_city  string comment '上级城市',
shortname  string comment '省份',
areacode string comment '区域编码',
zipcode string comment '压缩编码',
pinyin string comment '拼音',
cnty string comment '国家',
lat  string comment '纬度',
lng  string comment '经度',
city_level string comment '城市等级',
city_position string comment '位置',
sort string comment '排序',
etl_time timestamp comment 'etl时间')
stored as parquet
location '/dw_pb/edw.db/dim_city_location';

drop table if exists edw.dim_comp_store;
create external table edw.dim_comp_store(
comp_store_id string comment '可比门店id',
etl_time timestamp comment 'etl时间')
partitioned by (day_date string comment '日期')
stored as parquet
location '/dw_pb/edw.db/dim_comp_store';

drop table if exists edw.dim_date;
create external table edw.dim_date(
date_date string,
date_string string,
year_id int,
mon_id int,
day_id int,
week_id int,
weekday_id int,
quarter_id int )
stored as parquet
location '/dw_pb/edw.db/dim_date';

-- dim_movetype  移动类型维表
drop table if exists edw.dim_movetype;
create external table edw.dim_movetype(
movetype_id string comment '移动类型id',
is_active string comment '是否启用',
movetype string comment '移动类型',
description string comment '描述',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/dim_movetype';

-- dim_org_week_day_sales_weight 组织销售权重表 
drop table if exists edw.dim_org_week_day_sales_weight;
create external table edw.dim_org_week_day_sales_weight(
org_id string comment '组织',
org_code string comment '组织编码',
org_long_code string comment '组织长编码',
org_type string comment '组织类型',
week_day int comment '周几',
weight decimal (30, 8) comment '销售权重',
ref_year_id int comment '参考年份',
etl_time timestamp )
--stored as parquet
location '/dw_pb/edw.db/dim_org_week_day_sales_weight';

-- dim_product 产品表
drop table if exists edw.dim_product;
create external table edw.dim_product(
product_id string comment '货号',
product_name string comment '名称',
product_code string comment '产品编码',
year_id int comment '年份',
quarter_id string comment '季节',
big_class string comment '大类',
mid_class string comment '中类',
tiny_class string comment '小类',
mictiny_class string comment '细分小类',
brand string comment '品牌',
band string comment '波段',
cost_price decimal (38, 10) comment '成本价',
tag_price decimal (38, 10) comment '吊牌价',
gender string comment '性别',
put_on_date string comment '上架日期',
pull_off_date string comment '下架日期',
size_group_id int comment '尺码组id',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/dim_product';

-- dim_product_skc  产品skc表
drop table if exists edw.dim_product_skc;
create external table edw.dim_product_skc(
product_id string comment '产品id',
product_code string comment '产品code',
product_name string comment '产品名称',
color_id string comment '颜色 id',
color_code string comment '颜色编码',
color_type string comment '颜色类型',
color_name string comment '颜色名称',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/dim_product_skc';

-- dim_product_sku  产品sku表
drop table if exists edw.dim_product_sku;
create external table edw.dim_product_sku(
sku_id string comment 'sku_id',
sku_code string comment 'sku_code',
product_id string comment '产品id',
product_code string comment '产品code',
product_name string comment '产品名称',
color_id string comment '颜色 id',
color_code string comment '颜色编码',
color_name string comment '颜色名称',
size_id string comment '尺码 id',
size_code string comment '尺码 code',
size_name string comment '尺码名称',
status string comment '尺码状态',
order_num int comment '尺码序号',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/dim_product_sku';

drop table if exists edw.dim_size_group;
create table edw.dim_size_group (
  size_name     string comment '尺码名称',
  size_order    string comment '尺码顺序',
  size_group_id int comment '尺码组编码',
  size_id       string comment '尺码id'
)
  stored as parquet
location '/dw_pb/edw.db/dim_size_group';

-- dim_skc_otb_order skc的otb订货量
drop table if exists edw.dim_skc_otb_order;
create external table edw.dim_skc_otb_order(
product_id string comment '产品id',
color_id string comment '颜色id',
otb_order_qty int comment 'otb订货量',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/dim_skc_otb_order';

-- dim_stockorg  库存组织维表
drop table if exists edw.dim_stockorg;
create external table edw.dim_stockorg (
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
location '/dw_pb/edw.db/dim_stockorg';

drop table if exists edw.dim_store_trans_status;
CREATE EXTERNAL TABLE edw.dim_store_trans_status (
  store_id STRING comment '门店id',
  store_code STRING comment '门店编码',
  store_name STRING comment '门店名称',
  not_in STRING comment '不进',
  not_out STRING comment '不出',
  date_dec STRING comment '决策日期',
  etl_time string comment 'etl时间'
)
LOCATION '/dw_pb/edw.db/dim_store_trans_status';

drop table if exists edw.dim_target_amt_date;
create external table edw.dim_target_amt_date(
date_string string comment '日期',
year_id int comment '年份',
mon_id int comment '月份',
weekday_id int comment '星期几',
begin_date_week_id int comment '周的起始日期对应的今年第几周',
end_date_week_id int comment '周的结束日期对应的今年第几周',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/dim_target_amt_date';

-- dim_target_product 目标产品维表
drop table if exists edw.dim_target_product;
create external table edw.dim_target_product(
dq_id string comment '大区id',
dq_long_code string comment '大区长编码',
city_id string comment '城市id',
city_long_code string comment '城市长编码',
product_id string comment '货号',
product_name string comment '名称',
product_code string comment '产品编码',
year_id int comment '年份',
quarter_id string comment '季节',
big_class string comment '大类',
mid_class string comment '中类',
tiny_class string comment '小类',
mictiny_class string comment '细分小类',
brand string comment '品牌',
band string comment '波段',
cost_price decimal (38, 10) comment '成本价',
tag_price decimal (38, 10) comment '吊牌价',
gender string comment '性别',
put_on_date string comment '上架日期',
pull_off_date string comment '下架日期',
etl_time timestamp )
partitioned by (day_date string comment '日期')
stored as parquet
location '/dw_pb/edw.db/dim_target_product';

drop table if exists edw.dim_target_quarter;
create external table edw.dim_target_quarter(
day_date string comment '日期',
date_year int,
date_month int,
year_id int,
quarter_id string)
stored as parquet
location '/dw_pb/edw.db/dim_target_quarter';

drop table if exists edw.dim_ud_date;
create external table edw.dim_ud_date(
day_date string,
year_id string,
month_id string,
day_id string,
week_id string,
weekday_id string)
stored as parquet
location '/dw_pb/edw.db/dim_ud_date';

-- dim_weather  天气信息表
drop table if exists edw.dim_weather;
create external table edw.dim_weather(
id int comment '主键',
city_id string comment '城市编号',
city string comment '城市名称',
parent_city string comment '上级城市',
admin_area string comment '所属行政区',
country string comment '所属国家',
lat string comment '经度',
lon string comment '纬度',
time_zone string comment '所在时区',
time_local timestamp comment '当地时间',
time_utc timestamp comment 'utc时间',
weather_code_day string comment '白天天气状况代码',
weather_code_night string comment '晚间天气状况代码',
weather_txt_day string comment '白天天气状况描述',
weather_txt_night string comment '晚间天气状况描述',
weather_date string comment '预报日期',
humidity string comment '相对湿度',
precipitation string comment '降水量',
precipitation_probability string comment '降水概率',
pressure string comment '大气压强',
temperature_max string comment '最高温度',
temperature_min string comment '最低温度',
uv_rays string comment '紫外线强度指数',
visibility string comment '能见度，单位：公里',
wind_deg string comment '风向360角度',
wind_dir string comment '风向',
wind_sc string comment '风力',
wind_spd string comment '风速，公里/小时',
createtime string comment '创建时间',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/dim_weather';

-- 历史天气信息表 dim_weather_his
drop table if exists edw.dim_weather_his;
create external table edw.dim_weather_his(
id string comment '主键',
rowid string comment '',
city string comment '城市名称',
country string comment '所属国家',
parent_city string comment '上级城市',
province string comment '省份',
lat string comment '经度',
lon string comment '纬度',
weather string comment '天气',
weather_code string comment '天气code',
weather_date string comment '天气日期',
temperature_max string comment '最高温度',
temperature_min string comment '最低温度',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/dim_weather_his';

-- dim_webposdisproductitem  产品活动维度表
drop table if exists edw.dim_webposdisproductitem;
create external table edw.dim_webposdisproductitem(
id int comment '主键',
act_id int comment '促销策略表头',
product_id string comment '产品编号',
creation_date timestamp comment '创建时间',
modified_date timestamp comment '修改时间',
is_active string comment '可用',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/dim_webposdisproductitem';

-- dim_webposdisstoreitem  门店活动维度表
drop table if exists edw.dim_webposdisstoreitem;
create external table edw.dim_webposdisstoreitem(
id int comment '主键',
act_id int comment '促销策略表头',
store_id string comment '店仓编号',
creation_date timestamp comment '创建时间',
modified_date timestamp comment '修改时间',
is_active string comment '可用',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/dim_webposdisstoreitem';

--fct_business_advice 电商数据表
drop table if exists edw.fct_business_advice;
create external table edw.fct_business_advice(
brandid string comment '品牌编码',
date string comment '日期',
flat string comment '平台',
product_id string comment '款编号',
scan int comment '浏览量',
visitornum int comment '访客数',
avg_staytime decimal (30, 8) comment '平均停留时长，单位秒',
rate_detailpage decimal (30, 8) comment '产品id详情页跳出率',
rate_order decimal (30, 8) comment '下单转化率',
rate_orderpay decimal (30, 8) comment '下单支付转化率',
rate_pay decimal (30, 8) comment '支付转化率',
orderamount decimal (30, 8) comment '下单金额',
orderqty int comment '下单商品件数',
orderpeoplenum int comment '下单买家数',
payamount decimal (30, 8) comment '支付金额',
payqty int comment '支付商品件数',
addpayqty int comment '加购件数',
avg_visitorvalue decimal (30, 8) comment '访客平均价值',
hitcount int comment '点击次数',
rate_hit decimal (30, 8) comment '点击转化率',
rate_exposure int comment '曝光量',
collectpeoplenum int comment '收藏人数',
buyernum_searchguide int comment '搜索引导支付买家数',
avg_price decimal (30, 8) comment '客单价',
rate_searchpay decimal (30, 8) comment '搜索支付转化率',
visitornum_searchboot int comment '搜索引导访客数',
buyernum int comment '支付买家数',
returnamount_sale decimal (30, 8) comment '售中售后成功退款金额',
returnbillnum_sale int comment '售中售后成功退款笔数',
loaddate string comment '数据读取时间',
etl_time string )
stored as parquet
location '/dw_pb/edw.db/fct_business_advice';

-- 补货调拨需要的表结构
drop table if exists edw.fct_city_move_lt;
create external table edw.fct_city_move_lt(
city_send_id string comment '发货城市',
city_rec_id string comment '收货城市',
dist_send_id string comment '发货大区id',
prov_send_id string comment '发货省份',
dist_rec_id string comment '收货大区id',
prov_rec_id string comment '收货省份',
lt_mov int comment '货运时间',
time_update timestamp )
stored as parquet
location '/dw_pb/edw.db/fct_city_move_lt';

-- fct_io 出入库事实表
drop table if exists edw.fct_io;
create external table edw.fct_io(
doc_id string comment '单据id',
io_time timestamp comment '出入库时间',
io_date string comment '出入库日期',
org_id string comment '组织 id',
product_id string comment '货号',
color_id string comment '颜色 id',
size_id string comment '尺码 id',
io_type string comment '出入库类型',
qty int comment '出入库数量',
amt decimal (38, 10) comment '出入库金额',
etl_time timestamp comment 'etl 时间')
stored as parquet
location '/dw_pb/edw.db/fct_io';

-- fct_month_end_stock 月结库存表
drop table if exists edw.fct_month_end_stock;
create external table edw.fct_month_end_stock(
stock_date string comment '日期',
org_id string comment '组织 id',
product_id string comment '货号',
color_id string comment '颜色 id',
size_id string comment '尺码 id',
stock_qty int comment '库存数量',
stock_price decimal (38, 10) comment '库存单价',
stock_amt decimal (38, 10) comment '库存金额',
year string comment '年',
month string comment '月',
etl_time timestamp comment 'etl 时间')
stored as parquet
location '/dw_pb/edw.db/fct_month_end_stock';

-- fct_sales  销售事实表
drop table if exists edw.fct_sales;
create external table edw.fct_sales (
machine_id string comment '机器号',
audit_time timestamp comment '审核时间',
store_id string comment '店铺id',
pay_amt decimal (38, 10) comment '整单金额',
discount_amt decimal (38, 10) comment '整单折扣金额',
order_id string comment '订单号',
customer_code string comment '客户编码',
sale_time timestamp comment '业务时间',
sale_date string comment '业务日期',
serial_id string comment '序列号',
product_id string comment '货号',
color_id string comment '颜色id',
size_id string comment '尺码id',
qty int comment '数量',
real_price decimal (38, 10) comment '商品成交单价',
real_amt decimal (38, 10) comment '商品成交总额',
etl_time timestamp comment 'etl时间')
stored as parquet
location '/dw_pb/edw.db/fct_sales';

-- fct_sku_day_allot 实际调拨表
drop table if exists edw.fct_sku_day_allot;
create external table edw.fct_sku_day_allot(
product_code string comment '货号',
color_code string comment '颜色',
size_code string comment '尺码',
send_org_code string comment '调出组织',
send_org_long_code string comment '调出组织长编码',
real_allot_qty int comment '实际调拨量',
receive_org_code string comment '调入组织',
receive_org_long_code string comment '调入组织长编码',
day_date string comment '日期',
etl_time timestamp comment 'etl时间')
stored as parquet
location '/dw_pb/edw.db/fct_sku_day_allot';

-- fct_stock  库存移动事实表
drop table if exists edw.fct_stock;
create external table edw.fct_stock(
doc_id string comment '单据id',
product_id string comment '商品id',
color_id string comment '颜色 id',
size_id string comment '尺码 id',
send_org_id string comment '发出组织 id',
receive_org_id string comment '接收组织 id',
mid_org_id string comment '中转组织 id',
movetype_code string comment '库存事务处理类型',
send_qty int comment '发出数量',
send_amt decimal (38, 10) comment '发出金额',
receive_qty int comment '接收数量',
receive_amt decimal (38, 10) comment '接收金额',
send_time string comment '发出时间',
send_date string comment '发出日期',
receive_time string comment '接收时间',
receive_date string comment '接收日期',
out_description string comment '出库单描述',
in_description string comment '入库单描述',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/fct_stock';

-- fct_webposdis 事件活动事实表
drop table if exists edw.fct_webposdis;
create external table edw.fct_webposdis(
id int comment '主键',
act_code string comment '策略编号',
act_name string comment '策略名称',
act_type string comment '策略类型',
act_date_begin string comment '开始日期',
act_date_end string comment '结束日期',
act_time_limit string comment '限时优惠',
act_time_begin string comment '开始时间（限时优惠）',
act_time_end string comment '结束时间（限时优惠）',
vip_lime string comment '仅限vip',
vip_birdthday_limit string comment '仅限vip生日当日',
vip_birdthmonth_limit string comment '仅限vip生日当月',
is_list_limit string comment '是否正价前提',
act_off_type_name string comment '优惠方式',
execute_content string comment '执行内容（折扣0~1）',
auto_int string comment '自动翻倍执行',
monday string comment '周一',
tuesday string comment '周二',
wendnesay string comment '周三',
thursday string comment '周四',
friday string comment '周五',
saturday string comment '周六',
sunday string comment '周日',
exdispro string comment '打折商品除外',
ifexe_same string comment '是否和其他同类策略同时执行',
ifexe string comment '是否和其他策略同时执行',
isvipexp string comment '是否享受vip折上折',
islimitpro string comment '是否限制优惠商品',
limitpropricename string comment '限制优惠商品价格条件',
limitproqty int comment '限制优惠商品数量条件',
dissorttypename string comment '多种折扣排序方式',
all_store string comment '是否应用在所有店铺',
description string comment '描述',
act_vip_plan_id string comment 'vip活动计划',
oa_number string comment 'oa活动申请编号',
owner_id int comment '创建人',
modifier_id int comment '修改人',
creation_date timestamp comment '创建时间',
modified_date timestamp comment '修改时间',
status string comment '提交状态',
status_name string comment '提交状态',
submiter_id int comment '提交人',
submit_time timestamp comment '提交时间',
act_is_active string comment '可用',
write_time timestamp comment '写入时间',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/fct_webposdis';

drop table if exists edw.inv_targ_toc;
create table edw.inv_targ_toc (
  prod_id        string,
  color_id       string,
  size           string,
  store_id       string,
  daytype        string,
  quant_inv_targ float,
  quant_shift    int,
  date_dec       string,
  time_update    timestamp
)
  stored as parquet
location '/dw_pb/edw.db/inv_targ_toc';

-- mid_day_end_stock 日末库存表
drop table if exists edw.mid_day_end_stock;
create external table edw.mid_day_end_stock(
org_id string comment '库存组织id',
org_type string comment '库存组织类型',
product_id string comment '产品id',
color_id string comment '颜色id',
size_id string comment '尺码id',
stock_qty int comment '库存数量',
receive_qty int comment '日收货量',
send_qty int comment '日发货量',
etl_time timestamp )
partitioned by (stock_date string comment '库存日期')
stored as parquet
location '/dw_pb/edw.db/mid_day_end_stock';

-- mid_day_end_stock_lz 览众计算实际日末库存表
drop table if exists edw.mid_day_end_stock_lz;
create external table edw.mid_day_end_stock_lz(
org_id string comment '库存组织id',
org_type string comment '库存组织类型',
product_id string comment '产品id',
color_id string comment '颜色id',
size_id string comment '尺码id',
stock_qty int comment '库存数量',
etl_time timestamp )
partitioned by (stock_date string comment '库存日期')
stored as parquet
location '/dw_pb/edw.db/mid_day_end_stock_lz';

-- mid_lifecycle_otb_sales 细分小类otb生命周期计划销量
drop table if exists edw.mid_lifecycle_otb_sales;
create external table edw.mid_lifecycle_otb_sales(
year_id int comment '年份',
quarter_id string comment '季节',
band string comment '波段',
mictiny_class string comment '细分品类',
week_date string comment '年周',
sale_percent decimal (38, 10) comment '销售权重',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/mid_lifecycle_otb_sales';

drop table if exists edw.mid_model_allot_evaluation;
create table edw.mid_model_allot_evaluation (
  product_id          string comment '产品id',
  product_code        string comment '产品code',
  color_id            string comment '颜色id',
  color_code          string comment '颜色code',
  send_store_id       string comment '调出门店id',
  send_store_code     string comment '调出门code',
  receive_store_id    string comment '调入门店id',
  receive_store_code  string comment '调入门code',
  day_date            string comment '日期',
  ai_allot_qty        int comment 'ai调拨量',
  peacebird_allot_qty int comment '太平鸟调拨量',
  etl_time            timestamp
)
  stored as parquet
location '/dw_pb/edw.db/mid_model_allot_evaluation';

-- mid_model_demand 模型需求表(每个季度运行)
drop table if exists edw.mid_model_demand;
create table edw.mid_model_demand (
  product_id           string comment '产品id',
  color_id             string comment '颜色id',
  sale_date            string comment '销售日期',
  sale_total           int comment '累计销量',
  inventory_total      int comment '总库存 = 总仓 + 门店 + 在途',
  product_total        int comment '总生产量',
  min_distributed_date string comment '首铺日期',
  delta_day            int comment '销售日期与首铺日期之差',
  year_id              int comment '年份id',
  quarter_id           string comment '季节',
  overate              decimal(30, 8) comment '累计销量 / 总生产量',
  etl_time             timestamp
)
  --stored as parquet
location '/dw_pb/edw.db/mid_model_demand';

drop table if exists edw.mid_skc_day_sales_total_rate;
CREATE TABLE edw.mid_skc_day_sales_total_rate(
  product_id string COMMENT '产品id', 
  color_id string COMMENT '颜色id', 
  sale_total int COMMENT '累计销量', 
  inventory_total int COMMENT '总库存 = 总仓 + 门店 + 在途', 
  product_total int COMMENT '总生产量', 
  overate decimal(30,8) COMMENT '累计销量 / 总生产量', 
  etl_time string)
PARTITIONED BY ( 
  sale_date string COMMENT '销售日期')
stored as parquet
LOCATION
  '/dw_pb/edw.db/mid_skc_day_sales_total_rate';

drop table if exists edw.mid_skc_has_stock_store_count;
CREATE EXTERNAL TABLE edw.mid_skc_has_stock_store_count(
  product_id string COMMENT '产品id', 
  product_code string COMMENT '产品编码', 
  color_id string COMMENT '颜色id', 
  color_code string COMMENT '颜色编码', 
  has_stock_store_count int COMMENT '在途+在库 > 0', 
  etl_time string)
PARTITIONED BY ( 
  day_date string COMMENT '库存日期')
stored as parquet
LOCATION
  '/dw_pb/edw.db/mid_skc_has_stock_store_count';

drop table if exists edw.mid_model_evaluation_summary;
create table edw.mid_model_evaluation_summary (
  peacebird_rep_qty_total           int comment '太平鸟补货量',
  peacebird_rep_bags_total          int comment '太平鸟补货包裹数',
  peacebird_rep_skc_total           int comment '太平鸟补货skc总数',
  peacebird_rep_send_org_count      int comment '太平鸟补货发货组织数',
  peacebird_rep_receive_org_count   int comment '太平鸟补货收货组织数',
  ai_rep_qty_total                  int comment '模型补货量',
  ai_rep_bags_total                 int comment '模型补货包裹数',
  ai_rep_skc_total                  int comment '模型补货skc总数',
  ai_rep_send_org_count             int comment '模型补货发货组织数',
  ai_rep_receive_org_count          int comment '模型补货收货组织数',
  peacebird_allot_qty_total         int comment '太平鸟调拨量',
  peacebird_allot_bags_total        int comment '太平鸟调拨包裹数',
  peacebird_allot_skc_total         int comment '太平鸟调拨skc总数',
  peacebird_allot_send_org_count    int comment '太平鸟调拨发货组织数',
  peacebird_allot_receive_org_count int comment '太平鸟调拨收货组织数',
  ai_allot_qty_total                int comment '模型调拨量',
  ai_allot_bags_total               int comment '模型调拨包裹数',
  ai_allot_skc_total                int comment '模型调拨skc总数',
  ai_allot_send_org_count           int comment '模型调拨发货组织数',
  ai_allot_receive_org_count        int comment '模型调拨收货组织数',
  day_date                          string comment '日期',
  etl_time                          timestamp
)
  stored as parquet
location '/dw_pb/edw.db/mid_model_evaluation_summary';

drop table if exists edw.mid_model_replenish_evaluation;
create table edw.mid_model_replenish_evaluation (
  product_id        string comment '产品id',
  product_code      string comment '产品编码',
  color_id          string comment '颜色id',
  color_code        string comment '颜色编码',
  org_id            string comment '补货仓库id',
  org_code          string comment '补货仓库code',
  store_id          string comment '门店id',
  store_code        string comment '门店编码',
  ai_rep_qty        int comment 'ai补货量',
  peacebird_rep_qty int comment 'peacebird补货量',
  etl_time          timestamp comment 'etl时间')
  partitioned by (day_date string comment '库存日期')
  stored as parquet
location '/dw_pb/edw.db/mid_model_replenish_evaluation';

drop table if exists edw.mid_model_replenish_evaluation_detail;
create table edw.mid_model_replenish_evaluation_detail (
  product_id             string comment '产品id',
  product_code           string comment '产品编码',
  color_id               string comment '颜色id',
  color_code             string comment '颜色编码',
  size_id                string comment '尺码id',
  size_code              string comment '尺码编码',
  size_name              string comment '尺码名称',
  org_id                 string comment '补货仓库id',
  org_code               string comment '补货仓库code',
  store_id               string comment '门店id',
  store_code             string comment '门店编码',
  init_stock_qty         int comment '期初库存数量',
  target_stock_inventory decimal(30, 8) comment '目标库存数量',
  last_w1_sales_qty      bigint comment '过去七天销量',
  last_w2_sales_qty      bigint comment '过去十四天到七天的销量',
  ai_rep_qty             int comment 'ai补货量',
  peacebird_rep_qty      int comment 'peacebird补货量',
  etl_time               timestamp comment 'etl时间'
)
  partitioned by (day_date string comment '日期')
  stored as parquet
location '/dw_pb/edw.db/mid_model_replenish_evaluation_detail';

-- mid_org_day_target_amt 组织目标销售额表
drop table if exists edw.mid_org_day_target_amt;
create external table edw.mid_org_day_target_amt(
org_id string comment '组织',
org_code string comment '组织编码',
org_long_code string comment '组织长编码',
org_type string comment '组织类型',
target_amt decimal (30, 8) comment '目标销售额',
etl_time timestamp )
partitioned by (day_date string comment '日期')
stored as parquet
location '/dw_pb/edw.db/mid_org_day_target_amt';

-- mid_skc_day_road_stock skc日在途库存
drop table if exists edw.mid_skc_day_road_stock;
create external table edw.mid_skc_day_road_stock(
product_id string comment '产品id',
color_id string comment '颜色id',
road_stock_qty int comment '库存数量',
etl_time timestamp )
partitioned by (day_date string comment '库存日期')
stored as parquet
location '/dw_pb/edw.db/mid_skc_day_road_stock';

-- mid_skc_store_day_sales skc门店销量
drop table if exists edw.mid_skc_store_day_sales;
create external table edw.mid_skc_store_day_sales(
product_id string comment '产品id',
product_code string comment '产品编码',
color_id string comment '颜色id',
color_code string comment '颜色编码',
store_id string comment '门店id',
store_code string comment '门店编码',
sales_qty int comment '销售数量',
sales_amt decimal (30, 8) comment '销售额',
total_sales_qty int comment '累计销量',
total_sales_amt decimal (30, 8) comment '累计销售额',
etl_time timestamp )
partitioned by (day_date string comment '销售日期')
stored as parquet
location '/dw_pb/edw.db/mid_skc_store_day_sales';

-- mid_sku_add_order_expected_arrival 追单预计到货表
drop table if exists edw.mid_sku_add_order_expected_arrival;
create external table edw.mid_sku_add_order_expected_arrival(
order_id string comment '追单单据id',
sku_id string comment 'sku_id',
product_id string comment '产品id',
color_id string comment '颜色id',
size_id string comment '尺码id',
expected_arrive_date string comment '预计到货日期',
expected_arrive_qty int comment '预计到货数量',
actual_arrive_date string comment '实际到货日期',
actual_arrive_qty int comment '实际到货数量',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/mid_sku_add_order_expected_arrival';

-- mid_sku_add_order_info 追单信息表
drop table if exists edw.mid_sku_add_order_info;
create external table edw.mid_sku_add_order_info(
order_id string comment '追单单据id',
order_date string comment '追单日期',
sku_id string comment 'sku_id',
product_id string comment '产品id',
color_id string comment '颜色id',
size_id string comment '尺码id',
add_order_qty int comment '追单数量',
arrived_qty int comment '已到货数量',
not_arrived_qty int comment '未到货数量',
etl_time timestamp )
stored as parquet
location '/dw_pb/edw.db/mid_sku_add_order_info';

-- mid_sku_add_order_info_history 追单信息备份表
drop table if exists edw.mid_sku_add_order_info_history;
create external table edw.mid_sku_add_order_info_history(
order_id string comment '追单单据id',
order_date string comment '追单日期',
sku_id string comment 'sku_id',
product_id string comment '产品id',
color_id string comment '颜色id',
size_id string comment '尺码id',
add_order_qty int comment '追单数量',
arrived_qty int comment '已到货数量',
not_arrived_qty int comment '未到货数量',
etl_time string )
partitioned by (day_date string comment '备份日期')
stored as parquet
location '/dw_pb/edw.db/mid_sku_add_order_info_history';

drop table if exists edw.mid_store_day_sell_well_skc;
CREATE EXTERNAL TABLE edw.mid_store_day_sell_well_skc(
  store_code string COMMENT '门店 code', 
  product_code string COMMENT '产品code', 
  color_code string COMMENT '颜色code', 
  order_num int COMMENT '在门店销售排名', 
  etl_time timestamp)
PARTITIONED BY ( 
  day_date string COMMENT '日期')
LOCATION
  '/dw_pb/edw.db/mid_store_day_sell_well_skc';

drop table if exists edw.mid_sku_org_day_road_stock_peacebird;
create external table edw.mid_sku_org_day_road_stock_peacebird(
product_id string comment '产品id',
product_code string comment '产品编码',
color_id string comment '颜色id',
color_code string comment '颜色编码',
size_id string comment '尺码id',
size_code string comment '尺码编码',
org_id string comment '组织id',
org_code string comment '组织编码',
org_type string comment '组织类型',
road_stock_qty int comment '在途库存',
road_stock_amt decimal (30, 8) comment '在途库存金额',
on_order_qty int comment '在单库存',
on_order_amt decimal (30, 8) comment '在单库存金额',
peacebird_available_qty int comment '太平鸟可用库存',
peacebird_available_amt decimal comment '太平鸟可用库存金额',
is_emp string comment '是否清空',
etl_time timestamp comment 'etl时间')
partitioned by (day_date string comment '库存日期')
stored as parquet
location '/dw_pb/edw.db/mid_sku_org_day_road_stock_peacebird';

drop table if exists edw.mid_day_target_stock_peacebird;
CREATE external TABLE edw.mid_day_target_stock_peacebird(
  --stock_date string COMMENT '库存日期', 
  org_id string COMMENT '库存组织id', 
  org_type string COMMENT '库存组织类型', 
  product_id string COMMENT '产品id', 
  color_id string COMMENT '颜色id', 
  size_id string COMMENT '尺码id', 
  stock_qty int COMMENT '库存数量',
  active string  COMMENT '是否激活',
  is_emp string comment '是否清空',
  etl_time string)
partitioned by (stock_date string comment '库存日期')
stored as parquet
LOCATION
  '/dw_pb/edw.db/mid_day_target_stock_peacebird';


drop table if exists edw.mod_sale_prediction_param;
create table edw.mod_sale_prediction_param(
product_id string,
color_id string,
pa double,
pb double,
pc double,
pd double,
r_square double,
full_day double,
min_sell_date string,
delta_day bigint,
sale_total double,
pre_day_date string)
stored as parquet
location '/ user /hive/warehouse/edw.db/mod_sale_prediction_param';


create table edw.mod_sales_predict_assess_detail(
prod string,
color string,
actual_sales bigint,
pred_sales double,
week_num string,
accuracy double,
assess_date string,
update_time string)
stored as parquet
location '/ user /hive/warehouse/edw.db/mod_sales_predict_assess_detail';

create table edw.mod_sales_predict_assess_overview(
0% double,
10% double,
20% double,
30% double,
40% double,
50% double,
60% double,
70% double,
80% double,
90% double,
week_num string,
assess_date string,
update_time string)
stored as parquet
location '/ user /hive/warehouse/edw.db/mod_sales_predict_assess_overview';

-- mod_skc_expected_class_basis  skc超/低预期分类依据表
drop table if exists edw.mod_skc_expected_class_basis;
create table edw.mod_skc_expected_class_basis (
  year_id             int comment '年份id',
  quarter_id          string comment '季节id',
  class_celling_slope decimal(30, 8) comment '分类上限斜率',
  class_floor_slope   decimal(30, 8) comment '分类下限斜率',
  etl_time            timestamp
)
  location '/dw_pb/edw.db/mod_skc_expected_class_basis';

-- mod_skc_week_classify 模型周分类表
drop table if exists edw.mod_skc_week_classify;
create table edw.mod_skc_week_classify (
  product_id     string comment '货号',
  color_id       string comment '颜色',
  prod_class     string comment '产品分类',
  stage_lifetime string comment '生命周期阶段',
  cla_week_date  string comment '分类日期',
  etl_time       timestamp
)
  location '/dw_pb/dm.edw/mod_skc_week_classify';

-- mod_sku_day_allot 模型调拨表
drop table if exists edw.mod_sku_day_allot;
create table edw.mod_sku_day_allot (
  product_id       string comment '货号',
  color_id         string comment '颜色',
  size_id          string comment '尺码',
  send_store_id    string comment '发货组织',
  receive_store_id string comment '收货组织',
  date_send        string comment '发货日期',
  date_rec_pred    string comment '预计到货日期',
  send_qty         int comment '发货数量',
  dec_day_date     string comment '决策日期',
  etl_time         string
)
stored as parquet
  location '/dw_pb/edw.db/mod_sku_day_allot';


drop table if exists edw.mod_sku_day_allot_model;
create table edw.mod_sku_day_allot_model (
  product_id       string comment '货号',
  color_id         string comment '颜色',
  size_id          string comment '尺码',
  send_store_id    string comment '发货组织',
  receive_store_id string comment '收货组织',
  date_send        string comment '发货日期',
  date_rec_pred    string comment '预计到货日期',
  send_qty         int comment '发货数量',
  dec_day_date     string comment '决策日期',
  etl_time         string
)
stored as parquet
  location '/dw_pb/edw.db/mod_sku_day_allot_model';

--模型未拆分调拨表历史记录
CREATE EXTERNAL TABLE edw.mod_sku_day_allot_model_history (
  product_id STRING,
  color_id STRING,
  size_id STRING,
  send_store_id STRING,
  receive_store_id STRING,
  date_send STRING,
  date_rec_pred STRING,
  send_qty BIGINT,
  --dec_day_date STRING,
  etl_time STRING
)
PARTITIONED BY (
  dec_day_date STRING COMMENT '决策日期'
)
STORED AS PARQUET
LOCATION '/dw_pb/edw.db/mod_sku_day_allot_model_history';

-- mod_sku_day_replenish 模型补货表
drop table if exists edw.mod_sku_day_replenish;
create table edw.mod_sku_day_replenish (
  product_id       string comment '货号',
  color_id         string comment '颜色',
  size_id          string comment '尺码',
  send_org_id      string comment '发货组织',
  receive_store_id string comment '收货组织',
  date_send        string comment '发货日期',
  date_rec_pred    string comment '预计到货日期',
  send_qty         int comment '发货数量',
  dec_day_date     string comment '决策日期',
  etl_time         string
)
stored as parquet
  location '/dw_pb/edw.db/mod_sku_day_replenish';
  
drop table if exists edw.mod_sku_day_replenish_model_old;
create external table edw.mod_sku_day_replenish_model_old (
  product_id       string comment '货号',
  color_id         string comment '颜色',
  size_id          string comment '尺码',
  send_org_id      string comment '发货组织',
  receive_store_id string comment '收货组织',
  date_send        string comment '发货日期',
  date_rec_pred    string comment '预计到货日期',
  send_qty         int comment '发货数量',
  --dec_day_date     string comment '决策日期',
  etl_time         string
)
partitioned by (dec_day_date string comment '决策日期')
stored as parquet
  location '/dw_pb/edw.db/mod_sku_day_replenish_model_old';

-- mod_skc_day_sales_prediction 模型销量预测表
drop table if exists edw.mod_skc_day_sales_prediction;
create table edw.mod_skc_day_sales_prediction (
  product_id                  string,
  color_id                    string,
  org_id                      string,
  org_type                    string,
  one_week_sales_qty          int,
  two_week_sales_qty          int,
  four_week_sales_qty         int,
  two_week_sales_celling_qty  int,
  two_week_sales_floor_qty    int,
  four_week_sales_celling_qty int,
  four_week_sales_floor_qty   int,
  residue_sales_celling_qty   int,
  residue_sales_floor_qty     int,
  pre_day_date                string,
  etl_time                    timestamp
)
  location '/dw_pb/edw.db/mod_skc_day_sales_prediction';

drop table if exists edw.mod_sku_store_day_target_inventory;
create table edw.mod_sku_store_day_target_inventory (
  product_id   string comment '货号',
  color_id     string comment '颜色',
  size_id      string comment '尺码',
  year_id      int comment '年份',
  quarter_id   string comment '季节',
  store_id     string comment '门店',
  target_stock int comment '目标库存',
  day_date     string comment '日期',
  etl_time     timestamp
)
  location '/dw_pb/edw.db/mod_sku_store_day_target_inventory';

-- 根据 替代款 模型补货表
drop table if exists edw.mod_sku_day_replenish_model;
create table edw.mod_sku_day_replenish_model (
  product_id       string comment '货号',
  color_id         string comment '颜色',
  size_id          string comment '尺码',
  send_org_id      string comment '发货组织',
  receive_store_id string comment '收货组织',
  date_send        string comment '发货日期',
  date_rec_pred    string comment '预计到货日期',
  send_qty         int comment '发货数量',
  etl_time         string,
  dec_day_date     string comment '决策日期'
)
stored as parquet
  location '/dw_pb/edw.db/mod_sku_day_replenish_model';
  
-- 太平鸟替代款 备份表
drop table if exists edw.dim_relate_sku_history;
create external table if not exists edw.dim_relate_sku_history (
id string,
ad_client_id string,
ad_org_id string,
m_productalias_id string comment '主款sku_id', 
m_productalias2_id string comment '替代款sku_id',
priority string comment '替代款优先级',
modifieddate string comment '修改时间',
isactive string comment '是否可用',
etl_time string
) 
partitioned by (day_date string comment '备份日期')
stored as parquet
location '/dw_pb/edw.db/dim_relate_sku_history';
  
-- 计算齐码使用的表结构
drop table if exists edw.size_info;
create table edw.size_info (
  product_code string comment '产品code',
  color_code   string comment '颜色code',
  size_name    string comment '尺码名称',
  store_code   string comment '店铺code',
  stock_qty    int comment '库存数量',
  store_area   string comment '库存区域，1：华北、东北；2：其他',
  stock_date   string comment '库存日期'
)
  partitioned by (dt string comment '日期分区字段'
)
  stored as parquet
location '/dw_pb/edw.db/size_info';

drop table if exists edw.size_info_after_replenish_allot;
create table edw.size_info_after_replenish_allot (
  product_code string comment '产品code',
  color_code   string comment '颜色code',
  size_name    string comment '尺码名称',
  store_code   string comment '店铺code',
  stock_qty    int comment '库存数量',
  store_area   string comment '库存区域，1：华北、东北；2：其他',
  stock_date   string comment '库存日期'
)
  partitioned by (dt string comment '日期分区字段')
  stored as parquet
location '/dw_pb/edw.db/size_info_after_replenish_allot';

drop table if exists edw.size_info_available;
create table edw.size_info_available (
  product_code string comment '产品code',
  color_code   string comment '颜色code',
  size_name    string comment '尺码名称',
  store_code   string comment '店铺code',
  stock_qty    int comment '库存数量',
  store_area   string comment '库存区域，1：华北、东北；2：其他',
  stock_date   string comment '库存日期'
)
  partitioned by (dt string comment '日期分区字段'
)
  stored as parquet
location '/dw_pb/edw.db/size_info_available';

drop table if exists edw.mid_stock_check_res;
create table edw.mid_stock_check_res (
  stock_date           string,
  org_code             string,
  org_name             string,
  org_type             string,
  our_record_total     int,
  their_record_total   int,
  equal_record_total   int,
  equal_rate           decimal(30, 8),
  not_join_total       int,
  not_join_and_equal_0 int,
  equal_0_rate         decimal(30, 8),
  etl_time             timestamp
)
  location '/dw_pb/edw.db/mid_stock_check_res';
  
-- 补货调拨返仓相关的表的备份
  
drop table if exists edw.mod_sku_day_allot_old;
CREATE EXTERNAL TABLE edw.mod_sku_day_allot_old(
  product_id string, 
  color_id string, 
  size_id string, 
  send_store_id string, 
  receive_store_id string, 
  date_send string, 
  date_rec_pred string, 
  send_qty bigint, 
  --dec_day_date string, 
  etl_time string)
PARTITIONED BY (dec_day_date string)
STORED AS parquet
LOCATION
  '/dw_pb/edw.db/mod_sku_day_allot_old';

drop table if exists edw.mod_sku_day_replenish_old;  
CREATE EXTERNAL TABLE edw.mod_sku_day_replenish_old(
  product_id string, 
  color_id string, 
  size_id string, 
  send_org_id string, 
  receive_store_id string, 
  date_send string, 
  date_rec_pred string, 
  send_qty string, 
  --dec_day_date string, 
  etl_time string)
  PARTITIONED BY (dec_day_date string)
STORED AS parquet
LOCATION
  '/dw_pb/edw.db/mod_sku_day_replenish_old';

drop table if exists edw.mod_sku_day_return_result_old;  
CREATE EXTERNAL TABLE edw.mod_sku_day_return_result_old(
  product_id string, 
  color_id string, 
  size_id string, 
  send_store_id string, 
  receive_store_id string, 
  date_send string, 
  date_rec_pred string, 
  send_qty bigint, 
  --dec_day_date string, 
  etl_time string)
PARTITIONED BY (dec_day_date string)
STORED AS parquet
LOCATION
  '/dw_pb/edw.db/mod_sku_day_return_result_old';

drop table if exists edw.mod_sku_store_day_target_inventory_history;  
CREATE EXTERNAL TABLE edw.mod_sku_store_day_target_inventory_history(
  product_id string, 
  color_id string, 
  size_id string, 
  store_id string, 
  target_stock double, 
  --day_date string, 
  etl_time string, 
  year_id string, 
  quarter_id string)
PARTITIONED BY (day_date string)
STORED AS parquet
LOCATION
  '/dw_pb/edw.db/mod_sku_store_day_target_inventory_history';


drop table if exists edw.mid_sku_sale_recover;  
CREATE TABLE edw.mid_sku_sale_recover(
  store_id string, 
  product_id string,
  color_id string, 
  size_id string, 
  end_stock_qty int, 
  qty int, 
  --day_date string, 
  `dayofweek` int,
  holiyday string,
  etl_time string)
PARTITIONED BY (stock_date string)
STORED AS parquet
LOCATION
  '/dw_pb/edw.db/mid_sku_sale_recover';
