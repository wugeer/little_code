"""
------------------------------------
file name :tools
description: 通过工具包
author: xhw
date:2019-01-22
-----------------------------------
"""
from pyspark import  SparkConf
from pyspark.sql import  SparkSession


class SparkInit(object):
    def __init__(self, file_name):
        # 这个是表的存储路径
        # warehouse_location = abspath('hdfs://master1:9000/user/hive/warehouse')
        # 程序的名字
        app_name = "".join(["PySpark_", file_name])
        # 此处没有设置表的存储路径
        spark_conf = SparkConf()
        # 解决spark 默认的string最大长度为25，不够用的情况
        spark_conf.set("spark.debug.maxToStringFields", "100")
        # 设置表的存储路径
        # spark_conf.set("spark.sql.warehouse.dir", warehouse_location)
        # 允许动态分区
        spark_conf.set("hive.exec.dynamic.partition.mode", 'nonstrict')
        # 解决报错：Detected cartesian product for INNER join between logical plans
        spark_conf.set("spark.sql.crossJoin.enabled", 'true')
        # 这个设置可能导致很慢，如果确实很慢可以尝试注释这一行
        spark_conf.set("spark.sql.shuffle.partitions", '1')

        # sparkSession
        self.spark = SparkSession\
            .builder\
            .master('yarn')\
            .appName(app_name)\
            .config(conf=spark_conf)\
            .enableHiveSupport()\
            .getOrCreate()
        # 获取脚本执行需要的参数
        # self.params_dict = self.get_params()

    def register_udf(self, fun_name, udf_fun_name):
        """
        :param fun_name: 被注册的函数名字
        :param udf_fun_name: 注册后在spark sql中使用的函数
        :return: 无
        """
        self.spark.udf.register(udf_fun_name, fun_name)

    def create_temp_table(self, sql, table_name):
        """
        创建临时表，此处对sql不作处理
        :param sql: 执行的sql
        :param table_name: 得到临时表的名字
        :return: 临时表的dataframe
        """
        df_temp_table = self.spark.sql(sql)
        df_temp_table.createOrReplaceTempView(table_name)
        return df_temp_table

    def drop_temp_table(self, table_name):
        """
        删除临时表
        :param table_name:要删除的临时表的名字
        :return: 无
        """
        self.spark.catalog.dropTempView(table_name)

    def execute_sql(self, sql):
        """
        单纯的执行一段sql，不要返回值和注册成临时表等其他相关的操作
        :param sql: 要执行的sql
        :return: 无
        """
        self.spark.sql(sql)

    def return_df(self, sql):
        """
        执行一段sql,仅得到结果的df
        :param sql: 要执行的sql
        :return: 结果df
        """
        return  self.spark.sql(sql)

    def explicit_cache_table(self, table_name):
        """
        显式缓存临时表
        :param table_name: 要缓存的临时表的名字
        :return: 无
        """
        self.spark.sql("cache table %s" % table_name)

    def explicit_uncache_table(self, table_name):
        """
        显式缓存临时表
        :param table_name: 要缓存的临时表的名字
        :return: 无
        """
        self.spark.sql("uncache table %s" % table_name)

    def lazy_cache_table(self, table_name):
        """
        spark原生的惰性缓存
        :param table_name: 要缓存的临时表
        :return:
        """
        self.spark.catalog.cacheTable(table_name)

    def lazy_uncache_table(self, table_name):
        """
        去掉缓存的临时表
        :param table_name: 要去掉的缓存的临时表
        :return:
        """
        self.spark.catalog.uncacheTable(table_name)


