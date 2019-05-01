# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: linezone_peacebird_wh_1
# Filename: config.py
# Author: zsm
# Date: 2018/11/5 13:16
# ----------------------------------------------------------------------------
import os
import logging
from logging.handlers import TimedRotatingFileHandler
from ai_data_synchronization.constants.CommonConstants import COMMON_CONSTANTS


class Config(object):
    SQLALCHEMY_POOL_SIZE = 32
    SQLALCHEMY_POOL_TIMEOUT = 10
    SQLALCHEMY_TRACK_MODIFICATIONS = True


class DevelopmentConfig(Config):
    DEBUG = True
    # todo: 配置地址
    # SQLALCHEMY_DATABASE_URI = "postgresql://postgres:lzsj1701@192.168.200.201:5432/peacebird"
    SQLALCHEMY_DATABASE_URI = "postgresql://postgres:lzpb$123@172.18.1.190:5432/postgres"
    # SQLALCHEMY_DATABASE_URI = "postgresql://postgres:lzpb$123@192.168.201.23:5432/postgres"
    SQLALCHEMY_TRACK_MODIFICATIONS = True
    SQLALCHEMY_SCHEMA = 'rst'
    """
    # 测试环境oracle
    ORACLE_URL = "ai/aipeace@172.18.10.7:1521/orcl"
    ORACLE_USERNAME = "ai"
    ORACLE_PASSWD = "aipeace"
    ORACLE_HOST = "172.18.10.7"
    ORACLE_PORT = 1521
    ORACLE_DB_NAME = "orcl"
    """
    
    # 正式环境oracle
    ORACLE_URL = "ai/aipeace@172.18.102.10:1521/drpdg"
    ORACLE_USERNAME = "ai"
    ORACLE_PASSWD = "aipeace"
    ORACLE_HOST = "172.18.102.10"
    ORACLE_PORT = "1521"
    ORACLE_DB_NAME = "drpdg"

# 数据同步配置
class AiDataSynConfig(object):
    """
    # 正式环境获取token的地址
    TOKEN_URL = "https://api.pbwear.com/drp/getToken"

    # 正式环境发送补调数据的地址
    AI_DATA_SYN_URL = "https://api.pbwear.com/ipos/postipostransferai"

    # 正式环境App_ID
    SECRET = {
        "AppID": "87a34336f883e4f4e96d9737103875fe",
        "Secret": "bbfc5072986ca366331076a1891bb1a0"
        }
    """

    # 原测试环境
    # 测试环境获取token的地址
    TOKEN_URL = "http://apitest.pbwear.com/drp/getToken"

    # 测试环境发送补调数据的地址
    AI_DATA_SYN_URL = "http://apitest.pbwear.com/ipos/postipostransfernew"

    # 测试环境发送销售单数据的地址
    AI_DATA_SALES_SLIP_URL = "http://apitest.pbwear.com/ipos/postipossalesnew"

    SECRET = {
        "AppID": "18d7b1a151409b79",
        "Secret": "b3af409bb8423187c75e6c7f5b683908"
        }

    """
    # 测试环境获取token的地址
    TOKEN_URL = "http://apitest.pbwear.com/drp/getToken"

    # 测试环境发送补调数据的地址
    AI_DATA_SYN_URL = "http://apitest.pbwear.com/ipos/postipostransferai"
    
    # 测试环境App_ID
    SECRET = {
        "AppID": "87a34336f883e4f4e96d9737103875fe",
        "Secret": "bbfc5072986ca366331076a1891bb1a0"
        }
    """
    HEADER = {'Content-Type': 'application/json; charset=utf-8'}

    USER_CODE = "'zsm_data_monitor@163.com'"


# 日志配置
class Log(object):

    # 项目根目录
    BASE_PATH = os.path.abspath(os.path.dirname(__file__))

    ai_data_token_log = TimedRotatingFileHandler(
        BASE_PATH + COMMON_CONSTANTS.AI_DATA_TOKEN_LOG_ERROR_FILE, 'D', encoding='UTF-8')
    ai_data_token_log.suffix = "%Y-%m-%d"
    ai_data_token_log.setLevel(logging.ERROR)
    # 为handler设置一个格式器对象， 如：
    # '太平鸟产品 - %(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(lineno)s - %(message)s'
    # filename：调用日志记录函数的源码文件的全路径的文件名部分，包含文件后缀；
    # funcName：	调用日志记录函数的函数名
    # lineno: 调用日志记录函数的源代码所在的行号
    # message: 日志记录的文本内容，通过 msg % args计算得到的, 比如在给的例子中可能是
    # 太平鸟产品 - 2018-10-09 09:45:28,598 - ERROR - tools.py - exception_error_clean_data - 145 - 'product_code'
    ai_data_token_log.setFormatter(logging.Formatter(COMMON_CONSTANTS.LOG_FORMATTER))

    ai_data_syn_log = TimedRotatingFileHandler(
        BASE_PATH + COMMON_CONSTANTS.AI_DATA_SYN_LOG_ERROR_FILE, 'D', encoding='UTF-8')
    ai_data_syn_log.suffix = "%Y-%m-%d"
    ai_data_syn_log.setLevel(logging.ERROR)
    ai_data_syn_log.setFormatter(logging.Formatter(COMMON_CONSTANTS.LOG_FORMATTER))

    ai_data_server_log = TimedRotatingFileHandler(
        BASE_PATH + COMMON_CONSTANTS.AI_DATA_SERVER_LOG_ERROR_FILE, 'D', encoding='UTF-8')
    ai_data_server_log.suffix = "%Y-%m-%d"
    ai_data_server_log.setLevel(logging.ERROR)
    ai_data_server_log.setFormatter(logging.Formatter(COMMON_CONSTANTS.LOG_FORMATTER))

    # logging初始化工作
    logging.basicConfig()

    # token_log的初始化工作
    token_log = logging.getLogger('ai_data_token_log')
    token_log.addHandler(ai_data_token_log)

    # syn_log的初始化工作
    syn_log = logging.getLogger('ai_data_syn_log')
    syn_log.addHandler(ai_data_syn_log)

    # server_log的初始化工作
    server_log = logging.getLogger('ai_data_server_log')
    server_log.addHandler(ai_data_server_log)


if __name__ == '__main__':
    Log()
