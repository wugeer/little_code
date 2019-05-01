# coding:utf-8

from ai_data_synchronization.constants.SysConst import _constants


# 构造一个常量类的实例变量
COMMON_CONSTANTS = _constants()

# AI数据回写drp token验证日志相关配置日志
COMMON_CONSTANTS.AI_DATA_TOKEN_LOG_ERROR_FILE = '/logs/ai_data_token_error.log'
# AI数据回写drp 发送数据日志相关配置信息
COMMON_CONSTANTS.AI_DATA_SYN_LOG_ERROR_FILE = '/logs/ai_data_syn_error.log'
# AI数据回写drp 查询数据库，清洗数据错误等日志相关配置信息
COMMON_CONSTANTS.AI_DATA_SERVER_LOG_ERROR_FILE = '/logs/ai_data_server_error.log'
COMMON_CONSTANTS.LOG_FORMATTER = '太平鸟产品 - %(asctime)s - %(levelname)s - %(filename)s' \
                                 ' - %(funcName)s - %(lineno)s - %(message)s'

