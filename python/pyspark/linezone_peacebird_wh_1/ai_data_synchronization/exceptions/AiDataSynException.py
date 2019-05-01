# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: linezone-peacebird-backend-1
# Filename: AiDataSynchronizationException
# Author: zsm
# Date: 2018/11/2 23:32
# ----------------------------------------------------------------------------


class GetTokenException(Exception):
    def __init__(self, status_code=None, message=None):
        self.status_code = status_code
        self.message = message


class DatabaseException(GetTokenException):
    pass


class QueryNothingException(GetTokenException):
    pass


class CleanDataException(GetTokenException):
    pass


class DataSynException(Exception):
    def __init__(self, status_code=None, message=None, send_data=None):

        self.status_code = status_code
        self.message = message
        self.send_data = send_data


class SysException(GetTokenException):
    pass
