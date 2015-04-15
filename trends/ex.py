#!/usr/bin/env python
# -*- coding: utf-8 -*-

class TrendsError(Exception):
    pass

class DbError(TrendsError):
    pass

class MQError(TrendsError):
    pass

class DataError(TrendsError):
    pass
