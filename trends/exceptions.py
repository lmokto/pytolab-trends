class TrendsError(Exception):
    pass

class DbError(TrendsError):
    pass

class MQError(TrendsError):
    pass
