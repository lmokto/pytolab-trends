import logging

# Set up logger.
DEFAULT_LOG_FORMAT_STRING = '%(asctime)s [pid:%(process)d %(name)s ' \
                            '%(filename)s:(%(lineno)d)] %(levelname)s: ' \
                            '%(message)s'

logger = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT_STRING))
logger.addHandler(handler)
