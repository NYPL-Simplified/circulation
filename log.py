from nose.tools import set_trace
import datetime
import logging
import json
import os
import socket

DEFAULT_FORMAT = "%(asctime)s:%(name)s:%(levelname)s:%(filename)s:%(message)s"
DEFAULT_LOG_DATA_FORMAT = "text"

class JSONFormatter(logging.Formatter):
    hostname = socket.gethostname()
    def format(self, record):
        data = dict(
            host=self.hostname,
            name=record.name,
            level=record.levelname,
            filename=record.filename,
            message=record.msg % record.args,
            timestamp=datetime.datetime.utcnow().isoformat()
        )
        if record.exc_info:
            data['traceback'] = self.formatException(record.exc_info)
        return json.dumps(data)

class UTF8Formatter(logging.Formatter):
    """Encode all Unicode output to UTF-8 to prevent encoding errors."""
    def format(self, record):
        data = super(UTF8Formatter, self).format(record)
        if isinstance(data, unicode):
            data = data.encode("utf8")
        return data

log_level = os.environ.get('SIMPLIFIED_LOG_LEVEL', 'INFO').upper()
logging.basicConfig(format=DEFAULT_FORMAT)

def set_formatter(handler):
    log_format = os.environ.get(
        'SIMPLIFIED_LOG_FORMAT', DEFAULT_LOG_DATA_FORMAT).lower()    
    if log_format=='json':
        cls = JSONFormatter
    else:
        cls = UTF8Formatter
    handler.setFormatter(cls(DEFAULT_FORMAT))
    return handler

logger = logging.getLogger()
logger.setLevel(log_level)
for handler in logger.handlers:
    set_formatter(handler)

database_log_level = os.environ.get('SIMPLIFIED_DATABASE_LOG_LEVEL', 'WARN')
for logger in (
        'sqlalchemy.engine', 'elasticsearch', 
        'requests.packages.urllib3.connectionpool'
):
    logging.getLogger(logger).setLevel(database_log_level)
