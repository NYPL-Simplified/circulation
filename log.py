from nose.tools import set_trace
import datetime
import logging
import json
import os
import socket
from config import Configuration

if not Configuration.instance:
    Configuration.load()

DEFAULT_DATA_FORMAT = "%(asctime)s:%(name)s:%(levelname)s:%(filename)s:%(message)s"

class JSONFormatter(logging.Formatter):
    hostname = socket.gethostname()
    def format(self, record):
        try:
            message = record.msg % record.args
        except TypeError, e:
            if record.args:
                raise e
            else:
                message = record.msg
        data = dict(
            host=self.hostname,
            name=record.name,
            level=record.levelname,
            filename=record.filename,
            message=message,
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

log_config = Configuration.logging_policy()
log_level = log_config.get(Configuration.LOG_LEVEL, 'INFO').upper()
data_format = log_config.get(
    Configuration.LOG_DATA_FORMAT, DEFAULT_DATA_FORMAT)
logging.basicConfig(format=data_format)

def set_formatter(handler):
    output_type = log_config.get(Configuration.LOG_OUTPUT_TYPE, 'text').lower()
    if output_type=='json':
        cls = JSONFormatter
    else:
        cls = UTF8Formatter
    handler.setFormatter(cls(data_format))
    return handler

logger = logging.getLogger()
logger.setLevel(log_level)
for handler in logger.handlers:
    set_formatter(handler)

database_log_level = log_config.get(Configuration.DATABASE_LOG_LEVEL, 'WARN')
for logger in (
        'sqlalchemy.engine', 'elasticsearch', 
        'requests.packages.urllib3.connectionpool'
):
    logging.getLogger(logger).setLevel(database_log_level)
