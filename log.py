from nose.tools import set_trace
import datetime
import logging
import json
import os
import socket
from config import Configuration
from StringIO import StringIO

if not Configuration.instance:
    Configuration.load()

DEFAULT_DATA_FORMAT = "%(asctime)s:%(name)s:%(levelname)s:%(filename)s:%(message)s"

class JSONFormatter(logging.Formatter):
    hostname = socket.gethostname()
    fqdn = socket.getfqdn()
    if len(fqdn) > len(hostname):
        hostname = fqdn
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
            app="simplified",
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
        try:
            data = super(UTF8Formatter, self).format(record)
        except Exception, e:
            data = super(UTF8Formatter, self).format(record)
        if isinstance(data, unicode):
            data = data.encode("utf8")
        return data

class LogglyAPI(object):

    @classmethod
    def handler(cls, log_level):
        integration = Configuration.integration('logg.ly', required=True)
        token = integration['token']
        url = integration['url'] % dict(token=token)
        from loggly.handlers import HTTPSHandler
        return HTTPSHandler(url)
       

log_config = Configuration.logging_policy()
log_level = log_config.get(Configuration.LOG_LEVEL, 'INFO').upper()

output_type = log_config.get(Configuration.LOG_OUTPUT_TYPE, 'text').lower()
if output_type == 'logg.ly':
    logging.getLogger().addHandler(LogglyAPI.handler(log_level))

data_format = log_config.get(
    Configuration.LOG_DATA_FORMAT, DEFAULT_DATA_FORMAT)
stderr_handler = logging.StreamHandler()
logging.getLogger().addHandler(stderr_handler)

def set_formatter(handler):
    output_type = log_config.get(Configuration.LOG_OUTPUT_TYPE, 'text').lower()
    if output_type in ('json', 'logg.ly'):
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
