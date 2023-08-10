import multiprocessing
import os
from pathlib import Path

VENV_ACTUAL = Path("/simplified_venv")
SIMPLIFIED_HOME = os.environ.get("SIMPLIFIED_HOME", "/home/simplified/circulation")

# Shared Settings
wsgi_app = "api.app:app"
accesslog = "/var/log/gunicorn/access.log"
errorlog = "/var/log/gunicorn/error.log"
loglevel = "info"
limit_request_line = 4094   # max size of HTTP request line, in bytes
limit_request_fields = 100  # max number of header fields allowed in a request
limit_request_field_size = 8190  # allowed size of a single HTTP header field
preload_app = False         # defer app load till after worker start
chdir = SIMPLIFIED_HOME  # change to this dir before loading apps
daemon = False              # Don't background the process
user = "simplified"
group = "simplified"
bind = ["127.0.0.1:8000"]     # listen on 8000, only on the loopback address
workers = (2 * multiprocessing.cpu_count()) + 1
threads = 2
max_requests = 2000
max_requests_jitter = 100
pythonpath = ",".join([
    str(VENV_ACTUAL),
    SIMPLIFIED_HOME,
])

# Env-Specific Settings

if os.environ.get('FLASK_ENV', None) == 'development':
    reload = True       # restart workers when app code changes
    loglevel = "debug"  # default loglevel is 'info'
    workers = 1         # single worker for local dev
    threads = 1         # single thread for local dev
