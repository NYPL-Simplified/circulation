
# Pull in the session_fixture defined in core/testing.py
# which does the database setup and initialization
pytest_plugins = ["core.testing"]

try:
    import pydevd_pycharm
    pydevd_pycharm.settrace('localhost', port=8567, stdoutToServer=True, stderrToServer=True)
except Exception:
    pass