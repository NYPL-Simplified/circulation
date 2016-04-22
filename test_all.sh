# PYTHONWARNINGS=ignore suppresses SQLAlchemy and other warnings.
# --nocapture lets you output to stdout, while the tests is still running (arguably more useful while debugging a single test, not running the whole batch).
# --detailed-errors attempts a more detailed stack trace, but doesn't actually work.

PYTHONWARNINGS=ignore nosetests --nocapture --detailed-errors 




