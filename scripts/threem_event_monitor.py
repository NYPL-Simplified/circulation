import os
import site
import sys
import datetime
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.threem import (
    ThreeMEventMonitor,
)
from model import production_session

DEFAULT_START_TIME = datetime.datetime(2012, 8, 15)

if __name__ == '__main__':
    session = production_session()
    ThreeMEventMonitor(session, DEFAULT_START_TIME).run(session)
