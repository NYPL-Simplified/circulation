import os
import site
import sys
import datetime
d = os.path.split(__file__)[0]
site.addsitedir(os.path.join(d, ".."))
from integration.threem import (
    ThreeMCirculationMonitor,
)
from model import production_session


if __name__ == '__main__':
    session = production_session()
    ThreeMCirculationMonitor(session).run(session)
