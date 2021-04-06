import datetime
import pytz

# datetime helpers
# As part of the python 3 conversion, the datetime object went through a
# subtle update that changed how UTC works. Find more information here:
# https://blog.ganssle.io/articles/2019/11/utcnow.html
# https://docs.python.org/3/library/datetime.html#aware-and-naive-objects

def datetime_utc(*args):
    """Return a datetime object but with UTC information from pytz.

    :return: datetime object
    """
    return datetime.datetime(*args, tzinfo=pytz.UTC)

def from_timestamp(ts):
    """Return a UTC datetime object from a timestamp.

    :return: datetime object
    """
    return datetime.datetime.fromtimestamp(ts, tz=pytz.UTC)

def utc_now():
    """Get the current time in UTC.

    :return: datetime object
    """
    return datetime.datetime.now(tz=pytz.UTC)

def to_utc(dt):
    """This converts a naive datetime object that represents UTC into
    an aware datetime object.

    :return: datetime object
    """
    return dt.replace(tzinfo=pytz.UTC)