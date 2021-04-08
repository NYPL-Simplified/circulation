import datetime
import pytz

# datetime helpers
# As part of the python 3 conversion, the datetime object went through a
# subtle update that changed how UTC works. Find more information here:
# https://blog.ganssle.io/articles/2019/11/utcnow.html
# https://docs.python.org/3/library/datetime.html#aware-and-naive-objects

def datetime_utc(*args, **kwargs):
    """Return a datetime object but with UTC information from pytz.
    :return: datetime object
    """
    return datetime.datetime(*args, **kwargs, tzinfo=pytz.UTC)

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

    :type dt: datetime.datetime
    :return: datetime object, or None if `dt` was None.
    """
    if dt is None:
        return None
    return dt.replace(tzinfo=pytz.UTC)

def strptime_utc(date_string, format):
    """Parse a string that describes a time but includes no timezone,
    into a timezone-aware datetime object set to UTC.

    :raise ValueError: If `format` expects timezone information to be
        present in `date_string`.
    """
    if '%Z' in format or '%z' in format:
        raise ValueError(
            "Cannot use strptime_utc with timezone-aware format {}".format(
                format
            )
        )
    return to_utc(datetime.datetime.strptime(date_string, format))
