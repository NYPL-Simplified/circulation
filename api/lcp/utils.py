from core.lcp.exceptions import LCPError


def format_datetime(datetime_value):
    """Converts a datetime value into a string using the format which Go understands

    :param datetime_value: Datetime value
    :type datetime_value: datetime.datetime

    :return: String representation of the datetime value
    :rtype: string
    """
    datetime_string_value = datetime_value.strftime('%Y-%m-%dT%H:%M:%S')

    # NOTE: Go can parse only strings where the timezone contains a colon (e.g., -07:00)
    # Unfortunately, Python doesn't support such format and we have to do it manually
    # We assume that all the dates are in UTC
    datetime_string_value += '+00:00'

    return datetime_string_value


def get_target_extension(input_extension):
    if input_extension == '.epub':
        target_extension = '.epub'
    elif input_extension == '.pdf':
        target_extension = '.lcpdf'
    elif input_extension == '.lpf':
        target_extension = ".audiobook"
    elif input_extension == '.audiobook':
        target_extension = ".audiobook"
    else:
        raise LCPError('Unknown extension "{0}"'.format(input_extension))

    return target_extension


def bind_method(instance, func, as_name=None):
    """Bind the function *func* to *instance*, with either provided name *as_name*
    or the existing name of *func*. The provided *func* should accept the
    instance as the first argument, i.e. "self".
    """
    if as_name is None:
        as_name = func.__name__

    bound_method = func.__get__(instance, instance.__class__)
    setattr(instance, as_name, bound_method)

    return bound_method
