from datetime import tzinfo, timedelta, datetime

ZERO = timedelta(0)
UNIX_EPOC = datetime(1970, 1, 1)


class _UTC(tzinfo):
    """
    A UTC tzinfo.  Helpful for datetimes
    """

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO


_utc = _UTC()
del _UTC


def to_unix_time(datetime_):
    # python 3.3+ return datetime_.timestamp()
    return (datetime_ - UNIX_EPOC).total_seconds()


def datetime_from_timestamp(datetime_):
    """
    utc datetime from timestamp
    """
    return datetime.fromtimestamp(datetime_, _utc)
