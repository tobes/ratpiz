class Fail(BaseException):
    """
    Raised when a task wishes to fail
    """
    pass


class Retry(BaseException):
    """
    Raised when a task wishes to retry
    """
    pass
