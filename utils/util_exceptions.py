__author__ = 'Andrew Campbell'

class UtilError(Exception):
    pass

class MissingParamError(UtilError):
    """
    Used when function called with missing paramters
    """
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)
