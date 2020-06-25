class ReadTimeout(Exception):
    pass
class FatalError(Exception):
    pass
class SynchronizationError(Exception):
    pass
class PayloadOverflow(Exception):
    pass
class ConnectionLost(Exception):
    pass