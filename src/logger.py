import sys

class Logger:
    def __init__(self, logstream=sys.stdout):
        self._logstream = logstream

    def log(self, msg):
        if self._logstream is not None:
            self._logstream.write(msg)
            self._logstream.flush()

