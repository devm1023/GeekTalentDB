import conf
import requests
from requests.exceptions import ConnectionError
import sys
from logger import Logger
import time


logger = Logger(None)


class Session:
    def __init__(self):
        self._session = requests.Session()

    def count(self,
              params={},
              url=conf.DATOIN_SEARCH):
        params['rows'] = 0
        params['start'] = 0
        r = self._session.get(url, params=params).json()
        if 'totalResults' not in r:
            raise RuntimeError('Invalid reply: '+repr(r))
        return r['totalResults']

    def query(self,
              params={},
              url=conf.DATOIN_SEARCH,
              rows=None,
              offset=0,
              batchsize=100,
              maxdelay=60):
        maxdelay = max(1, maxdelay)

        curr_offset=offset
        if rows is not None:
            max_offset = offset+rows
        else:
            max_offset = None

        while True:
            if max_offset is not None:
                if curr_offset >= max_offset:
                    break
                curr_batchsize = min(batchsize, max_offset-curr_offset)
            else:
                curr_batchsize = batchsize
            curr_params = params.copy()
            curr_params['start'] = curr_offset
            curr_params['rows'] = curr_batchsize

            logger.log('Requesting data...')
            delay = 1
            while True:
                try:
                    r = self._session.get(url, params=curr_params).json()
                    if 'results' not in r:
                        raise RuntimeError('Invalid reply: '+repr(r))
                    break
                except (RuntimeError, ConnectionError):
                    if delay >= maxdelay:
                        raise
                    time.sleep(delay)
                    delay *= 2
            logger.log('done.\n')
            if not r['results']:
                break
            else:
                for d in r['results']:
                    yield d

            curr_offset += batchsize


def count(params={}, url=conf.DATOIN_SEARCH):
    session = Session()
    return session.count(params=params, url=url)

def query(params={},
          url=conf.DATOIN_SEARCH,
          rows=None,
          offset=0,
          batchsize=100,
          maxdelay=60):
    session = Session()
    for row in session.query(params=params, url=url,
                             rows=rows, offset=offset, batchsize=batchsize,
                             maxdelay=maxdelay):
        yield row

