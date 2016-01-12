import conf
import requests
from requests.exceptions import ConnectionError, ChunkedEncodingError, \
    ReadTimeout
from urllib.parse import urlencode
import sys
import time
from logger import Logger


class Session:
    def __init__(self, logger=Logger(None)):
        self._session = requests.Session()
        self._logger = logger
        self.get = self._session.get

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
              maxdelay=1,
              timeout=300):
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

            delay = 1
            while True:
                try:
                    request_url = '?'.join([url, urlencode(curr_params)])
                    r = self._session.get(url,
                                          params=curr_params,
                                          timeout=timeout)
                    r = r.json()
                    if 'results' not in r:
                        raise RuntimeError('Invalid reply: '+repr(r))
                    break
                except (RuntimeError, \
                        ConnectionError, \
                        ChunkedEncodingError,
                        ReadTimeout) as e:
                    self._logger.log('URL: '+request_url+'\n')
                    self._logger.log(str(e)+'\n')
                    if delay > maxdelay:
                        raise
                    time.sleep(delay)
                    delay *= 2
            if not r['results']:
                break
            else:
                for d in r['results']:
                    yield d

            curr_offset += batchsize


def count(params={}, url=conf.DATOIN_SEARCH, logger=Logger(None)):
    session = Session(logger=logger)
    return session.count(params=params, url=url)

def query(params={},
          url=conf.DATOIN_SEARCH,
          rows=None,
          offset=0,
          batchsize=100,
          maxdelay=1,
          timeout=300,
          logger=Logger(None)):
    session = Session(logger=logger)
    for row in session.query(params=params, url=url,
                             rows=rows, offset=offset, batchsize=batchsize,
                             maxdelay=maxdelay):
        yield row

