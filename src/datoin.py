import conf
import requests
from requests.exceptions import ConnectionError, ChunkedEncodingError, \
    ReadTimeout
from urllib.parse import urlencode
import sys
import time
from logger import Logger
from copy import deepcopy


class Session:
    def __init__(self, logger=Logger(None)):
        self._session = requests.Session()
        self._logger = logger
        self.get = self._session.get

    def count(self,
              params={},
              url=conf.DATOIN3_SEARCH):
        params = deepcopy(params)
        params['rows'] = 0
        r = self._session.get(url, params=params).json()
        if 'totalResults' not in r:
            raise RuntimeError('Invalid reply: '+repr(r))
        return r['totalResults']

    def query(self,
              params={},
              url=conf.DATOIN3_SEARCH,
              batchsize=1000,
              maxdelay=1,
              timeout=300):
        maxdelay = max(1, maxdelay)
        next_page = None
        last_page = False
        params = deepcopy(params)
        params['rows'] = batchsize

        while not last_page:
            if next_page:
                params['nextPage'] = next_page

            delay = 1
            while True:
                try:
                    request_url = '?'.join([url, urlencode(params)])
                    r = self._session.get(url,
                                          params=params,
                                          timeout=timeout)
                    r = r.json()
                    if 'results' not in r:
                        raise IOError('Invalid reply: '+repr(r))
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
            last_page = 'nextPageId' not in r
            if not last_page:
                next_page = r['nextPageId']

            for d in r['results']:
                yield d

def count(params={}, url=conf.DATOIN3_SEARCH, logger=Logger(None)):
    session = Session(logger=logger)
    return session.count(params=params, url=url)

def query(params={},
          url=conf.DATOIN3_SEARCH,
          batchsize=1000,
          maxdelay=1,
          timeout=300,
          logger=Logger(None)):
    session = Session(logger=logger)
    for row in session.query(params=params, url=url,
                             rows=rows, offset=offset, batchsize=batchsize,
                             maxdelay=maxdelay):
        yield row

