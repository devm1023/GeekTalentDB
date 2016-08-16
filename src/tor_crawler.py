import conf
from crawler import Crawler
from tor import TorProxyList, new_identity
from logger import Logger
import re
import argparse

class TorCrawler(Crawler):
    def __init__(self, site, nproxies=1, tor_base_port=13000, tor_timeout=60,
                 tor_retries=3, **kwargs):
        Crawler.__init__(self, site, **kwargs)
        self.add_config(nproxies=nproxies,
                        tor_base_port=tor_base_port,
                        tor_timeout=tor_timeout,
                        tor_retries=tor_retries)
        proxies = []
        for i in range(nproxies):
            proxy = 'socks5://127.0.0.1:{0:d}'.format(tor_base_port+2*i)
            proxies.append((proxy, proxy))
        self.set_config(proxies=proxies)
        self.tor_proxies = None

    @classmethod
    def on_visit(cls, iproxy, proxy, proxy_state, valid):
        if not valid:
            port = int(proxy.split(':')[1])
            new_identity(port=port+1, password=conf.TOR_PASSWORD)

    def init_proxies(self, config):
        self.tor_proxies \
            = TorProxyList(config['nproxies'],
                           base_port=config['tor_base_port'],
                           restart_after=config['tor_timeout'],
                           max_restart=config['tor_retries'],
                           hashed_password=conf.TOR_HASHED_PASSWORD)
        config['logger'].log('Tor proxies started.\n')
        return [None]*config['nproxies']

    def finish_proxies(self, config, proxy_states):
        self.tor_proxies.kill()
        self.tor_proxies = None
        
    def on_timeout(self, config, proxy_states):
        self.finish_proxies(config, proxy_states)
        return self.init_proxies(self, config)
        
