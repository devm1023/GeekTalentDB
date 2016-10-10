__all__ = ['new_identity', 'TorProxyList']

import subprocess
import os
import select
from datetime import datetime
import time
from logger import Logger

from stem import Signal
from stem.control import Controller


def new_identity(port=9051, password=''):
    with Controller.from_port(port=port) as controller:
        controller.authenticate(password=password)
        controller.signal(Signal.NEWNYM)


def _read_more(stream, encoding='utf-8', timeout=0, line_prefix=''):
    result = ''
    while select.select([stream], [], [], timeout)[0]:
        result += line_prefix + stream.readline().decode(encoding)
    return result


class TorProxyList:
    def __init__(self, nproxies, base_port=13000, data_dir='tordata',
                 hashed_password='', restart_after=60, max_restart=3,
                 logger=Logger(None)):
        self.nproxies = nproxies
        self.ports = [base_port + 2*i for i in range(nproxies)]
        self.control_ports = [base_port + 2*i + 1 for i in range(nproxies)]
        self._procs = []

        try:
            # start Tor processes
            for i in range(nproxies):
                cmd = ['tor',
                       '--CookieAuthentication', '0',
                       '--HashedControlPassword', hashed_password,
                       '--ControlPort', str(self.control_ports[i]),
                       '--SocksPort', str(self.ports[i]),
                       '--DataDirectory', os.path.join(data_dir, 'tor.'+str(i))]
                self._procs.append(subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                                    bufsize=0))

            # wait for processes to get ready
            ready = [False]*nproxies
            laststart = datetime.now()
            restart_count = 0
            while not all(ready):
                if restart_count >= max_restart:
                    raise RuntimeError('Some Tor processes failed to start.')
                time.sleep(1)
                now = datetime.now()
                restart = (now - laststart).total_seconds() > restart_after
                for i in range(nproxies):
                    if ready[i]:
                        continue
                    output = _read_more(self._procs[i].stdout,
                                        line_prefix='PROC {0:d}: '.format(i))
                    if output:
                        logger.log(output) 
                    if output.find(
                            'Tor has successfully opened a circuit. '
                            'Looks like client functionality is working') != -1:
                        ready[i] = True
                    elif restart:
                        logger.log('RESTARTING PROCESS {0:d}.\n'.format(i))
                        self._procs[i].kill()
                        cmd = ['stdbuf', '-o0', 'tor',
                               '--CookieAuthentication', '0',
                               '--HashedControlPassword', '',
                               '--ControlPort', str(self.control_ports[i]),
                               '--SocksPort', str(self.ports[i]),
                               '--DataDirectory', os.path.join(data_dir,
                                                               'tor.'+str(i))]
                        self._procs[i] \
                            = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                               bufsize=0)
                if restart:
                    restart_count += 1
                    laststart = now
        except:
            self.kill()
            raise

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.kill()
        return False

    def kill(self):
        for proc in self._procs:
            proc.kill()


if __name__ == '__main__':
    logger = Logger()

    with TorProxyList(10, logger=logger, restart_after=120) as tor_proxies:
        print('All Tor processes started.')

