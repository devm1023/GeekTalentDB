from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
from urllib.parse import urlparse, parse_qs
import json
from geekmaps_query import geekmapsQuery
from geekmapsdb import GeekMapsDB


class GeekMapsHTTPServer(HTTPServer):
    def __init__(self, address, handler, nutsids):
        HTTPServer.__init__(self, address, handler)
        self.nutsids = nutsids
    
    def process_request(self, request, address):
        thread = threading.Thread(target=self._new_request,
                                  args=(GeekMapsRequestHandler, request,
                                        address, self))
        thread.start()

    def _new_request(self, handlerClass, request, address, server):
        handlerClass(request, address, server)
        self.shutdown_request(request)


class GeekMapsRequestHandler(BaseHTTPRequestHandler):
    def _set_headers(self):
        query = urlparse(self.path).query
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin','*')
        self.end_headers()

    def _reply(self, jsonobj):
        self.wfile.write(json.dumps(jsonobj).encode('utf-8'))
        
    def _error(self, msg):
        self._reply({'messages' : msg})

    def do_GET(self):
        self._set_headers()
        query = parse_qs(urlparse(self.path).query)
        if 'type' not in query or 'query' not in query:
            self._error('Query must contain the parameters `type` and `query`.')
            return
        if len(query['type']) != 1:
            self._error('`type` parameter must appear exactly once.')
        if len(query['query']) != 1:
            self._error('`query` parameter must appear exactly once.')
        querytype = query['type'][0]
        querytext = query['query'][0]

        gmdb = GeekMapsDB(conf.GEEKMAPS_DB)
        counts, total = geekmapsQuery(querytype, querytext,
                                      gmdb, self.server.nutsids)
        if not counts:
            self._error('Invalid query.')
            return
        counts = [{'nutsId' : id, 'count' : count} \
                  for id, count in counts.items()]
        self._reply({'counts' : counts, 'total' : total, 'messages' : 'OK'})

    def do_HEAD(self):
        self._set_headers()
        
    def do_POST(self):
        # Doesn't do anything with posted data
        self._set_headers()
        self.wfile.write('POST METHOD')

        
if __name__ == '__main__':
    from sys import argv
    from nuts import NutsRegions
    import conf

    port = 8080
    if len(argv) == 2:
        port=int(argv[1])

    nuts = NutsRegions(conf.NUTS_DATA)
    nutsids = [id for id, shape in nuts.level(3)]
        
    httpd = GeekMapsHTTPServer(('', port), GeekMapsRequestHandler, nutsids)
    print('starting server')
    httpd.serve_forever()
    
