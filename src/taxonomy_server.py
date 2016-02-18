from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
from urllib.parse import urlparse, parse_qs
import json
from taxonomydb import *
import conf


class TaxonomyHTTPServer(HTTPServer):
    def process_request(self, request, address):
        thread = threading.Thread(target=self._new_request,
                                  args=(TaxonomyRequestHandler, request,
                                        address, self))
        thread.start()

    def _new_request(self, handlerClass, request, address, server):
        handlerClass(request, address, server)
        self.shutdown_request(request)


class TaxonomyRequestHandler(BaseHTTPRequestHandler):
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
        if 'id' in query:
            if 'taxonomy' in query:
                self._error('Request can\'t contain both `id` and `taxonomy`'
                            ' parameters.')
                return
            txdb = TaxonomyDB(conf.DATOIN2_DB)
            results = txdb.getTaxonomies(query['id'])

            self._reply({'messages' : 'OK',
                         'results'  : results})

        elif 'taxonomy' in query:
            if 'id' in query:
                self._error('Request can\'t contain both `id` and `taxonomy`'
                            ' parameters.')
                return
            nusers = query.get('nusers', 10)
            randomize = query.get('randomize', 'true') != 'false'
            
            txdb = TaxonomyDB(conf.DATOIN2_DB)
            results = txdb.getUsers(query['taxonomy'], nusers, randomize)

            self._reply({'messages' : 'OK',
                         'results'  : results})

        else:
            self._error('Request must contain either `id` or `taxonomy` '
                        ' parameters.')            

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

    port = 81
    if len(argv) == 2:
        port=int(argv[1])

    httpd = TaxonomyHTTPServer(('', port), TaxonomyRequestHandler)
    print('starting server')
    httpd.serve_forever()
    
