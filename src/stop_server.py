from httplib import HTTPConnection
from util import *

http_conn = HTTPConnection(HOST, PORT)
http_conn.request('GET', SHUTDOWN_PATH)
http_conn.getresponse()
