from httplib import HTTPConnection
from util import *

http_conn = HTTPConnection(HOST, PORT)
http_conn.request('GET', SHUTDOWN_PATH)
r = http_conn.getresponse()
http_conn.close()


# for host in HOSTS_LIST:
#   http_conn = HTTPConnection(host, PORT)
#   http_conn.request('GET', SHUTDOWN_PATH)
