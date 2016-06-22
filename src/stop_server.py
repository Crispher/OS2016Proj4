from httplib import HTTPConnection
from util import *

for host in HOSTS_LIST:
  http_conn = HTTPConnection(host, PORT)
  http_conn.request('GET', SHUTDOWN_PATH)
