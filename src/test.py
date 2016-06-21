from httplib import HTTPConnection
from urllib import quote
from util import *

MAX_RESPONSE_LENGTH = 200

def qstr(requestid, key, value=None):
    #t = time.time()
    if value is None:
        return '?key=' + quote(key) + '&requestid=' + quote(str(requestid))
    #elapsed = time.time()-t
    return '?key=' + quote(key) + '&value=' + quote(value) + '&requestid=' + quote(str(requestid))

def bstr(requestid, key, value=None):
    if value is not None:
        return 'key=' + quote(key) + '&value=' + quote(value) + '&requestid=' + quote(str(requestid))
    return 'key=' + quote(key) + '&requestid=' + quote(str(requestid))

def insert(http_conn, requestid, key, value):
    http_conn.request('POST', INSERT_PATH, bstr(requestid, key, value))
    r = http_conn.getresponse()
    rstr = r.read(MAX_RESPONSE_LENGTH)
    return json.loads(rstr.decode('utf-8'))

def get(http_conn, requestid, key):
    http_conn.request('GET', GET_PATH + qstr(requestid, key))
    r = http_conn.getresponse()
    rstr = r.read(MAX_RESPONSE_LENGTH)
    return json.loads(rstr.decode('utf-8'))
    
def update(http_conn, key, value):
    http_conn.request('POST', UPDATE_PATH, bstr(requestid, key, value))
    r = http_conn.getresponse()
    rstr = r.read(MAX_RESPONSE_LENGTH)
    return json.loads(rstr.decode('utf-8'))
    
def delete(http_conn, key):
    http_conn.request('POST', DELETE_PATH, bstr(requestid, key))
    r = http_conn.getresponse()
    rstr = r.read(MAX_RESPONSE_LENGTH)
    return json.loads(rstr.decode('utf-8'))

def countkey(http_conn):
    http_conn.request('GET', COUNT_PATH)
    r = http_conn.getresponse()
    rstr = r.read(MAX_RESPONSE_LENGTH)
    return json.loads(rstr.decode('utf-8'))

def dump(http_conn):
    http_conn.request('GET', DUMP_PATH)
    r = http_conn.getresponse()
    rstr = r.read(MAX_RESPONSE_LENGTH)
    return json.loads(rstr.decode('utf-8'))
    
def shutdown(http_conn):
    http_conn.request('GET', SHUTDOWN_PATH)
    http_conn.getresponse()
    return



http_conn = HTTPConnection(HOST, PORT)
# print dump(http_conn)
# print countkey(http_conn)['result']
# print shutdown(http_conn)

print insert(http_conn, 123, 'a', '1')
print get(http_conn, 456, 'a')

http_conn.close()

for host in HOSTS_LIST:
  http_conn = HTTPConnection(host, PORT)
  shutdown(http_conn)
