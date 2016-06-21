from httplib import HTTPConnection
from urllib import quote
from util import *

MAX_RESPONSE_LENGTH = 200

def qstr(key, value=None):
    #t = time.time()
    if value is None:
        return '?key=' + quote(key) 
    #elapsed = time.time()-t
    return '?key=' + quote(key) + '&value=' + quote(value)

def bstr(key, value=None):
    if value is not None:
        return 'key=' + quote(key) + '&value=' + quote(value)
    return 'key=' + quote(key)

def insert(http_conn, key, value):
    http_conn.request('POST', INSERT_PATH, bstr(key, value))
    r = http_conn.getresponse()
    rstr = r.read(MAX_RESPONSE_LENGTH)
    return json.loads(rstr.decode('utf-8'))

def get(http_conn, key):
    http_conn.request('GET', GET_PATH + qstr(key))
    r = http_conn.getresponse()
    rstr = r.read(MAX_RESPONSE_LENGTH)
    return json.loads(rstr.decode('utf-8'))
    
def update(http_conn, key, value):
    http_conn.request('POST', UPDATE_PATH, bstr(key, value))
    r = http_conn.getresponse()
    rstr = r.read(MAX_RESPONSE_LENGTH)
    return json.loads(rstr.decode('utf-8'))
    
def delete(http_conn, key):
    http_conn.request('POST', DELETE_PATH, bstr(key))
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

print insert(http_conn, 'a', '1')