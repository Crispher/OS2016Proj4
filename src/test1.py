import random
import string
import threading
from urllib import quote
from httplib import HTTPConnection
import threading
from threading import Thread
import time
from util import *
from store import Store


MAX_RESPONSE_LENGTH = 200
class client_thread(threading.Thread):
    def __init__(self, name, run_func, *args):
        threading.Thread.__init__(self)
        self.name = name
        self.run_func = run_func
        self.args = args
    def run(self):
        result = self.run_func(*self.args)
        self.result = result
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
    
def update(http_conn, requestid, key, value):
    http_conn.request('POST', UPDATE_PATH, bstr(requestid, key, value))
    r = http_conn.getresponse()
    rstr = r.read(MAX_RESPONSE_LENGTH)
    return json.loads(rstr.decode('utf-8'))
    
def delete(http_conn, requestid, key):
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



conn_list = [HTTPConnection(host, PORT) for host in HOSTS_LIST]
def separate_update(server_number, identification_code, key, value):
  update(conn_list[server_number], identification_code, key, value)
def test1():
  print insert(conn_list[0], 1, 'a', '1')
  print get(conn_list[1], 2, 'a')
  t10 = client_thread('t10', separate_update, 0, 3, 'a', 'server0')
  t11 = client_thread('t11', separate_update, 1, 4, 'a', 'server1')
  t12 = client_thread('t12', separate_update, 2, 5, 'a', 'server2')
  t10.start()
  t11.start()
  t12.start()
  t10.join()
  t11.join()
  t12.join()
  print get(conn_list[0], 6, 'a')
  print get(conn_list[1], 7, 'a')
  print get(conn_list[2], 8, 'a')
t1 = client_thread('t1',test1)
t1.start()