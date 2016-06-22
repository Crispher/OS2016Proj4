import random
import string
import threading
from threading import Lock
from urllib import quote
from httplib import HTTPConnection
import time
from util import *
from store import Store


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

def paxos_kill(http_conn):
    http_conn.request('GET', PAXOS_KILL_PATH)
    http_conn.getresponse()
    return

def paxos_resurrect(http_conn):
    http_conn.request('GET', PAXOS_RESURRECT_PATH)
    http_conn.getresponse()
    return


# print dump(http_conn)
# print countkey(http_conn)['result']
# print shutdown(http_conn)

# print insert(http_conn, 123, 'a', '1')
# print get(http_conn, 456, 'a')

conn_list = [HTTPConnection(host, PORT) for host in HOSTS_LIST]

def multiput(func, *args):
  c1 = []
  for host in HOSTS_LIST:
    while True:
      try:
        c1 += [HTTPConnection(host, PORT)]
        break
      except:
        print e
  ans = func(c1[0], *args)
  for c in c1:
    if func(c, *args) == ans:
      continue
    assert(False)
  for c in c1:
    c.close()
  return ans

import random
class _R():
  def __init__(self):
    self._ID = -1
    self.lock = Lock()
  def get_id(self):
    with self.lock:
      self._ID += 1
      return self._ID
  def get_random_host(self):
    return HTTPConnection(HOSTS_LIST[random.randrange(n_hosts)], PORT)
R = _R()

# print multiput(insert, R.get_id(), 'a', '1')
# paxos_kill(conn_list[2])
# # print get(R.get_random_host(), R.get_id(), 'a')
# print get(conn_list[1], R.get_id(), 'a')
# print 'resu-'
# paxos_resurrect(conn_list[2])
# print 'rrect'
# # print delete(conn_list[1], R.get_id(), 'a')
# print multiput(delete, R.get_id(), 'a')


class client_thread(threading.Thread):
    def __init__(self, name, run_func, *args):
        threading.Thread.__init__(self)
        self.name = name
        self.run_func = run_func
        self.args = args
    def run(self):
        result = self.run_func(*self.args)
        self.result = result

def test2(num = 0, amount = 1000):
    # serial pressure test
    succ = 1
    insert_time = []
    insert_time_total = 0
    insert_success = 0
    get_time = []
    get_time_total = 0
    for i in range(1+num,amount+num+1):
        t = time.time()
        r = multiput(insert, R.get_id(), str(i), str(i+10000))
        elapsed_t = time.time() - t
        if r['success'] == 'false':
            succ = 0
        else:
            insert_success += 1
        insert_time.append(elapsed_t * 1000)
        insert_time_total = insert_time_total + elapsed_t * 1000
    for i in range(1+num,amount+num+1):
        r = multiput(update, R.get_id(), str(i), str(i+50000))
        if r['success'] == 'false':
            succ = 0
    for i in range(1+num,amount+num+1):
        t = time.time()
        r = multiput(get, R.get_id(), str(i))
        elapsed_t = time.time() - t
        if r['success'] == 'false':
            succ = 0
        get_time.append(elapsed_t * 1000)
        get_time_total = get_time_total + elapsed_t * 1000
    for i in range(1+num,amount+num+1):
        r = multiput(delete, R.get_id(), str(i))
        if r['success'] == 'false':
            succ = 0
    r = countkey(R.get_random_host())
    insert_time.sort()
    get_time.sort()
    return succ, insert_success, insert_time_total / amount, get_time_total / amount, insert_time[amount // 5], get_time[amount // 5], insert_time[amount // 2], get_time[amount // 2], insert_time[(amount * 7) // 10], get_time[(amount * 7) // 10], insert_time[(amount * 9) // 10], get_time[(amount * 9) // 10]
    

def test():
    test_num = 55
    count_1 = 0
    mutex = Lock()
    def single_create(num = 0):
        c = R.get_random_host()
        r = multiput(insert, R.get_id(), str(num), '1')
        # r = insert(c, R.get_id(), str(num), '1')
        c.close()

        if r['success'] == 'true':
            return 1
        else:
            return 0
    list1 = []

    r = delete(R.get_random_host(), R.get_id(), str(test_num))
    for i in range(1,10):
        list1.append(client_thread('t' + str(i), single_create, test_num))
    for i in range(1,10):
        list1[i - 1].start()
    for i in range(1,10):
        list1[i - 1].join()
        count_1 = count_1 + list1[i-1].result
    r = delete(R.get_random_host(), R.get_id(), str(test_num))
    
    t2 = client_thread('test2', test2, 0, 70)
    t2.start()
    t2.join()
    # print 'hhh'
    t2succ, iss, ia, ga, i2, g2, i5, g5, i7, g7, i9, g9 = t2.result
    
    if count_1 == 1 and t2succ == 1:
        print("Result:success")
    else:
        print("Result:fail")
    print("Insertion: " + str(iss) + " / 1000")
    print("Average latency (ms): " + str(ia) + " / " + str(ga))
    print("Percentile latency:" + str(i2) + " / " + str(g2) + ", " + str(i5) + " / " + str(g5) + ", " + str(i7) + " / " + str(g7) + ", " + str(i9) + " / " + str(g9))


import os
def start_server(arg):
    os.system('bin/')
t3 = client_thread('t3',test)
t3.start();
