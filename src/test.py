import random
import string
import threading
from urllib import quote
from httplib import HTTPConnection

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



http_conn = HTTPConnection(HOST, PORT)
# print dump(http_conn)
# print countkey(http_conn)['result']
# print shutdown(http_conn)

# print insert(http_conn, 123, 'a', '1')
# print get(http_conn, 456, 'a')

def check_return(msg, success, value=None):
  err_flag = success != msg.get('success', None) or \
      (value is not None and value != msg.get('value', None))
  if err_flag:
    log('[ERROR] msg', msg, success, value)
  return not err_flag




class Test:

  def __init__(self, store, http_conn, requestid):
    self.store = store
    self.http_conn = http_conn
    self.requestid = requestid

  def _random_string(self):
    return ''.join(random.choice(string.ascii_uppercase + string.digits)
                   for _ in range(4))

  def _get_keys_length(self):
    keys = self.store.store_dict.keys()
    return keys, len(keys)

  def _get_key(self, is_random=False):
    if is_random:
      return self._random_string(), self._random_string()
    else:
      keys, length = self._get_keys_length()
      return keys[random.randint(0, length - 1)] if length > 1 \
          else None, self._random_string()

  def decorator(func):
    def magic(self, id):
      k, v = self._get_key()
      flag, key, value = func(self, k, v)
      if flag == '':
        pass
      elif flag:
        log('[INFO]', '[' + str(id) + ']', func.__name__, '\t', key, value)
      elif not flag:
        log('[INFO]', '[' + str(id) + ']','\t', 'failed', '\t', key, value)
    return magic

  @decorator
  def test_insert(self, key, value, id=None):
    key, value = self._get_key(is_random=True)
    success, _ = self.store.insert(key, value)
    msg = insert(self.http_conn, self.requestid, key, value)
    return check_return(msg, str(success).lower()), key, value

  @decorator
  def test_update(self, key, value, id=None):
    if key is None:
      return '', '', ''

    success, _ = self.store.update(key, value)
    msg = update(self.http_conn, self.requestid, key, value)
    return check_return(msg, str(success).lower()), key, value

  @decorator
  def test_delete(self, key, value, id=None):
    if key is None:
      return '', '', ''

    success, value = self.store.delete(key)
    msg = delete(self.http_conn, self.requestid, key)
    return check_return(msg, str(success).lower(), value), key, value

  @decorator
  def test_get(self, key, value, id=None):
    if key is None:
      return '', '', ''

    success, value = self.store.get(key)
    msg = get(self.http_conn, self.requestid, key)
    return check_return(msg, str(success).lower(), value), key, value


def single_test(store, id, length):
  t = Test(store, http_conn, id)
  funcs = [t.test_insert, t.test_update, t.test_delete, t.test_get]
  for i in xrange(length):
    funcs[random.randint(0, 3)](i)


public_store = Store()


ps = [threading.Thread(target=single_test, args=(public_store, i, 10))
        for i in xrange(0, 30)]
for p in ps:
  p.start()
  p.join()
print public_store.store_dict

http_conn.close()

for host in HOSTS_LIST:
  http_conn = HTTPConnection(host, PORT)
  shutdown(http_conn)
