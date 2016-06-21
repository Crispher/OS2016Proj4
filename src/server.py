import BaseHTTPServer
from threading import Thread
from threading import Lock
from urlparse import parse_qs
from util import *
from store import Store
from kvpaxos import *
import sys
import time


class Operation:
  def __init__(self, requestid, op_type, key, value=None):
    self.requestid = requestid
    self.op_type = op_type
    self.key = key
    self.value = value
    self.executed = False
    self.return_value = False
    
  def done(self, return_value):
    self.return_value = return_value
    self.executed = True
    
  def __eq__(self, other):
    return self.requestid == other.requestid and  self.op_type == other.op_type and self.key == other.key and self.value == other.value
  
  def encode(self):
    try:
      s = {
            'requestid':self.requestid,
            'op_type':self.op_type, 
            'key':self.key,
            'value':self.value,
            'executed':self.executed,
            'return_value':self.return_value
          }
      print json.dumps(s)
      return json.dumps(s)
    except Exception, e:
      print 'encode error', e
  
  @staticmethod
  def decode(jstr):
    try:
      d = json.loads(jstr)
      op = Operation(d['requestid'], d['op_type'], d['key'], d['value'])
      op.executed = d['executed']
      op.return_value = d['return_value']
      return op
    except Exception, e:
      print 'decode error', e
class MyHTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  # these functions runs in a seperate thread from the server thread
  def __init__(self, *args):
    self.kv_server = kv_paxos_server
    BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, *args)
  
  ''' propose a paxos event on value op, returns the seq that finally 
      currently uniqueness of each request is not taken care of.
  '''
  def paxos_consensus(self, op):
    # get the returned value from paxos protocol
    print 'iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii'
    while True:
      seq = self.kv_server.px.max() + 1
      print 'sssssssssstttttttttttttaaaaaaaaaaaarrrrrrrrrrrrrrrrttttttttttttt', seq
      assert op is not None
      self.kv_server.px.start(seq, op.encode())
      sleep_time = 0.01
      decided, op_value = self.kv_server.px.status(seq)
      while not decided:
        time.sleep(sleep_time)
        sleep_time = max(sleep_time*2, 1)
        decided, op_value = self.kv_server.px.status(seq)
      if Operation.decode(op_value) == op:
        print 'break'
        break
    return seq
    
  def do_HEAD(self):
    self.send_response(200)
    self.send_header("Content-type", "text/html")
    self.end_headers()
    log('[INFO] do HEAD', self.path)
  
  def do_GET(self):
    self.do_HEAD()
    try:
      print self.path
      if self.path[:4] == '/kvm':
        if self.path == COUNT_PATH:
          cnt = self.kv_server.kvstore.countkey()
          self.wfile.write('{"result":"%d"}' % cnt)
        elif self.path == DUMP_PATH:
          log('dump')
          pairs = self.kv_server.kvstore.dump()
          self.wfile.write(pairs)
        elif self.path == SHUTDOWN_PATH:
          self.wfile.write('preparing to shutdown')
          self.kv_server.handle_shutdown()
        else:
          pass
        log('a')
        return
      else:
        print self.path[8:]
        args = parse_qs(self.path[8:])
        print 'qqq'
        key, requestid = args['key'][0], args['requestid'][0]
        op = Operation(requestid, 'GET', key)
        assert (GET_PATH in self.path)
        # wrap an operation and call paxos 
        seq = self.paxos_consensus(op)
        print 'seq=', seq
        success, value = self.kv_server.execute(seq)
        self.wfile.write(generate_response(success, value))
        return
    except Exception, e:
      print 'errrr'
      self._error_handle(e)
      return
    assert False
    
  def do_POST(self):
    self.do_HEAD()
    # self.wfile.write('{ \"result\" : \"not yet implemented\" }')
    try:
      length = int(self.headers.getheader('content-length'))
      field_data = self.rfile.read(length)
      fields = parse_qs(field_data)
      key, value, requestid = fields.get('key')[0], fields.get('value', [None])[0], fields.get('requestid')[0]
      if INSERT_PATH in self.path and value is not None:
        op = Operation(requestid, 'INSERT', key, value)
      elif UPDATE_PATH in self.path and value is not None:
        op = Operation(requestid, 'UPDATE', key, value)
      elif DELETE_PATH in self.path:
        op = Operation(requestid, 'DELETE', key)
      else:
        assert False
      print 'aaa'
      seq = self.paxos_consensus(op)
      print 'seq=', seq, 'decided', self.kv_server.px.status(seq)[0]
      success, value = self.kv_server.execute(seq)
      print success, value
      self.wfile.write(generate_response(success, value))
      
    except IOError, e:
      self._error_handle(e)
      
    
  def _error_handle(self, e):
    print e
        
class KvPaxosServer:
  def __init__(self):
    self.px = Paxos.make(HOSTS_LIST, ME)
    self.http_server = BaseHTTPServer.HTTPServer(
        (HOST, int(PORT)), MyHTTPRequestHandler)
    self.kvstore = Store()
    self.keep_running = True
    self.lock = Lock()
    
    # do not modify these 2 vars outside the execute() function
    # lock required to access these values. # maybe unnecessary 
    self.executed_paxos_no = 0
    # contains all executed operations and their return values
    self.operation_log = []
    
  def start(self):
    log("HTTP Server Starts - %s:%s" % (HOST, PORT))
    try:
      while self.keep_running:
        self.http_server.handle_request()
    except KeyboardInterrupt:
      print 'interrupted'
      sys.exit(0)
    print 'exits'
    self.http_server.server_close()
    sys.exit(0)
  
  ''' this function is only called by the handler class'''
  def execute(self, seq):
      
    # catch up. this ensures that operations are executed in ascending order
    while self.executed_paxos_no < seq:
      self.execute(self.executed_paxos_no)
    
    
    with self.lock:
      if seq < self.executed_paxos_no:
        # the operations is done by other threads already, check the log directly
        return operation_log[seq].return_value
      while True:
        decided, op_jstr = self.px.status(seq)
        print seq, decided
        if decided:
          break 
        else:
          time.sleep(1)
      op = Operation.decode(op_jstr)
      assert decided
      if op.op_type == 'GET':
        success, value = self.kvstore.get(op.key)
      elif op.op_type == 'INSERT':
        success, value = self.kvstore.insert(op.key, op.value)
      elif op.op_type == 'DELETE':
        success, value = self.kvstore.delete(op.key)
      elif op.op_type == 'UPDATE':
        success, value = self.kvstore.update(op.key, op.value)
      self.executed_paxos_no += 1
      self.px.done(seq)
      op.done((success, value))
      self.operation_log += [op]
      return success, value
      

  def handle_count(self):
    if self.store is None:
      self._update_store()
      if self.store is None:
        return False, "backup failed"
    return self.store.countkey()

  def handle_dump(self):
    if self.store is None:
      self._update_store()

    return self.store.dump()

  def handle_shutdown(self):
    self.keep_running = False
    

  
kv_paxos_server = KvPaxosServer()
kv_paxos_server.start()