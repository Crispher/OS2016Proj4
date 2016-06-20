import BaseHTTPServer
from threading import Thread
from threading import Lock
from urlparse import parse_qs
from util import *
from store import Store
from kvpaxos import *
import sys


class Operation:
  def __init__(self, op_type, key, value=None):
    self.op_type = op_type
    self.key = key
    self.value = value
    self.executed = False

  def done(self, return_value):
    self.return_value = return_value
    self.executed = True
        
class MyHTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  # these functions runs in a seperate thread from the server thread
  def __init__(self):
    BaseHTTPRequestHandler.__init__(self)
    self.kv_paxos_server = server
  
  ''' propose a paxos event on value op, returns the seq that finally 
      currently uniqueness of each request is not taken care of.
  '''
  def paxos_consensus(self, op):
    # get the returned value from paxos protocol
    while True:
      seq = server.px.max() + 1
      server.px.start(op)
      sleep_time = 0.01
      decided, op_value = server.px.status(seq)
      while not decided:
        time.sleep(sleep_time)
        time = max(time*2, 1)
        decided, op_value = server.px.status(seq)
      if op_value == op:
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
      if self.path[:4] == '/kvm':
        if self.path == COUNT_PATH:
          cnt = self.kv_paxos_server.kvstore.countkey()
          self.wfile.write('{"result":"%d"}' % cnt)
        elif self.path == DUMP_PATH:
          pairs = self.kv_paxos_server.kvstore.dump()
          self.wfile.write(pairs)
        elif self.path == SHUTDOWN_PATH:
          self.kv_paxos_server.handle_shutdown()
        else:
          pass
        return
      else:
        key = self.path.split(GET_PATH + '?key=')[1]
        op = Operation('GET', key)
        assert (GET_PATH in self.path)
    except Exception, e:
      self._error_handle(e)

    # wrap an operation and call paxos 
    seq = paxos_consensus(op)
    success, value = self.server.execute(seq, op)
    self.wfile.write(generate_response(success, value))
    
  def do_POST(self):
    pass
    
  def _error_handle(self, e):
    pass
        
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
    self.executed_paxos_no = -1
    # contains all executed operations and their return values
    self.operation_log = []
    
  def start(self):
    log("HTTP Server Starts - %s:%s" % (HOST, PORT))
    while self.keep_running:
      self.http_server.handle_request()
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
      self.operation_log += [Operation(op)]
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
    log('shutting down')
    self.keep_running = False
    self.http_server.server_close()

  
server = KvPaxosServer()
server.start()