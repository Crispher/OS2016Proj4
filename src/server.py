class Operation:
  def __init__(self, type, key, value=None):
    executed = False
    pass
  def done(self):
    executed = True
        
class MyHTTPRequestHandler(BaseHTTPServer.BaseHTTPReqestHandler):
  # these functions runs in a seperate thread from the server thread
  def __init__(self):
    BaseHTTPReqestHandler.__init__(self)
    self.kv_paxos_server = server
  
  def do_HEAD(self):
    pass
  
  def do_GET(self):
    # wrap an operation and call paxos 
    # get the returned value from paxos protocol
    op = wrap(request)
    1:
    seq = server.px.Max() + 1
    server.px.start(, op)
    sleep_time = 0.01
    while server.px.status(seq) != 'ready':
      time.sleep(sleep_time)
      time = max(time*2, 1)
    if v != op:
      goto 1
    ret = server.execute(op)
    gen_response()
    pass
    
  def do_POST(self):
    pass
    
  def _error_handle(self, e):
    pass
        
class KvPaxosServer:
  def __init__(self, config):
    self.px = Paxos.Make()
    self.http_server = BaseHTTPServer.HTTPServer(
        (HTTP_HOST_NAME, int(HTTP_PORT_NUMBER)), MyHTTPRequestHandler)
    self.kvstore = Store()
    self.keep_running = True
    
    # do not modify this value outside the class
    # lock_required to access this value 
    self.executed_paxos_no = -1
    
  def start():
    while self.keep_running:
      # handle http request
    exit
    
  def execute(self, opertion):
    with lock do this:
      self.executed_paxos_no += 1
      pass
  # various handlers
  
  def handle_insert(self, key, value):

  def handle_dump(self):
  
  def handle_countkey(self):
  
  def handle_shutdown(self):