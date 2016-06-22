import BaseHTTPServer
import xmlrpclib
import threading
from threading import Thread
from threading import Lock
from urlparse import urlparse, parse_qs
from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import random
from util import *
from store import Store
import sys
import time



class _RPCFuncs:  # Reused LPH's structure
  def __init__(self, server):
    self.server = server
  def test(self):
    return True
  def response_to_proposed_lead(self, seq, roundNum):  # Stage 1b in the slides
    # print 'server numbered ' + str(self.server.number) + ' responding to proposed lead'
    if self.server.alive is False:
      return False, "", -3, -1
    with self.server.seqLock.read_access:
      while (self.server.minNum + len(self.server.listValue) <= seq):
        self.server.listValue.append("")
        self.server.listRoundNum.append(-1)
        self.server.listPendingValue.append("")
        self.server.listPendingToLead.append(0)
      #print 'Actual length is ' + str(len(self.server.listValue))+ str(len(self.server.listRoundNum))+ str(len(self.server.listPendingValue))+ str(len(self.server.listPendingToLead))
      #print 'minimal is ' + str(self.server.minNum)
      if self.server.listPendingToLead[seq-self.server.minNum] is 2:
      # I'm trying to lead too
        #print "exit position 1"
        return False, "", -2, self.server.listKnownMin[self.server.number]
      #print 'under investigation is' + str(seq-self.server.minNum)
      if (roundNum <= self.server.listRoundNum[seq-self.server.minNum]):  # validity check of seq needed
      # the round number is old
        #print "exit position 2"
        return False, "", self.server.listRoundNum[seq-self.server.minNum], self.server.listKnownMin[self.server.number]
      #print 'under investigation is' + str(seq-self.server.minNum)
      if (self.server.listValue[seq-self.server.minNum] != ""):
      # seq is already decided, so there's no need for another round
        #print "exit position 3"
        return False, self.server.listValue[seq-self.server.minNum], -1, self.server.listKnownMin[self.server.number]
      #print 'under investigation is' + str(seq-self.server.minNum)
      returnRoundNum = self.server.listRoundNum[seq-self.server.minNum]
      self.server.listRoundNum[seq-self.server.minNum] = roundNum
      if self.server.listPendingValue[seq-self.server.minNum] is "":
        returnRoundNum = -1
      #print "exit position 4"
      return True, self.server.listPendingValue[seq-self.server.minNum], returnRoundNum, self.server.listKnownMin[self.server.number]
  def response_to_proposed_value(self, seq, roundNum, proposedVal): # Stage 2b in the slides
    if self.server.alive is False:
      return False, -1
    with self.server.seqLock.read_access:    # A lock here is necessary
      if (roundNum < self.server.listRoundNum[seq-self.server.minNum]):
      # the round number is old
        return False, self.server.listKnownMin[self.server.number]
      self.server.listPendingValue[seq-self.server.minNum] = proposedVal
      self.server.listRoundNum[seq-self.server.minNum] = roundNum
      return True, self.server.listKnownMin[self.server.number]
  def response_to_decision(self, seq, decidedVal):  # Stage 3 in the slides
    # Maybe adding some assertations?
    if self.server.alive is True:
      with self.server.seqLock.read_access:
        self.server.listValue[seq-self.server.minNum] = decidedVal
      return True
    else :
      return False
  def response_to_resurrection_query(self):
    return self.server.minNum, self.server.minNum + len(self.server.listValue) - 1
  def response_to_resurrection_result_query(self, num):
    # Add more information in response, so bugs should appear less often (or never)
    if (num - self.server.minNum >= len(self.server.listValue) or num < self.server.minNum):
      return "", "", -1
    else:
      return self.server.listValue[num - self.server.minNum], self.server.listPendingValue[num - self.server.minNum], self.server.listRoundNum[num - self.server.minNum]
def threadedDone(server):
  if server.alive is False:
    return
  with server.seqLock.write_access:  # the whole session should be wrapped in locks
    newMinNum = min(server.listKnownMin)
    dif = newMinNum - server.minNum
    if (dif < 0):
      raise Exception("Shouldn't reached here")
    elif (dif > 0):
      server.minNum = newMinNum
      for i in range(dif):  # Suppose this is executed for dif times
        del server.listValue[0]
        del server.listRoundNum[0]
        del server.listPendingValue[0]
        del server.listPendingToLead[0]
def threadedMaintainence(server):
  while (True):
    time.sleep(random.uniform(0.5,1))  # number of seconds slept over next check, I randomly picked "2" without further examination. In reality, this number should depend on the number of servers as well as network condition
    if server.alive is False:
      continue
    with server.seqLock.read_access:
      for i in range(len(server.listValue)):  # suppose this is executed for len(server.listValue) times
        if (server.listValue[i] == "" and server.listPendingValue[i] != ""):
          print "I'm working!!!!!!!!!!!"
          client_thread('tStart' + str(server.number) + ' ' 
              + str(i + server.minNum) + ' by maintainence', threadedStart, server, i + server.minNum).start()
          time.sleep(random.uniform(0.1,0.8))
def threadedResurrect(server):
  for i in range(0, server.amount):
    if server.checkListServer(i) is False:
      continue
    try:
      remoteMin, remoteMax = server.listServer[i].response_to_resurrection_query()
    except Exception, e:
      continue
    if remoteMin > server.listKnownMin[i]:
      server.listKnownMin[i] = remoteMin
    with server.seqLock.read_access:
      while (server.minNum + len(server.listValue) - 1 < remoteMax):
        server.listValue.append("")
        server.listRoundNum.append(-1)
        server.listPendingValue.append("")
        server.listPendingToLead.append(0)
      for j in range(remoteMin, remoteMax + 1):
        if j < server.minNum:
          continue
        if server.listValue[j - server.minNum] is "":
          remoteResult, remotePending, remoteNumber = server.listServer[i].response_to_resurrection_result_query(j)
          if (remoteResult != ""):
            server.listValue[j - server.minNum] = remoteResult
          elif (remoteNumber > server.listRoundNum[j - server.minNum]):
            server.listRoundNum[j - server.minNum] = remoteNumber
            server.listPendingValue[j - server.minNum] = remotePending
  """
  tDone = client_thread('tDone' + str(server.number), threadedDone, server)
  tDone.start()
  """
def threadedServerForever(server):
  server.serve_forever()
def threadedStart(server, seq):
  with server.seqLock.read_access:
    if (server.listPendingToLead[seq - server.minNum] > 0 or server.alive == False): 
      return  # full fix
    server.listPendingToLead[seq - server.minNum] = 2
    roundNum = server.listRoundNum[seq - server.minNum] + 1
  flag = True  # flag about whether to repeat
  while(flag):
    flag = False
    trueReply = 0
    highestPrevRound = -1
    prevRoundReply = ""
    for i in range(0, server.amount):
      """
      if (i == server.number):
        continue
      """  # not necessary, since this is done in checkListServer part
      # print "Trying to connect to server " + str(i)
      if server.checkListServer(i) is False:
        continue
      # print "Decided to connect to server " + str(i)
      try:
        """
        returnVal = server.listServer[i].response_to_proposed_lead(seq, roundNum)
        result = returnVal[0]
        prePropose = returnVal[1]
        returnRoundNum = returnVal[2]
        individualMin = returnVal[3]
        """
        result, prePropose, returnRoundNum, individualMin = server.listServer[i].response_to_proposed_lead(seq, roundNum)
      except Exception, e:
        # print "Exception returned from server " + str(i)
        continue  # in case remote server is unreachable, continue
      # except xmlrpclib.ProtocolError as err:
      #   print "A protocol error occurred"
      #   print "URL: %s" % err.url
      #   print "HTTP/HTTPS headers: %s" % err.headers
      #   print "Error code: %d" % err.errcode
      #   print "Error message: %s" % err.errmsg
      # print "Successfully got reply from server " + str(i)
      """
      if (individualMin > server.listKnownMin[i]):  # some remote server sends his Done message
        # print "Updating local min list"
        if server.listKnownMin[i] is server.minNum:
          server.listKnownMin[i] = individualMin
          tDone = client_thread('tDone' + str(server.number) + ' ' + str(individualMin), threadedDone, server)
          tDone.start()
        else:
          server.listKnownMin[i] = individualMin
      """
      if result is True:
        trueReply += 1
        if (returnRoundNum > highestPrevRound):
          if (prePropose == ""):
            # print str(returnRoundNum) + str(highestPrevRound)
            raise Exception("Shouldn't reached here")
          else:
            prevRoundReply = prePropose
            highestPrevRound = returnRoundNum
      else:
        if (prePropose != ""):
          with server.seqLock.read_access:
            server.listValue[seq - server.minNum] = prePropose
            server.listPendingToLead[seq - server.minNum] = 0
          return
        else:
          if returnRoundNum is -3:    # distant server currently dead
            continue
          elif returnRoundNum is -2:  # Somebody else is trying to lead
            """server.listPendingToLead[seq - server.minNum] = False
            return"""
            break  # I think this is a better solution
          else:
            roundNum = returnRoundNum + 1
            #server.listRoundNum[seq - server.minNum] = roundNum  # This shouldn't be here
            flag = True
            break
    if server.alive is False:
      with server.seqLock.read_access:
        server.listPendingToLead[seq - server.minNum] = 0
        return
  with server.seqLock.read_access:
    if ((trueReply + 1) * 2 <= server.amount):  
      server.listPendingToLead[seq - server.minNum] = 0
      return 
    else:
      server.listPendingToLead[seq - server.minNum] = 1
    # I'm OK to lead
    #print "I'm leading"
    #with server.seqLock:
    if (highestPrevRound > server.listRoundNum[seq - server.minNum]):
      proposedVal = prevRoundReply
      server.listPendingValue[seq - server.minNum] = proposedVal
    else:
      proposedVal = server.listPendingValue[seq - server.minNum]
    server.listRoundNum[seq - server.minNum] = roundNum
  trueReply = 0
  if proposedVal is "":
    assert False, "not reached"
    print "Serious trouble!!!!!!!!!!!!!!!!!!"
  for i in range(0, server.amount):
    """
    if (i == server.number):
      continue
    """  # not necessary, since this is done in checkListServer part
    if server.checkListServer(i) is False:
      continue
    try:
      # print seq
      # print roundNum
      # print proposedVal
      result, individualMin = server.listServer[i].response_to_proposed_value(seq, roundNum, proposedVal)
    except Exception, e:
      # print "Exception in the second stage"
      #print e
      continue  # in case remote server is unreachable, continue
    if result is True:
      trueReply += 1
    """
    if (individualMin > server.listKnownMin[i]):  # some remote server sends his Done message
      # On a second thought I think no lock is fine here
      with server.seqLock:
        if server.listKnownMin[i] is server.minNum:
          server.listKnownMin[i] = individualMin
          tDone = client_thread('tDone' + str(server.number) + ' ' + str(individualMin), threadedDone, serverd)
          tDone.start()
        else:
          server.listKnownMin[i] = individualMin
    """
  if ((trueReply + 1) * 2 <= server.amount or server.alive == False):  
    with server.seqLock.read_access:
      server.listPendingToLead[seq - server.minNum] = 0
      return 
  # It has been decided
  with server.seqLock.read_access:
    server.listValue[seq - server.minNum] = proposedVal
  for i in range(server.amount):
    """
    if (i == server.number):
      continue
    """  # not necessary, since this is done in checkListServer part
    if server.checkListServer(i) is False:
      continue
    try:
      server.listServer[i].response_to_decision(seq, proposedVal)
    except Exception, e:
      continue  # in case remote server is unreachable, continue
  with server.seqLock.read_access:
    server.listPendingToLead[seq - server.minNum] = 0
  return 
class Paxos:
  def __init__ (self):
    self.alive = True
    self.number = -1
    self.amount = -1  # Total amount of peers, so that no length(..) is needed
    self.listValue = []  # decided values
    self.listRoundNum = []  # recorded round numbers for each seq
    self.listPendingValue = []  # recorded last responsed value
    self.listServerAddress = []  # Addresses of other paxos peers
    self.listKnownMin = []
    self.listServer = []  # RPC servers of other paxos peers
    self.listPendingToLead = []
    self.minNum = 0
    #self.listLocks = []  # Locks for different sequence numbers
    self.seqLock = RWLock()
  def checkListServer(self, num):
    if num is self.number:
      return False
    #print num
    #print len(self.listServer)
    if self.listServer[num] is None:
      try:
        self.listServer[num] = xmlrpclib.ServerProxy('http://' + self.listServerAddress[num] + ':' + str(RPC_PORT))
        #log('[INFO] connect to RPC server')
        # print 'server numbered ' + str(self.number) + ' successfully connected to server numbered ' + str(num)
        return True
      except Exception, e:
        self.listServer[num] = None
        #log('[ERROR] fail to connect to RPC server', e)
        # print 'server numbered ' + str(self.number) + ' fails to connect to server numbered ' + str(num)
        return False
    else:
      return True
  @staticmethod
  def make(peers, me):
    p = Paxos()
    p._make1(peers, me)
    return p
  def _make1(self, peers, me):
    #print 'ab'
    if (self.number != -1):
      return False
    self.number = me
    self.listServerAddress = peers
    self.amount = len(peers)
    if (me < 0 or me >= self.amount):
      return False
    for i in range(0, self.amount):  # Suppose this is executed for (len(peers)) times
      self.listKnownMin.append(0)
      self.listServer.append(None)
      #self.CheckListServer(i)  # not necessary
      """  # I moved this part to somewhere else, because every time before RPC call availability needs to be checked
      if i is me:
        self.listServer.append(None)
        continue
      # Reused code from LHP
      try:
        self.listServer.append(xmlrpclib.ServerProxy(self.listServerAddress[i]))
        #self.backup.test()#Have'nt realized this
        #log('[INFO] connect to RPC server')
        return True
      except Exception, e:
        self.listServer[i] = None  # Or maybe I should append a none
        #log('[ERROR] fail to connect to RPC server', e)
        return False
      """
    # Reused code from LHP
    self.server = SimpleXMLRPCServer(
      (self.listServerAddress[me],  RPC_PORT),
      requestHandler=Paxos.RequestHandler
    )
    self.server.register_introspection_functions()
    self.server.register_instance(_RPCFuncs(self))
    tServerForever = client_thread('tServerForever' + str(self.number), threadedServerForever, self.server)
    tServerForever.start()
    tMaintainence = client_thread('tMaintainence' + str(self.number), threadedMaintainence, self)
    tMaintainence.start()
    return True
  def start(self, seq, value):
    while self.alive is False:
      time.sleep(1)# Modified at GY's request
    with self.seqLock.write_access:
      while (self.minNum + len(self.listValue) <= seq):
        self.listValue.append("")
        self.listRoundNum.append(-1)
        self.listPendingValue.append("")
        self.listPendingToLead.append(0)
        #self.listLocks.appen(Lock())
      if self.listPendingValue[seq - self.minNum] is "":
        self.listPendingValue[seq - self.minNum] = value
        tStart = client_thread('tStart' + str(self.number) + ' ' + str(seq), threadedStart, self, seq)
        tStart.start()
        return True
      return False
  def min(self):
    return self.minNum
  def max(self):
    with self.seqLock.read_access:
      return self.minNum + len(self.listValue) - 1
  def kill(self):
    self.alive = False
  def resurrect(self, wait_bool = True):    # preset bool as true so that bugs should appear less often
    self.alive = True
    tResurrect = client_thread('tResurrect' + str(self.number), threadedResurrect, self)
    tResurrect.start()
    if wait_bool is True:
      tResurrect.join()
  """
  def done(self, seq):
    if (self.number == -1):
      return False
    if (seq > self.listKnownMin[self.number]):
      if (self.listKnownMin[self.number] == self.minNum):
        self.listKnownMin[self.number] = seq
        tDone = client_thread('tDone' + str(self.number) + ' ' + str(seq), threadedDone, self)
        tDone.start()
      else:
        self.listKnownMin[self.number] = seq
    return True
  """
  def status(self, seq):
    with self.seqLock.read_access:
      if (seq < self.minNum) :
        # return False, ""    # Modified at GY's request
        return True, None
      print "access"
      if (seq - self.minNum >= len(self.listValue)) :
        return False, '1'
      if (self.listValue[seq - self.minNum] == "") :
        return False, '2'
      else:
        return True, self.listValue[seq - self.minNum]
  class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class client_thread(Thread):  # Reused from Google's test.py
  def __init__(self, name, run_func, *args):
    Thread.__init__(self)
    self.name = name
    self.run_func = run_func
    self.args = args
  def run(self):
    result = self.run_func(*self.args)
    self.result = result

	
class RWLock:
    '''Non-reentrant write-preferring rwlock.'''
    DEBUG = 0

    def __init__(self):
        self.lock = threading.Lock()

        self.active_writer_lock = threading.Lock()
        # The total number of writers including the active writer and
        # those blocking on active_writer_lock or readers_finished_cond.
        self.writer_count = 0

        # Number of events that are blocking on writers_finished_cond.
        self.waiting_reader_count = 0

        # Number of events currently using the resource.
        self.active_reader_count = 0

        self.readers_finished_cond = threading.Condition(self.lock)
        self.writers_finished_cond = threading.Condition(self.lock)

        class _ReadAccess:
            def __init__(self, rwlock):
                self.rwlock = rwlock
            def __enter__(self):
                self.rwlock.acquire_read()
                return self.rwlock
            def __exit__(self, type, value, tb):
                self.rwlock.release_read()
        # support for the with statement
        self.read_access = _ReadAccess(self)

        class _WriteAccess:
            def __init__(self, rwlock):
                self.rwlock = rwlock
            def __enter__(self):
                self.rwlock.acquire_write()
                return self.rwlock
            def __exit__(self, type, value, tb):
                self.rwlock.release_write()
        # support for the with statement
        self.write_access = _WriteAccess(self)

        if self.DEBUG:
            self.active_readers = set()
            self.active_writer = None

    def acquire_read(self):
        with self.lock:
            if self.DEBUG:
                me = threading.currentThread()
                assert me not in self.active_readers, 'This thread has already acquired read access and this lock isn\'t reader-reentrant!'
                assert me != self.active_writer, 'This thread already has write access, release that before acquiring read access!'
                self.active_readers.add(me)
            if self.writer_count:
                self.waiting_reader_count += 1
                self.writers_finished_cond.wait()
                # Even if the last writer thread notifies us it can happen that a new
                # incoming writer thread acquires the lock earlier than this reader
                # thread so we test for the writer_count after each wait()...
                # We also protect ourselves from spurious wakeups that happen with some POSIX libraries.
                while self.writer_count:
                    self.writers_finished_cond.wait()
                self.waiting_reader_count -= 1
            self.active_reader_count += 1

    def release_read(self):
        with self.lock:
            if self.DEBUG:
                me = threading.currentThread()
                assert me in self.active_readers, 'Trying to release read access when it hasn\'t been acquired by this thread!'
                self.active_readers.remove(me)
            assert self.active_reader_count > 0
            self.active_reader_count -= 1
            if not self.active_reader_count and self.writer_count:
                self.readers_finished_cond.notifyAll()

    def acquire_write(self):
        with self.lock:
            if self.DEBUG:
                me = threading.currentThread()
                assert me not in self.active_readers, 'This thread already has read access - release that before acquiring write access!'
                assert me != self.active_writer, 'This thread already has write access and this lock isn\'t writer-reentrant!'
            self.writer_count += 1
            if self.active_reader_count:
                self.readers_finished_cond.wait()
                while self.active_reader_count:
                    self.readers_finished_cond.wait()

        self.active_writer_lock.acquire()
        if self.DEBUG:
            self.active_writer = me

    def release_write(self):
        if not self.DEBUG:
            self.active_writer_lock.release()
        with self.lock:
            if self.DEBUG:
                me = threading.currentThread()
                assert me == self.active_writer, 'Trying to release write access when it hasn\'t been acquired by this thread!'
                self.active_writer = None
                self.active_writer_lock.release()
            assert self.writer_count > 0
            self.writer_count -= 1
            if not self.writer_count and self.waiting_reader_count:
                self.writers_finished_cond.notifyAll()

    def get_state(self):
        with self.lock:
            return (self.writer_count, self.waiting_reader_count, self.active_reader_count)