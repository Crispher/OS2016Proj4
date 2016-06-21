import BaseHTTPServer
import xmlrpclib
from threading import Thread
from threading import Lock
from urlparse import urlparse, parse_qs
from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler

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
    print 'server numbered ' + str(self.server.number) + ' responding to proposed lead'
    with self.server.seqLock:
      #print 'server numbered ' + str(self.server.number) + ' responding to proposed lead, lock acquired'
      while (self.server.minNum + len(self.server.listValue) <= seq):
        #print "Appending!!!~~~"
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
      #print "exit position 4"
      return True, self.server.listPendingValue[seq-self.server.minNum], returnRoundNum, self.server.listKnownMin[self.server.number]
  def response_to_proposed_value(self, seq, roundNum, proposedVal): # Stage 2b in the slides
    print "stage 2"
    print 'Stage 2: Actual length is ' + str(len(self.server.listValue))+ str(len(self.server.listRoundNum))+ str(len(self.server.listPendingValue))+ str(len(self.server.listPendingToLead))
    print str(seq-self.server.minNum)
    if (roundNum < self.server.listRoundNum[seq-self.server.minNum]):
    # the round number is old
      print 'aaaaa'
      return False, self.server.listKnownMin[self.server.number]
    print 'bbbbb'
    self.server.listPendingValue[seq-self.server.minNum] = proposedVal
    print 'ccccc'
    self.server.listRoundNum[seq-self.server.minNum] = roundNum
    return True, self.server.listKnownMin[self.server.number]
  def response_to_decision(self, seq, decidedVal):  # Stage 3 in the slides
    # Maybe adding some assertations?
    self.server.listValue[seq-self.server.minNum] = decidedVal
    return True
def threadedDone(server):
  with server.seqLock:  # the whole session should be wrapped in locks
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
    time.sleep(5)  # number of seconds slept over next check, I randomly picked "2" without further examination. In reality, this number should depend on the number of servers as well as network condition
    with server.seqLock:
      for i in range(len(server.listValue)):  # suppose this is executed for len(server.listValue) times
        if (server.listValue[i] == "" and server.listPendingValue[i] != ""):
          """tStart = """
          client_thread('tStart' + str(server.number) + ' ' 
              + str(i + server.minNum) + ' by maintainence', threadedStart, server, i + server.minNum).start()
          #tStart.start()
def threadedServerForever(server):
  server.serve_forever()
def threadedStart(server, seq):
  """print server.number"""
  #server.listRoundNum[seq - server.minNum] += 1
  with server.seqLock:
    if (server.listPendingToLead[seq - server.minNum] > 0): 
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
      print "Trying to connect to server " + str(i)
      if server.checkListServer(i) is False:
        continue
      print "Decided to connect to server " + str(i)
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
        print "Exception returned from server " + str(i)
        continue  # in case remote server is unreachable, continue
      # except xmlrpclib.ProtocolError as err:
      #   print "A protocol error occurred"
      #   print "URL: %s" % err.url
      #   print "HTTP/HTTPS headers: %s" % err.headers
      #   print "Error code: %d" % err.errcode
      #   print "Error message: %s" % err.errmsg
      print "Successfully got reply from server " + str(i)
      if (individualMin > server.listKnownMin[i]):  # some remote server sends his Done message
        print "Updating local min list"
        if server.listKnownMin[i] is server.minNum:
          server.listKnownMin[i] = individualMin
          tDone = client_thread('tDone' + str(server.number) + ' ' + str(individualMin), threadedDone, server)
          tDone.start()
        else:
          server.listKnownMin[i] = individualMin
      if result is True:
        trueReply += 1
        if (returnRoundNum > highestPrevRound):
          if (prePropose == ""):
            print str(returnRoundNum) + str(highestPrevRound)
            raise Exception("Shouldn't reached here")
          else:
            preRoundReply = prePropose
            highestPrevRound = returnRoundNum
      else:
        if (prePropose != ""):
          with server.seqLock:
            server.listValue[seq - server.minNum] = prePropose
            server.listPendingToLead[seq - server.minNum] = 0
          return
        else:
          if returnRoundNum is -2:  # Somebody else is trying to lead
            """server.listPendingToLead[seq - server.minNum] = False
            return"""
            break  # I think this is a better solution
          else:
            roundNum = returnRoundNum + 1
            #server.listRoundNum[seq - server.minNum] = roundNum  # This shouldn't be here
            flag = True
            break
  with server.seqLock:
    if ((trueReply + 1) * 2 <= server.amount):  
      server.listPendingToLead[seq - server.minNum] = 0
      return 
    else:
      server.listPendingToLead[seq - server.minNum] = 1
  # I'm OK to lead
  print "I'm leading"
  #proposedVal = ""
  with server.seqLock:
    if (highestPrevRound > server.listRoundNum[seq - server.minNum]):
      proposedVal = prevRoundReply
      server.listPendingValue[seq - server.minNum] = proposedVal
    else:
      proposedVal = server.listPendingValue[seq - server.minNum]
    server.listRoundNum[seq - server.minNum] = roundNum
  trueReply = 0
  for i in range(0, server.amount):
    """
    if (i == server.number):
      continue
    """  # not necessary, since this is done in checkListServer part
    if server.checkListServer(i) is False:
      continue
    try:
      print seq
      print roundNum
      print proposedVal
      result, individualMin = server.listServer[i].response_to_proposed_value(seq, roundNum, proposedVal)
    except Exception, e:
      print "Exception in the second stage"
      print e
      continue  # in case remote server is unreachable, continue
    if result is True:
      trueReply += 1
    if (individualMin > server.listKnownMin[i]):  # some remote server sends his Done message
      if server.listKnownMin[i] is server.minNum:
        server.listKnownMin[i] = individualMin
        tDone = client_thread('tDone' + str(server.number) + ' ' + str(individualMin), threadedDone, serverd)
        tDone.start()
      else:
        server.listKnownMin[i] = individualMin
  if ((trueReply + 1) * 2 <= server.amount):  
    with server.seqLock:
      server.listPendingToLead[seq - server.minNum] = 0
      return 
  # It has been decided
  print "It's decided"
  print proposedVal
  with server.seqLock:
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
  with server.seqLock:
    server.listPendingToLead[seq - server.minNum] = 0
  return 
class Paxos:
  def __init__ (self):
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
    self.seqLock = Lock()
  def checkListServer(self, num):
    if num is self.number:
      return False
    #print num
    #print len(self.listServer)
    if self.listServer[num] is None:
      try:
        self.listServer[num] = xmlrpclib.ServerProxy('http://' + self.listServerAddress[num] + ':' + str(RPC_PORT))
        #log('[INFO] connect to RPC server')
        print 'server numbered ' + str(self.number) + ' successfully connected to server numbered ' + str(num)
        return True
      except Exception, e:
        self.listServer[num] = None
        #log('[ERROR] fail to connect to RPC server', e)
        print 'server numbered ' + str(self.number) + ' fails to connect to server numbered ' + str(num)
        return False
    else:
      return True
  @staticmethod
  def make(peers, me):
    p = Paxos()
    p._make1(peers, me)
    return p
  
  def _make1(self, peers, me):
    print 'ab'
    if (self.number != -1):
      return False
    print 'ac', me, len(peers)
    self.number = me
    self.listServerAddress = peers
    self.amount = len(peers)
    if (me < 0 or me >= self.amount):
      return False
    print 'aa', self.amount
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
    return True
  def start(self, seq, value):
    #print "starting: "
    #print str(seq)
    #print value
    with self.seqLock:
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
        tMaintainence = client_thread('tMaintainence' + str(self.number), threadedMaintainence, self)
        tMaintainence.start()
        return True
      return False
  def min(self):
    return self.minNum
  def max(self):
    with self.seqLock:
      return self.minNum + len(self.listValue) - 1
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
  def status(self, seq):
    with self.seqLock:
      if (seq < self.minNum) :
        return False, ""
      if (seq - self.minNum >= len(self.listValue)) :
        return False, ""
      if (self.listValue[seq - self.minNum] == "") :
        return False, ""
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
