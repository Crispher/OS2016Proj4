import BaseHTTPServer
import xmlrpclib
from threading import Thread
from threading import Lock
from urlparse import urlparse, parse_qs
from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler

from util import *
from store import Store
import sys



class _RPCFuncs:  # Reused LPH's structure
  def __init__(self, server):
    self.server = server
  def test(self):
    return True
  def response_to_proposed_lead(self, seq, roundNum):  # Stage 1b in the slides
    with self.server.seqLock:
      if self.server.listPendingToLead[seq-self.server.minNum] is True:
      # I'm trying to lead too
        return False, "", -2
      if (roundNum <= self.server.listRoundNum[seq-self.server.minNum]):  # validity check of seq needed
      # the round number is old
        return False, "", self.server.listRoundNum[seq-self.server.minNum]
      if (self.server.listValue[seq-self.server.minNum] != ""):
      # seq is already decided, so there's no need for another round
        return False, self.server.listValue[seq-self.server.minNum], -1
      returnRoundNum = self.server.listRoundNum[seq-self.server.minNum]
      self.server.listRoundNum[seq-self.server.minNum] = roundNum
      return True, self.server.listPendingValue, returnRoundNum
  def response_to_proposed_value(self, seq, roundNum, proposedVal): # Stage 2b in the slides
    if (roundNum < self.server.listRoundNum[seq-self.server.minNum]):
    # the round number is old
      return False
    self.server.listPendingValue = proposedVal
    self.server.listRoundNum[seq-self.server.minNum] = roundNum
    return True
  def response_to_decision(self, seq, decidedVal):  # Stage 3 in the slides
    # Maybe adding some assertations?
    self.server.listValue[seq-self.server.minNum] = decidedVal
    return True
def threadedStart(server, seq):
  """print server.number"""
  #server.listRoundNum[seq - server.minNum] += 1
  with server.seqLock:
    if server.listPendingToLead[seq - server.minNum] is True return  # partial fix
    server.listPendingToLead[seq - server.minNum] = True
    roundNum = server.listRoundNum[seq - server.minNum] + 1
  flag = True  # flag about whether to repeat
  while(Flag):
    flag = False
    trueReply = 0
    highestPrevRound = -1
    prevRoundReply = ""
    for i in [0, server.amount - 1]:
      """
      if (i == server.number):
        continue
      """  # not necessary, since this is done in checkListServer part
      if server.checkListServer(i) is False:
        continue
      try:
        result, prePropose, returnRoundNum = server.listServer[i].response_to_proposed_lead(seq, roundNum)
      except Exception, e:
        continue  # in case remote server is unreachable, continue
      if result is True:
        trueReply += 1
        if (returnRoundNum > highestPrevRound):
          if (prePropose == ""):
            raise Exception("Shouldn't reached here")
          else:
            preRoundReply = prePropose
            highestPrevRound = returnRoundNum
      else:
        if (prePropose != ""):
          with server.seqLock:
            server.listValue[seq - server.minNum] = prePropose
            server.listPendingToLead[seq - server.minNum] = False
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
    server.listPendingToLead[seq - server.minNum] = False
  if ((trueReply + 1) * 2 <= server.amount):  
    return 
  # I'm OK to lead
  with server.seqLock:
    if (highestPrevRound > server.listRoundNum[seq - server.minNum]):
      proposedVal = prevRoundReply
      server.listPendingValue[seq - server.minNum] = proposedVal
    else:
      proposedVal = server.listPendingValue[seq - server.minNum]
    server.listRoundNum[seq - server.minNum] = roundNum
    trueReply = 0
  for i in [0, server.amount - 1]:
    """
    if (i == server.number):
      continue
    """  # not necessary, since this is done in checkListServer part
    if server.checkListServer(i) is False:
      continue
    try:
      result = server.listServer[i].response_to_proposed_value(seq, roundNum, proposedVal)
    except Exception, e:
      continue  # in case remote server is unreachable, continue
    if result is True:
      trueReply += 1
  if ((trueReply + 1) * 2 <= server.amount):  
    return 
  # It has been decided
  with server.seqLock:
    server.listValue[seq - server.minNum] = proposedVal
  for i in [0, server.amount - 1]:
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
    if self.listServer[num] is None:
      try:
        self.listServer[num] = xmlrpclib.ServerProxy(self.listServerAddress[num])
        #log('[INFO] connect to RPC server')
        return True
      except Exception, e:
        self.listServer[numi] = None
        #log('[ERROR] fail to connect to RPC server', e)
        return False
    else:
      return True
  def Make(self, peers, me):
    if (self.number != -1):
      return False
    self.number = me
    self.listServerAddress = peers
    self.amount = len(peers)
    if (me < 0 || me >= self.amount):
      return False
    for i in [0, self.amount - 1]:  # Suppose this is exercised for (len(peers)) times
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
      ("""HTTP_HOST_NAME"""listServerAddress[me]"""this should be my address, or LOCALHOST to be exact""", """int(PRIMARY_PORT_NUMBER)""" 8388"""This should be some various ports for different applications"""),
      requestHandler=PrimaryServer.RequestHandler
    )
    self.server.register_introspection_functions()
    self.server.register_instance(_RPCFuncs(self))
    return True
  def Start(self, seq, value)
    with self.seqLock:
      while (self.minNum + len(self.listValue) <= seq):
        self.listValue.append("")
        self.listRoundNum.append(-1)
        self.listPendingValue.append("")
        self.listPendingToLead.append(False)
        #self.listLocks.appen(Lock())
      if self.listPendingValue[seq - self.minNum] is "":
        self.listPendingValue[seq - self.minNum] = value
        tStart = client_thread('tStart' + self.number + ' ' + seq, threadedStart, self, seq)
        tStart.start()
        return True
      return False
  def Min(self):
    return self.minNum
  
  def Max(self):
    return self.minNum + len(self.listValue) - 1
  
  def Done(self, seq):
    # some other handler
    return True
  def Status(self, seq):
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
    
  class client_thread(threading.Thread):  # Reused from Google's test.py
    def __init__(self, name, run_func, *args):
      threading.Thread.__init__(self)
      self.name = name
      self.run_func = run_func
      self.args = args
    def run(self):
      result = self.run_func(*self.args)
      self.result = result