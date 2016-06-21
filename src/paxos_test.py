from kvpaxos import *
from util import *
import time
px = Paxos.make(HOSTS_LIST, ME)
if ME == 1:
  px.start(0, 1)
  while True:
    b, v = px.status(0)
    print b
    if b:
        break
    time.sleep(1)
    
  px.start(1, 2)
  while True:
    b, v = px.status(0)
    print b
    if b:
        break
    time.sleep(1)
    
  print 'finish'