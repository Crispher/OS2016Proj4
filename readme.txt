This is a too simple (sometimes naive) test that fulfills all the requirements stated in the project description. This test (test.py) follows (copys) exactly the test file of project 3. It tries 1000 (maybe less, since this time there's more messages passing around, which slows things down) insert/update/get/delete in serial, as well as a multi-thread exclusion test on the insertion of keys. 

To start the test, the command line should be
python2 src/server.py n01
python2 src/server.py n02
python2 src/server.py n03
python2 src/test.py

For the Paxos part, should anyone find himself with excess time and energy and decide to run test on it, the KVpaxos.py file implements all the required paxos operations as in the project description file. The paxos server serves forever unless interrupted. One can send kill() or resurrect() signals to it to stop/resume its service. When paxos is down (after "kill"), it sends empty response to any incoming request. When paxos resumes service (after "resurrect"), it will try to connect to its peers and recover any lost information.