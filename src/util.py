import sys
import json

with open('conf/settings.conf') as data_file:
  data = json.load(data_file)

PORT = int(data['port'])
RPC_PORT = PORT + 1
data.pop('port')
if len(sys.argv) > 1:
  node_id = sys.argv[1]
  HOST = data[node_id]
  ME = int(node_id[1:])-1
n_hosts = len(data)
HOSTS_LIST = [str(data['n' + ('%02i'%i)]) for i in range(1, n_hosts+1)]



DEBUG = True

GET_PATH = '/kv/get'
INSERT_PATH = '/kv/insert'
UPDATE_PATH = '/kv/update'
DELETE_PATH = '/kv/delete'

COUNT_PATH = '/kvman/countkey'
DUMP_PATH = '/kvman/dump'
SHUTDOWN_PATH = '/kvman/shutdown'


def generate_response(success, value=None):
  return '{"success" : "%s"' % str(success).lower() + \
         ('}' if value is None else (', "value": "%s"}' % value))


def log(*args):
  if DEBUG:
    sys.stdout.write(" ".join(map(str, args)) + '\n')
