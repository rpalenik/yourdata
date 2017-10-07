
"""First hug API (local, command-line, and HTTP access)"""


import hug
from bitcoinrpc.authproxy import AuthServiceProxy
import pika
import uuid
import re
import os
import binascii
import json
import sys
import dask
import treelib
import core as ycore
import requests
from os.path import expanduser
xhome = expanduser("~")


# BEGIN config parameters
D_HOME = '/home/ubuntu'
# A_CATALOG ='testcon@172.31.9.219:9253'
A_CATALOG = 'catalog@172.31.9.219:5757'
P_TIMEOUT = 10

catalog_name = re.match('\w+', A_CATALOG).group(0)

# Multichain RPC remote parameters
rpc_user = 'rp'
rpc_password = 'heslorichard'
rpc_server = '52.54.155.107'
rpc_port = '2770'

# connection on server side (example): multichaind dask -server -rpcuser=rp -rpcpassword=heslorichard -rpcallowip=0.0.0.0/0
# END config parameters

# Neo4j graph

from py2neo import Graph, authenticate
authenticate('23.20.90.129:7474', 'neo4j', 'heslorichard')
NEO_GRAPH = Graph('http://23.20.90.129:7474/db/data/')




class RpcClient(object):
    def __init__(self):
        credentials = pika.PlainCredentials(username='admin', password='admin')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='52.54.155.107', credentials=credentials))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='topic_logs',
                                      type='topic')

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.on_response, no_ack=True,
                                   queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        from timeit import default_timer
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key='catalog@172.31.9.219:5757',
                                   properties=pika.BasicProperties(
                                       reply_to=self.callback_queue,
                                       correlation_id=self.corr_id,
                                   ),
                                   body=str(n))
        self.timeout = False
        self.start = default_timer()
        while self.response is None and self.timeout is False:
            self.connection.process_data_events()
            if default_timer() - self.start > P_TIMEOUT:
                self.timeout = True
                self.response = '***Error connection timeout'
        return self.response


# BEGIN Connections
# rpc = RpcClient()

# END Connections


def message_to_json(message):
    try:
        message = message.decode("utf-8")
        print(message)
        message = message.replace("'", '"')
        print(message)
        message = json.loads(message)
    except:
        print("error", message)
        pass
    return message

# shebang does not work over all platforms
# ping.py  2016-02-25 Rudolf
# subprocess.call() is preferred to os.system()
# works under Python 2.7 and 3.4
# works under Linux, Mac OS, Windows


def write_to_chain(l, key_driver, dataset, subset):
    import binascii
    try:
        rpc_connection = create_rpc_conn(dataset)
        key = key_driver.generate_key(l)
        if type(l) != bytes:
            l = l.encode()
        payload = binascii.hexlify(l)
        rpc_connection.publish(subset, key, payload.decode('utf-8'))
    except:
        raise


def get_driver(namespace, name):
    from stevedore import driver
    x = driver.DriverManager(
        namespace=namespace,
        name=name,
        invoke_on_load=True,
        # invoke_args=(parsed_args.width,),
    )
    return x.driver


def ping(host, port):

    import socket

    max_error_count = 10

    def increase_error_count():
        # Quick hack to handle false Port not open errors
        with open('ErrorCount.log') as f:
            for line in f:
                error_count = line
        error_count = int(error_count)
        print("Error counter: " + str(error_count))
        file = open('ErrorCount.log', 'w')
        file.write(str(error_count + 1))
        file.close()
        if error_count == max_error_count:
            # Send email, pushover, slack or do any other fancy stuff
            print("Sending out notification")
            # Reset error counter so it won't flood you with notifications
            file = open('ErrorCount.log', 'w')
            file.write('0')
            file.close()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    result = sock.connect_ex((host, int(port)))
    if result == 0:
        print("Port is open")
        result = True
    else:
        print("Port is not open")
        # increase_error_count()
        result = False
    return result


def request_action(message):
    response = message_to_json(rpc.call(message))
    if response['result'] == 'ok':
        result = True
    else:
        result = False
    return result


def create_rpc_conn(chain, remote=True):
    if remote:
        try:
            rpc_connection = AuthServiceProxy(
                'http://%s:%s@%s:%s' % (rpc_user, rpc_password, rpc_server, rpc_port))
            # print('***Remote rpc connection created')
            return rpc_connection
        except:
            print('***Cannot create remote rpc connection')
            raise
    else:
        try:
            rpc_u, rpc_pw, rpc_por = get_credentials(chain)
            rpc_connection = AuthServiceProxy(
                "http://%s:%s@127.0.0.1:%s" % (rpc_u, rpc_pw, rpc_por))
            return rpc_connection
        except:
            print("***Cannot create local rpc connection")
            raise


def get_credentials(chainname):
    with open(xhome + '/.multichain/' + chainname + '/multichain.conf',
              'r') as xfile:
        x = xfile.read()
        rpc_user = re.search('(?:rpcuser=)(.*)', x).group(1)
        rpc_password = re.search('(?:rpcpassword=)(.*)', x).group(1)

    with open(xhome + '/.multichain/' + chainname + '/params.dat', 'r') as xfile:
        x = xfile.read()
        rpc_port = re.search('(?:default-rpc-port = )(\d*)', x).group(1)
    return rpc_user, rpc_password, rpc_port


@hug.get('/connect')
@hug.cli()
def connect():
    """connect to the platform"""
    if os.path.isdir(D_HOME + '/.multichain/' + catalog_name):
        # Catalog already exists locally, just connecting
        print("***Connecting to local catalog ...")
        os.system("multichaind " + A_CATALOG + " -daemon")
        try:
            rpc_connection = create_rpc_conn(catalog_name, remote=False)
            xres = rpc_connection.getinfo()['nodeaddress']
            # print (xres)
            print("***Running " + xres)
            result = {'result': 'ok'}
        except:
            # print ("ERROR .......")
            print("***Cannot connect to " + catalog_name)
            print("***Exiting...")
            result = {'result': 'failed'}
    else:
        # First connection, going to platform server
        print("*** Connecting to the platform")
        host = re.search('([0-9\.])+', A_CATALOG).group(0)
        port = re.search('(?:\:)(\d+)', A_CATALOG).group(1)
        print(host, port)
        if ping(host, port):
            os.system("multichaind " + A_CATALOG + " -daemon")
            rpc_connection = create_rpc_conn(catalog_name, remote=False)
            xadd = rpc_connection.getaddresses()[0]
            message = {"from": A_CATALOG, "type": "request",
                       "command": "grant_access", "address": xadd}
            response = message_to_json(rpc.call(message))
            if response['result'] == 'ok':
                print("DONE")
                result = {'result': 'ok'}
            else:
                print(response['result'])
                result = {'result': 'failed'}
        else:
            result = {'result': 'failed'}
    return result

@hug.get('/conn')
@hug.cli()
def conn():


@hug.get('/disconnect')
@hug.cli()
def disconnect():
    """disconnect from the platform"""
    try:
        os.system("multichain-cli catalog stop")
        result = {'result': 'ok'}
    except:
        result = {'result': 'failed'}
    return result


@hug.get('/create_dataset')
@hug.cli()
def create_dataset(dataset, description="None"):
    """create new dataset on local vendor chain - create new chain if creating first time"""
    if not os.path.isdir(D_HOME + '/.multichain/' + dataset):
        # test if platform messaging is online
        if request_action({"from": A_CATALOG, "type": "ping"}):
            os.system('multichain-util create ' + dataset)
            os.system('multichaind ' + dataset + ' -daemon')
            rpc_connection = create_rpc_conn(dataset, remote=False)
            nodeaddress = rpc_connection.getinfo()['nodeaddress']
            if not request_action({"from": A_CATALOG, "type": "new_dataset", "dataset": dataset, "nodeaddress": nodeaddress, "description": description}):
                info = '*** Error in subscribing dataset {0} to platform metadata.'.format(
                    dataset)
                print(info)
                result = {'result': 'failed', 'info': info}
            else:
                info = '*** Dataset {0} created. You can use "upload_dataset" to add your data'.format(
                    dataset)
                print(info)
                result = {'result': 'ok', 'info': info}
        else:
            info = '*** Platform is offline. Try again later'
            print(info)
            result = {'result': 'failed', 'info': info}
    else:
        info = '*** Dataset {0} already exists.'.format(dataset)
        print(info)
        result = {'result': 'ok', 'info': info}
    return result


@hug.get('/run_recipe')
@hug.cli()
def run_recipe(recipe_id, runtime):
    """run recipe based on recipe id in catalog:

        recipe_id (int) : Recipe ID
        runtime (dict) : Runtime parameters required by recipe
    """
    runtime = json.loads(runtime)
    print(runtime)

    QUERY = '''
    match (s:recipe) WHERE id(s)=$recipe_id WITH s
    match (s)-[:NEXT_STEP|FIRST_STEP*]->(s2:step) with s,s2
    match (s2)<-[:NEXT_STEP|FIRST_STEP]-(s1) with s1,s2
    match (s2)-[:METHOD]->(me:method) with s1,s2,me
    match (me)<-[:HAS_METHOD]-(cl:class) with s1,s2,me,cl
    match (cl)<-[:HAS_CLASS]-(mo:module) with s1,s2,me,cl,mo
    optional match (me)-[:HAS_PARAM]->(np:param) WITH s1,s2,me,cl,mo,np
    optional match (np)-[:HAS_ALLOWED_VALUE]->(val:paramvalue)-[:USES_PARAM]-(s2) WITH s1,s2,me,cl,mo,np,val
    optional match (np)-[:HAS_DEFAULT_VALUE]->(vald:paramvalue) WITH s1,s2,me,cl,mo,np,val,vald
    WITH s1,s2,me,cl,mo,collect({param:np.name,value:val.value,default:vald.value}) as params
    WITH s1,s2,collect({namespace:mo.namespace,class:cl.name,method:me.name,params:params}) as action
    return {step0:CASE labels(s1)[0]
     WHEN "recipe" THEN -1
     ELSE id(s1)
    END,step1:id(s2),description: s2.description, specs: action[0]}
    '''
    tx = NEO_GRAPH.begin()
    params = {'recipe_id': int(recipe_id)}
    # print(params)
    result = tx.run(QUERY, params)
    tx.commit()
    res = list(result)
    # print(res)
    recipe = [x[0] for x in res]
    if len(recipe)>0:
        tree = treelib.Tree()
        tree= ycore.instantiate_node(tree, recipe, runtime)
        c = [x.data['fnc'] for x in tree.leaves()]
        b = [tree.get_node(x).data['fnc'] for x in tree.expand_tree()]
        print(c)
        r = dask.compute(c)
        print(len(r))
        return r
    else:
        info = '*** Recipe does not exists. Exiting'
        print(info)
        result = {'result': 'failed'}
        return result



@hug.get('/catalog')
@hug.cli()
def catalog():
    """list the platform catalog"""
    rpc_user, rpc_password, rpc_port = get_credentials("catalog")
    rpc_connection = AuthServiceProxy(
        "http://%s:%s@127.0.0.1:%s" % (rpc_user, rpc_password, rpc_port))
    result = {'result': []}
    try:
        xkeys = rpc_connection.liststreamkeys("root", "*", True)

        for x in xkeys:
            xdata = binascii.unhexlify(x['last']['data'])
            xdata = xdata.decode("utf-8")
            xdata = xdata.replace("'", '"')
            try:
                xdata = json.loads(xdata)
                result['result'].append(
                    {'key': x['last']['key'], 'created': x['last']['blocktime'], 'data': xdata})
            except Exception as e:
                continue

    except:
        raise
        result = {'result': 'failed'}

    return result


@hug.get('/subscribe_to_stream')
@hug.cli()
def subscribe_to_stream(chainaddress: hug.types.text, stream: hug.types.text):
    """subscribe to catalog item"""
    try:
        try:
            import commands
            status, output = commands.getstatusoutput("cat /etc/services")
            print(status)
            # os.system("multichaind "+chainaddress+" -daemon > connect.log")
        except Exception:
            sys.exc_clear()
        chainname = re.match('\w+', chainaddress).group(0)
        print(chainname)
        rpc_user, rpc_password, rpc_port = get_credentials(chainname)
        print(rpc_user, rpc_password, rpc_port)
        # rpc_connection = AuthServiceProxy(
        #    "http://%s:%s@127.0.0.1:%s" % (rpc_user, rpc_password, rpc_port))
        # xitems = rpc_connection.subscribe(stream)
        result = {'result': 'Subscription to ' + stream + ' succesful'}
    except Exception:
        raise
        result = {'result': 'failed'}
    return result


@hug.get('/list_stream_items')
@hug.cli()
def list_stream_items(chainname: hug.types.text, stream: hug.types.text):
    """list stream data"""
    try:
        # os.system("multichaind "+chainaddress+" -daemon")
        # chainname = re.match('\w+', chainaddress)
        rpc_user, rpc_password, rpc_port = get_credentials(chainname)
        rpc_connection = AuthServiceProxy(
            "http://%s:%s@127.0.0.1:%s" % (rpc_user, rpc_password, rpc_port))
        print(rpc_user, rpc_password, rpc_port, stream)
        xitems = rpc_connection.liststreamitems(stream)
        result = xitems
    except:
        raise
        result = {'result': 'failed to access stream. Check if subscribed'}
    return result


if __name__ == '__main__':
    connect.interface.cli()
    run_recipe.interface.cli()
    disconnect.interface.cli()
    catalog.interface.cli()
    subscribe_to_stream.interface.cli()
    list_stream_items.interface.cli()
