#!/usr/bin/env python
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from py2neo import Graph, authenticate
import pika
import re
import json
from os.path import expanduser
xhome = expanduser("~")


def get_credentials(chainname):
    with open(xhome + '/.multichain/' + chainname + '/multichain.conf', 'r') as xfile:
        x = xfile.read()
        rpc_user = re.search('(?:rpcuser=)(.*)', x).group(1)
        rpc_password = re.search('(?:rpcpassword=)(.*)', x).group(1)

    with open(xhome + '/.multichain/' + chainname + '/params.dat', 'r') as xfile:
        x = xfile.read()
        rpc_port = re.search('(?:default-rpc-port = )(\d*)', x).group(1)
    return rpc_user, rpc_password, rpc_port


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


def on_request(ch, method, props, body):

    print(" [.] message (%s)" % body)
    response = message_to_json(body)
    response['result'] = 'None'
    print(type(response))

    if response['type'] == "ping":
        response['result'] = "ok"

    elif response['type'] == "new_dataset":
        x = apply_metadata_command(response)
        response['result'] = x

    elif response['type'] == "request":
        x = apply_command(response)
        print(x)
        response['result'] = x
    else:
        response['result'] = "Unknown request type : " + response['type']

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(
                         correlation_id=props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)


def apply_command(message):
    """ Apply command on blockchain"""
    if message['command'] == 'grant_access':
        xres = rpc_connection.grant(message['address'], "send,receive")
        print(xres)
        return "ok"
    else:
        return "unknown command " + message['command']


def run_neo_query(QUERY, message):
    try:
        tx = graph.begin()
        tx.run(QUERY, message)
        tx.commit()
        return True
    except:
        raise
        return False


def apply_metadata_command(message):
    """ Apply command on metadata store"""
    if message['type'] == 'new_dataset':
        QUERY = 'MATCH (r:rootnode) WITH r MERGE (n:node {nodeaddress: {nodeaddress}, name: {dataset}, description: {description}})-[:CONNECTED_TO]->(r)'
        if run_neo_query(QUERY, message):
            return "ok"
        else:
            return "error in metadata write"
    else:
        return "unknown message type " + message['type']


# connections
rpc_user, rpc_password, rpc_port = get_credentials("catalog")
rpc_connection = AuthServiceProxy(
    "http://%s:%s@127.0.0.1:%s" % (rpc_user, rpc_password, rpc_port))
credentials = pika.PlainCredentials(username='admin', password='admin')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', credentials=credentials))
channel = connection.channel()

channel.exchange_declare(exchange='topic_logs',
                         type='topic')
queue_names = ['catalog@172.31.9.219:5757']

neo_auth_h = "23.20.90.129:7474"
neo_auth_n = "neo4j"
neo_auth_p = "heslorichard"
neo_graph = "http://23.20.90.129:7474/db/data/"
authenticate(neo_auth_h, neo_auth_n, neo_auth_p)
graph = Graph(neo_graph)

# connections end

for queue_name in queue_names:
    channel.queue_bind(exchange='topic_logs',
                       queue=queue_name,
                       routing_key=queue_name)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(on_request, queue=queue_name)


print(" [x] Awaiting RPC requests")
channel.start_consuming()
