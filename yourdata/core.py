import re
from os.path import expanduser
from bitcoinrpc.authproxy import AuthServiceProxy


def get_driver(namespace, name):
    from stevedore import driver
    x = driver.DriverManager(
        namespace=namespace,
        name=name,
        invoke_on_load=True,
        # invoke_args=(parsed_args.width,),
    )
    return x.driver


def get_credentials(chainname, xhome=expanduser("~")):
    with open(xhome + '/.multichain/' + chainname + '/multichain.conf',
              'r') as xfile:
        x = xfile.read()
        rpc_user = re.search('(?:rpcuser=)(.*)', x).group(1)
        rpc_password = re.search('(?:rpcpassword=)(.*)', x).group(1)

    with open(xhome + '/.multichain/' + chainname + '/params.dat', 'r') as xfile:
        x = xfile.read()
        rpc_port = re.search('(?:default-rpc-port = )(\d*)', x).group(1)
    return rpc_user, rpc_password, rpc_port


def create_rpc_conn(chain, remote=True):
    if remote:
        try:
            rpc_u = 'rp'
            rpc_pw = 'heslorichard'
            rpc_server = '52.54.155.107'
            rpc_por = '2770'
            rpc_connection = AuthServiceProxy(
                'http://%s:%s@%s:%s' % (rpc_u, rpc_pw, rpc_server, rpc_por))
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


def bracket(params, runtime):
    param_list = [(x['param'] + '="' + str(runtime[x['param']]) + '"' if x['param'] in runtime.keys()
                   else x['param'] + '="' + str(x['value']) + '"' if x['value'] is not None
                   else x['param'] + '="' + str(x['default']) + '"') for x in params if x['param'] is not None]

    br = ''
    for x in param_list:
        br = br + x + ','
    return br


def instantiate_node(tree, recipe, runtime):
    current_step = -1
    line = [x for x in recipe if x['step0'] == current_step][0]
    newstep = line['step1']
    namespace = line['specs']['namespace']
    name = line['specs']['class']
    method = line['specs']['method']
    params = line['specs']['params']
    driver = get_driver(namespace, name)
    xparams = bracket(params, runtime)
    function = eval('driver.' + method + '(' + xparams + ')')
#     print ('driver.'+method+'('+xparams+')')
    xnode = tree.create_node("Step " + str(newstep), newstep, data=line)
    xnode.data['fnc'] = function
    current_step = newstep

    while True:
        #     print(new)
        new = [x for x in recipe if x['step0'] == current_step]
        if len(new) > 0:
            for line in new:

                newstep = line['step1']
                namespace = line['specs']['namespace']
                name = line['specs']['class']
                method = line['specs']['method']
                params = line['specs']['params']
#                 print(namespace)
#                 print(name)
#                 print(method)
#                 print(params)
#                 print (bracket(params))
                driver = get_driver(namespace, name)
                xparams = bracket(params, runtime)

                xparent = tree.get_node(current_step)
#                 print(xparent.data["fnc"])
#                 print ('driver.'+method+'(xparent.data["fnc"],'+xparams+')')
                function = eval('driver.' + method +
                                '(xparent.data["fnc"],' + xparams + ')')
                xnode = tree.create_node(
                    "Step " + str(newstep), newstep, parent=current_step, data=line)
                xnode.data['fnc'] = function
#                 print(xnode.data['fnc'])
            current_step = newstep
        else:
            break

    return tree
