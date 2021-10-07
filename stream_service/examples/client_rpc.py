"""
In this example we show how we can create two clients using the class StreamChannelClient_Thread as a super class.
We extend the base functionality by adding rpc-methods (remote procedure calls) that can be executed and used by
other clients.
create 4 Clients that will connect to the server and setup several channels
Then different type of data is streamed in between the clients


    Client1
    call rpc Client2
            \
             \
              \
               Server
              /
             /
            /
    Client2
    call rpc Client 1

REMARK: To let the example run start the run_server.py script in a parallel process
"""

import asyncio
import sys
import os
import time
import threading
import ssl

if "-root_path" in sys.argv: #started via shell script
    ROOT=os.path.dirname(os.path.dirname(os.path.normpath(sys.argv[(sys.argv.index("-root_path")+1)])))
else:
    ROOT=os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from stream_service import *

class RPCClass():
    """
    helper class that contains more rpc methods
    """
    def rpc_add(self,a,b):
        return self.myadd(a,b)

    def rpc_substract(self, a, b):
        return a - b

    def myadd(self,a,b):
        return a + b

# 1. Define new client classes with extended rpc function using the StreamChannelClient_Thread as super class

class Client1(StreamChannelClient_Thread):

    def __init__(self,*args,**kwargs):
        super().__init__(*args, **kwargs)
        self.rpc_calc=RPCClass()
        self._rpc_methods_descs, self._rpc_methods = rpc.RPCFactory.class_init_rpc_functions(self, first_items=[
            'rpc_get_rpc_info', 'rpc_activate_write_channel'],address_size=self.address_size)

class Client2(StreamChannelClient_Thread):

    def rpc_multiply(self, a, b):
        return a * b

    def rpc_divide(self, a, b):
        return a / b


if __name__ == '__main__':

    # 2. create the client threads based on the two new class definitions

    clt1=Client1('RPCClient1','127.0.0.1',server_authenticate=b'STRM_Client')
    clt1.start()
    clt1.wait_until_alive()

    print('Client1 connected to server')

    clt2 = Client2('RPCClient2','127.0.0.1', server_authenticate=b'STRM_Client')
    clt2.start()
    clt2.wait_until_alive()
    print('Client2 connected to server')

    # 3. get the related client RPCFactory objects
    rpc_client2= clt1.create_rpc_client_service('RPCClient2')
    rpc_client1 =clt2.create_rpc_client_service('RPCClient1')

    # 4. call the rpc methods on the clients:
    print('Result multiply: 2*3 =',rpc_client2.multiply(2,3))
    print('Result divide: 9/3 =',rpc_client2.divide(9, 3))
    print('Result add: 2+3 =',rpc_client1.calc.add(2,3))
    print('Result substract: 2-3 =',rpc_client1.calc.substract(2, 3))

    # 5. stop the clients
    clt1.stop_client()
    clt2.stop_client()
