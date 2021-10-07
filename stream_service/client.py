"""
This code is taken from PyPi stream_service package
https://github.com/BR1py/stream_service
"""
from __future__ import absolute_import
import asyncio
import uuid
import time
import sys
import queue
import threading

from .lib import *
from .lib.frame import *
from .lib.buffer import *
from .lib.rpc import *

DEBUG = False  # Variable used for development proposes (more print outputs given)

__package__ = 'stream_service'
__version__ = '0.1.0'
__licence__ = 'MIT'
__author__ = 'B.R.'
__url__ = 'https://github.com/BR1py/stream_service'
__description__ = 'Client/Server implementation for data streaming in channels based on python >= 3.5'

# helper classes for the STRM_CLientThread

class SrvInfo():
    '''
    This class holds the server related information
    '''

    def __init__(self, c_id, control_key, RPCMethodDescriptions):
        self.c_id = c_id
        self.control_key = control_key
        self.RPCMethodDescriptions = RPCMethodDescriptions


class ClientChannel():
    '''
    ClientChannel object for each channel used (owned/writer/reader) in the STRM_Client we have such an object
    The object contains all channel related information
    '''

    def __init__(self, name, chl_id, server_maxsize=None, client_maxsize=None, full_behavior=RING_BUFFER_FULL,
                 loop=None,
                 owned_control_key=None):
        '''

        :param name: channel name
        :param chl_id: channel id
        :param queue_maxsize: maximum buffer-size of the read queue
        :param loop:  event loop  - if the loop is None the read queue will be a normal not a asynchio Queue
                                    in this case  the commands must not be awaited
        :param is_ring_buffer: If wait_if_full is False and queue is full:
                               True - oldest element will be deleted and new element will be placed in
                               False - queue will be cleared and new element is put in
        :param wait_if_full: If this is True the read queue will be blocked until the the data is taken out of the queue
                             this ensures data consistency on client side, on server side we can only ensure if the
                             buffer_size given to the channel_reader is sufficient to catch all the data.
        '''
        self._name = name
        self._id = chl_id
        self._loop = loop
        self._owned_control_key = owned_control_key
        self._server_maxsize = server_maxsize
        self._client_maxsize = client_maxsize
        self._full_behavior = full_behavior
        self._active = True

        # The queue_maxsize is increased by one to be bigger then on the server
        if loop is not None:
            if client_maxsize is not None:
                self._buffer = Buffer(client_maxsize, full_behavior, loop=loop,
                                      chl_id=chl_id, name=self.name)
            else:
                self._buffer = Buffer(None, full_behavior, loop=loop,
                                      chl_id=chl_id, name=self.name)

    # give access to some internals

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def control_key(self):
        return self._owned_control_key

    @property
    def is_active(self):
        return self._active

    @property
    def buffer(self):
        return self._buffer

    def set_activate(self, activate=True):
        self._active = bool(activate)

    def __repr__(self):
        if self._owned_control_key is not None:
            control_key = '***********'
        else:
            control_key = 'None'
        return 'ClientChannel(name = %s, chl_id = %s, server_maxsize = %s, client_maxsize = %s' \
               'full_behavior = %s, loop = %s, owned_control_key = %s)' % (repr(self._name),
                                                                           repr(self._id),
                                                                           repr(self._server_maxsize),
                                                                           repr(self._client_maxsize),
                                                                           repr(FULL_BEHAVIOR_STR.get(
                                                                               self._full_behavior)),
                                                                           repr(self._loop),
                                                                           control_key
                                                                           )


# Client

class StreamChannelClient_Thread(threading.Thread):
    def __init__(self, name,
                 ip_address,
                 port=None,
                 ssl=None,
                 server_authenticate=None,
                 loop=None,
                 tto_default=TTO_DEFAULT,
                 tto_para_name=TTO_DEFAULT_PARA_NAME):
        '''
        Stream Server Client Thread

        :param name: name of the client
        :param ip_address: ip address of the Stream Server
        :param port: port of the Stream Server
        :param server_authenticate: authentication string for clients t connect to the server
        :param loop: optional external event loop (if not given the asyncio default event loop will be used)
        :param tto_default: default transaction time out (after this time any transaction will be stopped)
                            the default time out is only used if no explicit time out is given
                            (tto_parameter in functions)
        :param tto_para_name:  name of the tto_parameter in rpc_functions
                               If this parameter is given it will be removed and will not be given to the rpc function.
                               But it will be considered as explicit timeout of the specific call
                               (we will not use the default timeout in this case).I
        '''
        super().__init__(name='StreamChannelClient')
        self._ = loop
        if port is None:
            port = DEFAULT_BUFFER_STREAM_MGR_PORT
        self._address = (ip_address, port)
        self._ssl = ssl
        if type(server_authenticate) is not bytes:
            raise TypeError('Given server_authenticate parameter must be of type bytes')
        self._server_authenticate = server_authenticate
        self._tto_default = tto_default
        self._tto_para_name = tto_para_name
        self._me = uuid.uuid4().hex[:8]
        self._name = str(name)
        self._address_size = 2  # default will be rewritten after server connection is made
        mask = 0
        for i in range(self._address_size):
            mask = (mask << 8) + 0xFF
        self._address_mask = mask

        self._loop = loop
        self._srv_info = None
        self._srv_rpc = None
        self._client_rpc_services = {}
        self._client_id = None
        if DEBUG: print('Client initialized')
        self._write_chls = {}
        self._read_chls = {}
        self._transactions = {}
        self._server_died = False
        self._alive = False
        self._reader = None
        self._writer = None
        self._stop_event = threading.Event()
        self._ta_id_hdl = IdsHandler(self._address_size)
        # analyse clients rpc methods
        self._rpc_methods_descs, self._rpc_methods = rpc.RPCFactory.class_init_rpc_functions(self, first_items=[
            'rpc_get_rpc_info', 'rpc_activate_write_channel'])

    #give access to some internals

    @property
    def address_size(self):
        return self._address_size

    @property
    def name(self):
        return self._name

    def run(self):
        """
        # start the the thread and the main loop for reading data

        :return:
        """
        # Start the client thread
        if DEBUG: print('Client thread started')
        if self._loop is not None:
            asyncio.set_event_loop(self._loop)
        try:
            self._loop = asyncio.get_event_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        if DEBUG: self._loop.set_debug(True)
        # we try to connect to the server
        self._loop.run_until_complete(self._connect())
        # we start the read loop
        self._loop.run_until_complete(self._read_from_server_forever())
        # if the read loop is stopped the client thread will be stopped
        if DEBUG: print('Client thread stopped')

    def __get_new_transaction(self, tto=None):
        '''
        a helper function to create a new transaction object
        :param tto: give the transaction_timeout for this transaction
        :return:
        '''
        if tto is None:
            tto = self._tto_default
        return Transaction(self._loop, self._transactions, self._ta_id_hdl, self._address_size, tto,
                           self._tto_para_name)

    def wait_until_alive(self):
        '''
        helper function for the thread init
        it waits until the thread is fully started and the Client can be used
        :return:
        '''
        while not self._alive:
            time.sleep(0.5)

    # main methods:
    async def _connect(self):
        """
        connect to the server is the first thing executed after the thread is started
        :return:
        """
        transaction1 = None
        try:
            if DEBUG: print('Connection to StreamChannelServer:', self._address)
            try:
                # create the first connection to the server
                reader, writer = await asyncio.open_connection(self._address[0], self._address[1], ssl=self._ssl)
            except ConnectionRefusedError:
                raise ConnectionRefusedError('Server %s not reached!' % repr(self._address))
            self._reader = reader
            self._writer = writer
            #wait for first frame received from the server (server starts the communication)
            first_rx_frame = await read_msg(reader)
            if DEBUG: print('First response from server', first_rx_frame)
            if first_rx_frame == b'0':
                raise StreamError_Overload('Server rejected connection request, to many clients connected')
            # get the address size used on the server:
            address_size = first_rx_frame[0]
            if not type(address_size) is int:
                address_size = int.from_bytes(address_size, BO)
            if address_size > 8:
                raise ValueError(
                    'Address size told by server to large (unplausible); something wrong in server response')
            # redefine global address size
            self._address_size = address_size
            mask = 0
            for i in range(self._address_size):
                mask = (mask << 8) + 0xFF
            self._address_mask = mask
            if DEBUG: print('New address size set to:', address_size)
            # seed and key:
            seed = first_rx_frame[1:]
            if DEBUG: print('Seed received:', seed)
            raw_key = get_raw_key(seed, self._server_authenticate)  # +b'fehler'
            if DEBUG: print('Raw key:', raw_key)
            transaction1 = self.__get_new_transaction()
            tx_frame = Frame(FrameHeader(container_type=CT_CMD,
                                         coder_type=CODE_PICKLE,
                                         transaction_id=transaction1.id,
                                         command_id=0  # authentication command
                                         ),
                             raw_key,  # authentication
                             self.name,  # client_name
                             self._rpc_methods_descs,  # rpc methods
                             )
            if DEBUG: print('Connection data_frame send to server', tx_frame)
            tx_bytes = tx_frame.encode()
            if DEBUG: print('Connection data_frame send to server', tx_bytes)
            # send key to server
            await send_msg(writer, tx_bytes)

            if DEBUG: print('Waiting for server to respond...')
            rx_bytes = await read_msg(reader)
            if rx_bytes is None:
                raise StreamError_Connection("Connection to Stream Server couldn't been established")
            if DEBUG: print('Server response raw bytes', rx_bytes)
            rx_frame = Frame(FrameHeader(CT_DECODE, address_size=address_size), rx_bytes)
            if DEBUG: print('Server response decoded:', rx_frame)
            if rx_frame.header.container_type == CT_EXCEPTION:
                raise StreamError_RPC(repr(rx_frame.args))
            if rx_frame.header.transaction_id != transaction1.id:
                raise StreamError_TransactionMissmatch(
                    'The received transaction_id does not match, no connection established')
            # store the response info
            # store server related information received
            self.srv_info = SrvInfo(*rx_frame.args)
            self.client_id = self.srv_info.c_id
            # create the server RPC Factory object for the server rpc calls
            self.srv_rpc = RPCFactory(self.srv_info.RPCMethodDescriptions,
                                      FrameHeader(CT_SRV_CMD, CODE_PICKLE,
                                                  transaction_id=1,
                                                  command_id=1,
                                                  client_id=0,
                                                  channel_id=self.srv_info.c_id,
                                                  address_size=self.address_size),
                                      reader, writer, self._loop, self.__get_new_transaction)
            if DEBUG: print('Server RPC methods discovered:', self.srv_rpc._rpc_methods)
            transaction1.delete()
            return 'Server connection established'
        except:
            if transaction1 is not None:
                transaction1.delete()
            raise

    async def _read_from_server_forever(self):
        '''
        This is the main loop of the Client thread it reads the data received from the server
        The event loop is on hold (awaited until the reader contains any data received from the server)
        We process here also all RPC calls to the client received from the outside
        :return:
        '''
        loop = self._loop
        reader = self._reader
        writer = self._writer
        address_size = self._address_size
        self._alive = True
        if DEBUG: print('Client read loop started', writer, reader)
        none_cnt = 0
        rx_bytes = None
        while not self._stop_event.is_set():
            try:
                rx_bytes = await asyncio.wait_for(read_msg(reader),
                                                  timeout=5)  # await for a frame received in the reader
            except asyncio.TimeoutError:
                # From time to time we break the await in the event loop to see if the while loop should be stopped
                continue
            except:
                pass
            if rx_bytes is None:
                # server died?
                none_cnt += 1
                if none_cnt > 10:
                    # we give the server a small time to come back
                    self.server_died = True
                    self._stop_event.set()
                continue
            none_cnt = 0
            # if DEBUG: print('rx_frame', rx_frame)
            if rx_bytes == b'STOP':
                # in case of client close this trigger received to stop the loop
                #  but we continue be cause we expect the self.alive flag is set to False in this case
                # otherwise we ignore the frame for security reasons,
                # we do not like to allow this simple stop from the outside it's for internal use only
                continue
            # first analysis of received frame
            rx_header = FrameHeader(CT_DECODE, rx_bytes, address_size=address_size)
            container_type = rx_header.container_type
            rx_payload = rx_bytes[len(rx_header):]
            # react depending of the container type
            if container_type == CT_DATA or container_type == CT_DATA_END:
                chl = self._read_chls.get(rx_header.channel_id)
                if chl is None:
                    continue
                await chl.buffer.put((rx_header, rx_payload))
            elif container_type == CT_CMD:
                # call client rpc command
                try:
                    rpc_cmd = self._rpc_methods[rx_header.command_id]
                except:
                    rpc_cmd = None
                if (rpc_cmd is None):  # we do not allow the access to the authentication cmd here!
                    tx_header = FrameHeader(container_type=CT_EXCEPTION,
                                            coder_type=CODE_MARSHAL,
                                            transaction_id=rx_header.transaction_id,
                                            address_size=address_size)
                    tx_frame = Frame(tx_header, StreamError_Command('Client RPC command unknown'))
                else:
                    rx_frame = Frame(rx_header)
                    rx_frame.decode_payload(rx_payload)
                    if rx_frame.args is None:
                        args = tuple()
                    else:
                        args = rx_frame.args
                    if rx_frame.kwargs is None:
                        kwargs = {}
                    else:
                        kwargs = rx_frame.kwargs
                    back = rpc_cmd(*args, **kwargs)
                    tx_header = FrameHeader(container_type=CT_RETURN,
                                            coder_type=rx_header.coder_type,
                                            transaction_id=rx_header.transaction_id,
                                            client_id=rx_header.client_id,
                                            address_size=self.address_size)
                    if back is None:
                        tx_frame = Frame(tx_header)
                    else:
                        tx_frame = Frame(tx_header, back)
                await send_msg(writer, tx_frame.encode())
                continue
            elif container_type in {CT_RETURN, CT_EXCEPTION}:
                ta = self._transactions.get(rx_header.transaction_id)
                if ta is not None:
                    await ta.buffer.put(rx_bytes)
        if self._server_died:
            self.stop_client()

    def create_new_channel(self, name, public=True, lifetime=None, single_in=True):
        """
        create a new channel on the connected server
        and create the related client internal object related to the channel
        Note:: see the related rpc command n the server
        :param name: channel name
        :param public: is channel public or not
        :param lifetime: if not None give integer in seconds for temporary channels
        :param single_in: Do we allow multi write on the channel (other clients can write or not)
        :return:
        """
        name = str(name)  # to differentiate in the dicts the name must always be a string
        back = self.srv_rpc.create_new_channel(self.client_id, name, public, lifetime, single_in)
        if back is None:
            raise StreamError_Channel('Channel creation failed no data received from server')
        chl_id, control_key = back
        new_chl = ClientChannel(name, chl_id, owned_control_key=control_key)
        # we put two entries in the internal write channel dict to make it reachable via both keys (id and name)
        self._write_chls[name] = new_chl
        self._write_chls[chl_id] = new_chl
        return 'Channel created'

    def subscribe_read_from_channel(self, chl_id_or_name,
                                    server_buffer_size=100,
                                    server_full_behavior=RING_BUFFER_FULL,
                                    fill_rate=1,
                                    local_buffer_size=None,
                                    local_wait_if_full=True):
        """
        read subscription on the channel on the server and
        creation of the related client internal objects
        see related rpc command on the server
        :param chl_id_or_name: channel name which should be subscriped
        :param server_buffer_size: buffer size on the server
        :param server_full_behavior: what happens if buffer is full (see Buffer object)
        :param fill_rate: Integer defines how often we sample,
                  * if fill rate is 1 any channel item is put in the read buffer
                  * if fill rate is 2 any second item will be put in the read buffer
                  * if fill rate is n and n item will be put in the read buffer
        :param local_buffer_size: if given the local read buffer size can be defined
                                  if None the size is calculated based on the related to server_buffer_size
                                  this ensures that under normal conditions the local client buffer cannot overrun
        :param local_wait_if_full: Should we wait in case the local buffer is full or not
        :return:
        """
        back = self.srv_rpc.subscribe_read_from_channel(self.client_id,
                                                        chl_id_or_name,
                                                        server_buffer_size,
                                                        server_full_behavior,
                                                        fill_rate)
        if back is None:
            return False
        chl_name, chl_id = back

        if local_buffer_size is None:
            local_buffer_size = server_buffer_size + 1
        # create local ClientChannel representation object
        new_chl = ClientChannel(chl_name, chl_id,
                                local_buffer_size,
                                server_full_behavior,
                                local_wait_if_full,
                                self._loop)

        self._read_chls[chl_name] = new_chl
        self._read_chls[chl_id] = new_chl
        return 'Channel read subscription finished'

    def unsubscribe_read_from_channel(self, chl_id_or_name):
        """
        delete the subscribtion for a channel
        all related objects will be deleted
        :param chl_id_or_name: channel name or id
        :return:
        """
        back = self.srv_rpc.unsubscribe_read_from_channel(self.client_id,
                                                          chl_id_or_name)
        chl = self._read_chls[chl_id_or_name]
        del self._read_chls[chl.name]
        del self._read_chls[chl.id]
        end_header = FrameHeader(CT_DATA_END,
                                 coder_type=CODE_MARSHAL,
                                 channel_id=chl._channel_id,
                                 address_size=self._address_size)
        # to stop the loop

        chl.buffer.put_nowait(data_bytes=Frame(end_header, 'END DATA').encode())
        return 'Channel read unsubscription finished'

    def subscribe_write_to_channel(self, chl_id_or_name):
        """
        subscribe to channel as additional writer
        (see related rpc command on teh server)
        :param chl_id_or_name: channel name or id for identification
        :return:
        """
        back = self.srv_rpc.subscribe_write_to_channel(self.client_id, chl_id_or_name)
        if back is None:
            raise StreamError_Channel('Channel subscription for channel %s failed' % repr(chl_id_or_name))
        chl_name, chl_id = back
        new_chl = ClientChannel(chl_name, chl_id, self._loop)
        self._write_chls[chl_name] = new_chl
        self._write_chls[chl_id] = new_chl
        return 'Channel write subscription finished'

    def start_data_transfer(self, chl_id_or_name, coder_type):
        """
        stream data on the channel start the streaming with this command
        :param chl_id_or_name: channel name
        :param coder_type: type of encoding that should be used for the given data
                           CODER_MARSHAL;CODER_PICKLE or CODER_NUMPY (if numpy is available)
                           additionally data might be packed with gzip
        :return: FrameHeader object that should be used for the following data transfer
        """
        chl = self._write_chls[chl_id_or_name]
        chl_id = chl.id
        t_id = self.srv_rpc.start_data_transfer_on_channel(self.client_id, chl_id)
        if DEBUG: print(
            'New data stream shall be created on channel: %s, transaction_id: %s' % (repr(chl), repr(t_id)))
        header = FrameHeader(CT_DATA,
                             coder_type,
                             transaction_id=t_id,
                             counter=1,
                             channel_id=chl_id,
                             address_size=self._address_size)
        header.set_counter(0)
        return header

    def end_data_transfer(self, header):
        """
        Send the end frame to stop the data transfer
        :param header: FrameHeader used for the data transfer stream that should be stopped
        :return:
        """
        end_header = FrameHeader(CT_DATA_END,
                                 coder_type=header._coder_type,
                                 channel_id=header._channel_id,
                                 transaction_id=header._transaction_id,
                                 client_id=header._client_id,
                                 address_size=header._address_size)
        frame = Frame(end_header, 'END DATA')
        t = asyncio.run_coroutine_threadsafe(send_msg(self._writer, frame.encode()), self._loop)
        t.result()

    def put_data(self, header, item, wait_for_active=False):
        """
        stream a data package into the channel
        :param header: FrameHeader used for this stream (return of start_data_transfer())
        :param item: dat item that should be send (depending on used encoder any python object might be given here)
        :param wait_for_active: If the channel is inactive (no reader subscribed the channel) we can wait for a reader
                                or skip.
                                * True - we will wait for a reader
                                * False - we do not transfer the data we skip the transfer
        :return:
        """
        t = asyncio.run_coroutine_threadsafe(self.async_put_data(header, item, wait_for_active), self._loop)
        return t.result()

    async def async_put_data(self, header, item, wait_for_active=False):
        """
        async variant of stream a data package into the channel
        :param header: FrameHeader used for this stream (return of start_data_transfer())
        :param item: dat item that should be send (depending on used encoder any python object might be given here)
        :param wait_for_active: If the channel is inactive (no reader subscribed the channel) we can wait for a reader
                                or skip.
                                * True - we will wait for a reader
                                * False - we do not transfer the data we skip the transfer
        """
        # we expect that the counter in the given header is not used yet and we will increase after sending the frame
        if self._write_chls[header.channel_id].is_active:
            data_bytes = Frame(header, item).encode()
            await send_msg(self._writer, data_bytes)
        else:
            if wait_for_active:
                data_bytes = Frame(header, item).encode()
                while 1:
                    await asyncio.sleep(1)
                    if self._write_chls[header.channel_id].is_active:
                        await send_msg(self._writer, data_bytes)
                        break
            else:
                print('skipped', self._write_chls[header.channel_id].name)
        header.count_up()
        return header

    def get_data(self, chl_id_or_name, timeout=None):
        """
        get data from the subscribed channel
        :param chl_id_or_name: channel name
        :param timeout: optional timeout in seconds
        :return: received item object (already decoded)
        """
        chl = self._read_chls[chl_id_or_name]
        header, payload = chl.buffer.get_wait(timeout=timeout)
        frame = Frame(header)
        frame.decode_payload(payload)
        args = frame.args
        if args and len(args) == 1:
            args = args[0]
        return header, args

    async def async_get_data(self, chl_id_or_name):
        """
        async variant of get data from the subscribed channel
        :param chl_id_or_name: channel name
        :param timeout: optional timeout in seconds
        :return: received item object (already decoded)
        """

        chl = self._read_chls[chl_id_or_name]
        await asyncio.sleep(0.0001)  # give other threads possibility to be executed
        header, payload = await chl.buffer.get()
        frame = Frame(header)
        frame.decode_payload(payload)
        args = get_args(*frame.args)

        return header, args

    def get_read_channel_buffer(self, chl_id_or_name):
        '''
        delivers the read buffer directly one can use the buffer get methods to take the data out
        The object can be used for direct queue access!
        :param chl_id_or_name: name or channel id
        :return: Buffer object for channel reading
        '''
        return self._read_chls[chl_id_or_name].buffer

    def stop_client(self):
        """
        stop the client
        the client will close the server connection
        :return:
        """
        self.srv_rpc.delete_client(self.srv_info.c_id, self.srv_info.control_key)
        time.sleep(0.5)
        self._stop_event.set()
        if self._server_died:
            self.writer = None
            self.reader = None
            self._ = None
            raise StreamError_Connection('Server died.')

    # rpc
    def create_rpc_client_service(self, client_name_or_id):
        """
        create a RPCFactory object for another client to access the rpc methods in this client
        :param client_name_or_id: target client name or ip
        :return:
        """
        back = self.srv_rpc.get_client_info(self.client_id, client_name_or_id)
        #print(client_name_or_id,back)
        new_client_rpc = RPCFactory(back['RPC_METHOD_DESC'],
                                FrameHeader(CT_CMD, CODE_PICKLE,
                                            transaction_id=1,
                                            command_id=3, # we set the default to echo
                                            client_id=back['ID'],
                                            address_size=self.address_size),
                                self._reader, self._writer, self._loop, self.__get_new_transaction)
        # ToDo store internally?
        return new_client_rpc

    # Those are the standard rpc methods that must be available on each client
    # The user can extend the function by creating an own client class which inherits this class as a super class

    def rpc_activate_write_channel(self, chl_name_or_id, activate=True):
        """
        helper method used by the server to activate or deactivate the writing to a channel
        This reduces the network load because if no reader subscribed the channel no data must be send
        :param chl_name_or_id: channel name or id
        :param activate:
                        * True - channel is active
                        * False - channel is inactive
        :return:
        """
        if chl_name_or_id not in self._write_chls:
            raise StreamError_Channel('Unknown channel %s given!' % chl_name_or_id)
        chl = self._write_chls[chl_name_or_id]
        chl.set_activate(activate)
        print('Activation request received:', chl.name, activate)
        return True

    def rpc_echo(self, *args,**kwargs):
        """
        With this method one can test the rpc connection
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :param args: arguments list to be echoed into the return
        :param kwargs:  keyword arguments dict to be echoed into the return

        :return args,kwargs
        """
        return args,kwargs
