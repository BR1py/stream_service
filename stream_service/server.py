"""
This code is taken from PyPi stream_service package
https://github.com/BR1py/stream_service

Stream Buffer Server main module

The naming in this module is always from the perspective of the server!
client writer -> transport data to the client
client reader -> read data from the client

The address_size can be defined in the server. It gives the number of bytes used for all kinds of addressing:
server_ids
channel_ids
client_ids
rpc_command_ids
data_package counter
transaction_ids
...

The packages exchanged have the following form

1. server_address for mirror servers (=0 if no server function should be mirrored)
2. channel_id (=0 cannot be used directly this is the server channel which is used for channel control, server control and rpc calls)
3. type byte: byte contains container type and code information
4. data package counter
5. rest of the package is the payload
# if container type is command
4. command id
5. rest are the command arguments

About RPC calls

the rpc calls are handled via server channel 0
Each client can call a exposed rpc function of the other client here.
All calls are asynchronous which means if the call request is send a transaction id is returned.
After the call is calculated the return is given with the related transaction id to the client.

The client object itself contains async or snyc calls handling for sync calls the Client will block the exeution and
wait for the return with the right transaction id.

"""
from __future__ import absolute_import

import asyncio
import traceback
from threading import Lock
from multiprocessing import Process
from asyncio import StreamReader, StreamWriter, QueueFull
import uuid
import time
import threading
from .lib import *
from .lib.frame import *
from .lib.buffer import *

# from .stream_commands import *
DEBUG = False  # Variable used for development proposes (more print outputs given)
               # curious leads into an exception when set -> I must investigate

__package__ = 'stream_service'
__version__ = '0.1.0'
__licence__ = 'MIT'
__author__ = 'B.R.'
__url__ = 'https://github.com/BR1py/stream_services'
__description__ = 'Client/Server implementation for data streaming in channels based on python >= 3.5'


# helper classes for the server:


class ServeClient():
    __slots__ = ('_peername', '_id', '_name', '_control_key', '_async_reader', '_async_writer', '_rpc_methods_descs',
                 '_read_task', '_write_task', '_owned_chl_ids', '_write_chls', '_read_bufs', '_alive', '_rpc_methods',
                 '_send_cnt', '_id_bytes')

    def __init__(self, peername, client_id, id_bytes, name, control_key, async_reader, async_writer, rpc_methods_descs):
        """
        Object contains all client related information for each client that is connected to the sever

        :param peername: Adresse of the client host
        :param client_id: unique id of the client
        :param id_bytes: client id already converted to bytes
        :param name: name of the client
        :param control_key: authentication key to delete the client
        :param async_reader: asyncio StreamReader of the client (communication channel from the client to the server
        :param async_writer: asyncio StreamWriter of the client (communication channel to the client from the server
        :param rpc_methods_descs: rpc method description of the client (to be shared with other clients)
        """
        self._peername = peername
        self._id = client_id
        self._name = name
        self._control_key = control_key
        self._id_bytes = id_bytes
        self._async_reader = async_reader
        self._async_writer = async_writer
        self._rpc_methods_descs = rpc_methods_descs

        self._read_task = None # here we store the client read task
        self._owned_chl_ids = set() # ServeChannel object owned by this client
        self._write_chls = {} # ServeChannel writers subscribed by the client
        self._read_bufs = set() # ReadBuffer for the ServeChannels the client subscribed for reading
        self._alive = True # alive flag for the _read_task
        self._send_cnt = 0 # counter for statistics for the send frames

    # make some private elements available
    @property
    def id(self):
        return self._id

    @property
    def id_bytes(self):
        return self._id_bytes

    @property
    def name(self):
        return self._name

    @property
    def is_alive(self):
        return self._alive

    @property
    def writer(self):
        return self._async_writer

    @property
    def control_key(self):
        return self._control_key

    def stop(self):
        """
        stop the read_task by switching the alive flag to False
        (The task will stop after timeout of the read function is reached or when a read item is received)
        """
        self._alive = False

    def register_owned_chl(self, chl):
        """
        Helper to register a new owned channel (used in the rpc_create_new_channel() method of the server)
        (An owned channel is automatically appended as a writer channel)
        :param chl: ServeChannel object that is owned by the channel
        """
        chl_id = chl.id
        self._owned_chl_ids.add(chl_id)
        self._write_chls[chl_id] = chl
        self._write_chls[chl.name] = chl

    def set_read_task(self, task):
        """
        Helper method to set the read task (used in the _connect() method of teh server)
        :param task: give the asyncio task with the writer
        :return:
        """
        self._read_task = task

    def set_send_cnt(self, cnt):
        """
        helper to set the actual send count (used by the reader task)
        :param cnt: new counter value
        """
        self._send_cnt = cnt

    def get_async_stream_objects(self):
        """
        Helper to get the clients asyncio StreamReader and StreamWriter of the client
        :return: StreamReader,StreamWriter
        """
        return self._async_reader, self._async_writer

    def get_rpc_method_desc(self):
        """
        get the rpc method description of the client
        :return:
        """
        return self._rpc_methods_descs

class ServeChannel():
    __slots__ = ('_parent', '_id', '_name', '_owner', '_is_public', '_is_single_in', '_lifetime',
                 '_read_bufs', '_control_key', '_transaction_ids', '_address_mask', '_read_writers',
                 '_channel_task', '_task_trigger', '_task_alive', '_loop', '_slot_time', '_write_cnt',
                 '_next_send_time', '_is_slot_time_fixed', '_address_size', '_statistic_chl', '_event_cnt',
                 '_statistic_chl_fill_rate', '_writer_clients', '_ta_ids_hdl')

    def __init__(self, chl_id, name, owner, is_public, is_single_in, lifetime, loop, address_size,
                 fixed_slot_time=None):
        """
        This class represents a channel on the server and it contains an own task to send the data to the subscribers
        It's the most important class in the server!

        :param chl_id: channel id
        :param name: channel name
        :param owner: channel owner (client that has created the class (this client must also delete the channel))
        :param is_public: Will the channel be published by the server (server gives an info list if requested)
        :param is_single_in: Only one writer is allowed for this channel
        :param lifetime: temporary channels can be created too they will be eliminated after lifetime (in seconds)
        :param loop: event loop of the server
        :param address_size: address size used on this server
        :param fixed_slot_time: channel has a fixed slot time (load balancing disabled for this channel)
        """
        self._id = chl_id
        self._name = name
        self._owner = owner
        self._is_public = is_public
        self._is_single_in = is_single_in
        self._lifetime = lifetime
        self._address_size = address_size
        # Create a mask based on address_size:
        mask = 0
        for i in range(address_size):
            mask = (mask << 8) + 0xFF
        self._address_mask = mask
        self._control_key = uuid.uuid4().bytes # need for channel delete
        self._ta_ids_hdl = IdsHandler(address_size) # Generater for Transaction ids used on this channel
        self._read_bufs = {} # subscribed read buffers
        self._writer_clients = {} # subscribed writers
        self._task_alive = True  # alive flag for the channel read task will be set to false to stop the task
        self._loop = loop # event loop that should be used (the one from the sevrer)
        self._task_trigger = asyncio.Event(loop=loop) # activity trigger for the read task used by client reader tasks to trigger the channel writer task
        self._channel_task = self._loop.create_task(self._write_task()) # writer task is started here
        if fixed_slot_time is None: # a fixed slot time can be set for a channel if not the slottime is set depending of the number of channels on the server
            self._slot_time = 2  # default is 2 seconds
            self._is_slot_time_fixed = False
        else:
            self._slot_time = fixed_slot_time  # default is 2 seconds
            self._is_slot_time_fixed = True
        self._next_send_time = 0 # timing used for load balancing
        self._write_cnt = 0 # statistics counter
        self._event_cnt = 0 # statistics counter
        self._statistic_chl = None # statistics channel (if set statistic data from the channel is put in this channel
        self._statistic_chl_fill_rate = 1 # helper for statistic channel (gives the fill rate for this channel

    # publish some internals

    @property
    def is_slot_time_fixed(self):
        return self._is_slot_time_fixed

    @property
    def write_cnt(self):
        return self._write_cnt

    @property
    def last_send_time(self):
        return (self._next_send_time - self._slot_time)

    @property
    def control_key(self):
        return self._control_key

    @property
    def name(self):
        return self._name

    @property
    def id(self):
        return self._id

    @property
    def is_single_in(self):
        return self._is_single_in


    def add_client_writer(self, client):
        """
        add a new writer to the channel
        :param client: writer client
        :return:
        """
        self._writer_clients[client.id] = client

    def remove_client_writer(self, client_id):
        """
        remove a writer from the channel
        :param client_id: client id that should be removed
        :return:
        """
        if client_id in self._writer_clients:
            del self._writer_clients[client_id]
            return True
        return False

    def add_client_reader(self, buffer):
        '''
        add a given read buffer to the channel
        :param buffer: Buffer() object (we expect that the additional kwargs in the object a set as expected ->
                       this is normally ensured in the rpc method of the server)
        :return:
        '''
        client_id = buffer.client.id
        if client_id in self._read_bufs:
            raise StreamError_Channel(
                'Given client_id: %s exists already as reader in the channel, delete first!' % repr(client_id))
        if len(self._read_bufs) == 0:
            self._notify_active(True)
        self._read_bufs[client_id] = buffer

    def remove_client_reader(self, client_id):
        """
        remove a client reader from the channel
        :param client_id:
        :return:
        """
        if client_id in self._read_bufs:
            raise StreamError_Channel(
                'Given client_id: %s not found in channel readers, cannot delete!' % repr(client_id))
        read_buffer = self._read_bufs.pop(client_id)
        # we clear the buffer and we send as last item an end data object
        read_buffer.clear()
        header = FrameHeader(CT_DATA_END, CODE_MARSHAL, address_size=self._address_size)
        read_buffer.put_nowait(header, b'END DATA')
        if len(self._read_bufs) == 0:
            self._notify_active(False)
        return read_buffer

    def _notify_active(self, active=True):
        """
        signalize if put_data activity is needed for this channel
        To avoid useless traffic on the network the writers for this channel can be deactivated
        in case no subscriber listens to the channel
        :param active: True - activity on, False activity of
        :return:
        """
        for client in self._writer_clients.values():
            frame = Frame(FrameHeader(CT_CMD, CODE_MARSHAL, command_id=1,
                                      # rpc_command for switch the activity on clients must have id==1 !
                                      client_id=client.id), self.id, active)
            task = asyncio.run_coroutine_threadsafe(send_msg(client._async_writer, frame.encode()), self._loop)
            task.result()

    def set_slot_time(self, slot_time):
        """
        slot time can be adapted in case of server load or to prioritize a channel
        :param slot_time: slot time for sending
        :return:
        """
        if not self._is_slot_time_fixed:
            self._slot_time = slot_time

    def set_statistic_channel(self, chl, fill_rate):
        """
        The statistic channel can be activated on the server to track the load,
        with this method the channel is set and activated for this channel
        :param chl: statistics channel
        :param fill_rate: fill_rate for the statistic channel
        :return:
        """
        self._statistic_chl = chl
        self._statistic_chl_fill_rate = fill_rate

    def stop_channel(self):
        """
        Will stop the channel read task (cannot be restarted!)

        :return:
        """
        self._task_alive = False
        self._task_trigger.set()

    async def _write_task(self):
        """
        This tasks ensures that the data is send only in the given timeslot
        Therefore we do not send directly we buffer in queues
        """
        # prepare some locals for quicker access
        # (The task should run as quick as possible to reduce the load on the server)
        read_buffers = self._read_bufs
        buf_infos = []
        statistics_channel_id = None
        statistic_header = None
        statistics_channel_fill_rate = 1
        last_statistics_event_cnt = -1
        while self._task_alive: # Main write loop
            # We wait for a trigger event signalize that we have new data
            await self._task_trigger.wait()
            # clear the event to be usable again
            self._task_trigger.clear()
            # print('Trigger received')
            await asyncio.sleep(0.001)  # give other tasks a slot
            if self._statistic_chl is not None:
                # if statistics channel is active we measure some buffer statistics before we put items out
                buf_infos = [(buf.client.name, buf.qsize(), buf.full_cnt, buf.fill_cnt) for k, buf in
                             self._read_bufs.items() if
                             type(k) is int]
                statistics_active = True
            else:
                statistics_active = False
            # init some variables for break conditions
            cnt = 0
            t = self._loop.time()
            end_time = t + self._slot_time
            # take data from the buffers and send them to the clients:
            while self._task_alive:
                all_empty = True
                for buffer in list(read_buffers.values()):
                    if not buffer.empty():
                        await asyncio.sleep(0.001)  # give other tasks a slot
                        tx_data = await buffer.get() # take buffer data out (we might use here get_nowait too but hen we must handle the exception)
                        buffer.task_done()  # we free the queue for next operation
                        await asyncio.sleep(0.001)  # give other tasks a slot
                        await self._send_encoded(buffer.client._async_writer, tx_data) # send data to the client
                        # print('Send to client',tx_data)
                        cnt = (cnt + 1) & 0b11111111
                        all_empty = False
                # check break conditions:
                # we break earliest after sending at least one item per read_buffer
                if end_time > self._loop.time():
                    # time slot over
                    self._write_cnt = cnt
                    self._next_send_time = self._loop.time() + self._slot_time
                    await asyncio.sleep(0.001)
                    # set trigger (we are not finished)
                    self._task_trigger.set()
                    break
                if all_empty:
                    # all read buffers are empty -> Fine no data lost in this case!
                    self._write_cnt = cnt
                    self._next_send_time = self._loop.time() + self._slot_time
                    await asyncio.sleep(0.1)  # here we wait a bit longer (the buffers are well cleaned)
                    break
            if statistics_active is not None:
                # if statistics channel is active we send statistics data
                # this will slow down the task!
                if not last_statistics_event_cnt == cnt:
                    try:
                        # We put this in try except because in a corner-case the statistic channel might be set to None
                        # during this operation!
                        chl_id = self._statistic_chl.id
                        if statistics_channel_id != chl_id:
                            # channel changed!
                            # create new header
                            statistic_header = FrameHeader(CT_DATA, CODE_MARSHAL, channel_id=chl_id)
                            statistics_channel_id = chl_id
                            if statistics_channel_id == self.id:
                                # we do not use a fill_rate on the statistics channel itself
                                statistics_channel_fill_rate = 1
                            else:
                                statistics_channel_fill_rate = self._statistic_chl_fill_rate
                        if not cnt % statistics_channel_fill_rate:
                            info = '%f %s(%s): send_count: %i; event_count: %i; buffer sizes/full_cnt/fill_cnt: %s\n' % (
                                t, self._name, self._id, cnt, self._event_cnt, repr(buf_infos))
                            frame = Frame(statistic_header, info)
                            self._statistic_chl.put_nowait(CT_DATA, frame.encode())
                            last_statistics_event_cnt = cnt
                    except:
                        pass


    def get_new_transaction_id(self):
        """
        We do not have transaction objects on server like on the clients but we use transaction ids in some cases
        Here we can create unique ids
        Will raise an Exception IndexError (or ValueError) in case no more id is available
        :return:
        """
        return self._ta_ids_hdl.get_new_id()

    def clear_buffers(self):
        """
        clear all the buffers
        :return:
        """
        for read_buffer in list(self._read_bufs.values()):
            read_buffer.clear()

    def put_nowait(self, container_type, data):
        """
        put data function used by the client writers
        Inside we distribute the data to the buffers
        :param container_type: This info is already parsed by the reader thread and we reuse it here
        :param data: raw data received for this channel
        :return:
        """
        force = False
        if container_type == CT_DATA_END:
            # DATA :END RECEIVED!
            # In case an data end is received the transaction id must be freed for reuse
            header = FrameHeader(CT_DECODE, data, self._address_size)
            self._ta_ids_hdl.free_id(header.transaction_id)
            force=True
        self._event_cnt = (self._event_cnt + 1) & 0xFFFFFFFF #statistics counter
        for read_buf in list(self._read_bufs.values()): # put data into the buffers
            read_buf.put_nowait(data,force=force)
        if self._loop.time() > self._next_send_time:
            # we set the task trigger event to signalize the send task that we have new data to be send
            self._task_trigger.set()

    async def put(self, container_type, data):
        """
        async version of put_nowait but this is not used at the Moment
        :param container_type:
        :param data:
        :return:
        """
        if container_type == CT_DATA_END:
            # DATA :END RECEIVED!
            header = FrameHeader(CT_DECODE, data, self._address_size)
            self._ta_ids_hdl.free_id(header.transaction_id)
        for read_buf in list(self._read_bufs.values()):
            await read_buf.put(data)
        if self._loop.time() > self._next_send_time:
            self._task_trigger.set()

    async def _send_encoded(self, writer, tx_data):
        '''
        helper method to encode the send data
        the encoding is done depending on the tx_data type given

        :param writer: writer
        :param tx_data: tx_data can be bytes or tuple header and payload or a Frame() object
        :return:
        '''
        if type(tx_data) is tuple:  # we have header object and payload!
            await send_msg(writer, tx_data[0].encode() + tx_data[1])
        elif type(tx_data) is bytes:  # we have raw bytes
            await send_msg(writer, tx_data)
        else:  # we have a tx_frame
            await send_msg(writer, tx_data.encode())


# Server:

class StreamChannelServer_Process(Process):

    def __init__(self, ip_address=None, port=None, ssl=None,
                 address_size=2, log_file=None, quiet=False, name='StreamDataServer',
                 client_authenticate=b'STRM_Client',
                 # scale and performance parameters
                 max_clients=100,
                 max_slottime=30,
                 min_slottime=2,
                 default_limit=2 ** 16,
                 statistic_channel_name='ServerChannelStatistics',
                 daemon=False, *kwargs):
        """
        Server Process - main stream service server class
        Important parts of the class init are executed in the moment the process is started (see run() method)
        :param ip_address: network IP address that will be used by the server
        :param port: network port that will be used by the server
        :param ssl: optional ssl configuration (search for  sll on asyncio servers for more details)
        :param address_size: address_size used on the server default is 2 bytes
                             e.g. if you need more channels this can be extended but the frame size increases in this case!
        :param log_file: USe a log file to store the log messages of the server (the file will be limited in size)
        :param quiet: run in quiet mode (no log outputs printed into the terminal)
        :param name: name of the server
        :param client_authenticate: authentication bytes for clients (need for connections)
        :param max_clients: maximum number of clients accepted by the server
        :param max_slottime: maximum slot time might be increased depending on the machine the server is running on;
                             Used for load balancing, it's the upper limit for the slot-time a channel has to send data
                             and it is divided by the number of channels on the server
                             (normally the parameter should be kept as it is)
        :param min_slottime: minimum slot time might be increased depending on the machine the server is running on
                             Used for load balancing; if a lot of channels running on the server the slot-time
                             for writing is fixed at this value
                             (normally the parameter should be kept as it is
        :param default_limit: This is the size of the StreamWriter buffer (normally this should not be changed)
        :param statistic_channel_name: name of the statistic channel
        :param daemon: run the process in daemon mode
        :param kwargs: additional arguments to give to the asyncio server
        """
        if port is None:
            port = DEFAULT_BUFFER_STREAM_MGR_PORT
        self._address = (ip_address, port)
        self._kwargs=kwargs
        self._address_size = address_size
        self._max_slottime = max_slottime
        self._min_slottime = min_slottime
        self._default_limit = default_limit
        self._current_max_writer_size = self._default_limit
        self._current_slot_delta_time = self._max_slottime
        self._ssl = ssl
        self._statistic_channel_name = statistic_channel_name
        self._statistic_chl = None
        self._statistic_chl_fill_rate = 1
        mask = 0
        for i in range(address_size):
            mask = (mask << 8) + 0xFF
        self._address_mask = mask
        # prepare random generator
        super().__init__(name=name, daemon=daemon)
        self._srv_lock = None  # lock must be instanced in run
        self._server_ready = False
        self._server_active = True
        self._logger = Logger(log_file=log_file, quiet=quiet)
        self._write_log = self._logger.write_log
        if type(client_authenticate) is not bytes:
            raise TypeError('Given client_authenticate parameter must be of type bytes')

        self._client_authenticate = client_authenticate
        self._clients = {}
        self._max_clients = max_clients
        self._channels = {}
        # the following two items are not used in the channels only on the server for client rpc calls
        self._server_transactions={}
        self._server_transaction_id_hdl = IdsHandler(address_size)

        # id handlers
        self._client_id_hdl = IdsHandler(address_size)
        self._channel_id_hdl = IdsHandler(address_size)

        self._rpc_methods_descs = []
        self._rpc_methods = []

    @property
    def address_size(self):
        return self._address_size

    def run(self):
        try:
            # post initialization:
            self._write_log('Start StreamChannelServer (Version %s)' % __version__)
            self._srv_lock = Lock()
            self._rpc_methods_descs, self._rpc_methods = rpc.RPCFactory.class_init_rpc_functions(self, first_items=[
                'rpc_authenticate_client'],address_size=self._address_size)
            #print(self._rpc_methods_descs)
            #Event loop
            self._loop = loop = asyncio.get_event_loop()
            #self._loop.set_debug(DEBUG)
            #prepare the asyncio server:
            co_rtn = asyncio.start_server(self.client_connect, host=self._address[0], port=self._address[1], loop=loop,
                                          limit=self._default_limit, ssl=self._ssl,**self._kwargs)
            self._server = loop.run_until_complete(co_rtn) # this will be executed in the moment the event loop is started!
            # Serve requests until Ctrl+C is pressed
            self._write_log('Serving on {}'.format(self._server.sockets[0].getsockname()))
            self._server_ready = True
            # start the event loop and run for ever!
            loop.run_forever()
        except:
            self._write_log('Exception in StreamChannelServer:\n')
            self._write_log(''.join(traceback.format_exc()))

    async def client_connect(self, reader: StreamReader, writer: StreamWriter):
        '''
        request from a client for a new connection
        this is the asyncio standard connect functionality
        The return of this method is send to the client
        Note:: In this method the internal client related objects are created especially the two tasks
        for reading and sending from/to the client
        State: code tested
        :param reader: client StreamReader object
        :param writer: client StreamWriter object
        '''
        rx_header = None
        address_size = self._address_size
        try:
            # get the peername
            peername = writer.get_extra_info('peername')
            self._write_log('client_connect(): Client Request from %s' % str(peername))
            # is the server full?
            # we must divide by 2 because we store each client two times in the dict (id and name)!
            if len(self._clients) / 2 >= self._max_clients:
                self._write_log('client_connect(): Client Request denied, to many clients connected')
                # negative response server full
                await send_msg(writer, b'0')
                return
            # Server starts communication and send address_size and seed
            # here we put some small security in
            # for more one must use ssl!
            seed = uuid.uuid4().bytes + uuid.uuid4().bytes
            raw_key = get_raw_key(seed, self._client_authenticate)
            if DEBUG: print('client_connect(): seed+key pair is:', seed, raw_key)
            tx_bytes = address_size.to_bytes(1, BO) + seed
            if DEBUG: print('client_connect(): First message send is:', tx_bytes)
            await send_msg(writer, tx_bytes)
            # waiting for clients response with authentication info
            rx_bytes = await read_msg(reader)
            if DEBUG: print('client_connect(): Package raw bytes received:', rx_bytes)
            # decode received answer
            rx_frame = Frame(FrameHeader(CT_DECODE, address_size=address_size), rx_bytes)
            rx_header = rx_frame.header
            if DEBUG: print('client_connect(): Frame received:', repr(rx_frame))
            # we already prepare the response header here, will be used in the exceptions too!
            tx_header = FrameHeader(container_type=CT_RETURN,
                                    coder_type=CODE_PICKLE,
                                    transaction_id=rx_header.transaction_id,
                                    address_size=address_size)
            if rx_header.container_type != CT_SRV_CMD and rx_header.command_id != 0:
                self._write_log('client_connect(): Client Request denied, wrong connect command send')
                raise StreamError_Command('client_connect(): Wrong connect command send')
            # authenticate client
            args = rx_frame.args
            back = self.rpc_authenticate_client(*args, raw_key=raw_key)
            if back[0] != 0:
                self._write_log('client_connect(): Client Request denied, authentication failed')
                raise StreamError_Authenticate('client_connect(): Client Request denied, authentication failed')
            # extract other data
            client_name = back[1]
            rpc_methods_descs = back[2]
            if DEBUG: print('client_connect(): Client authentication finished')
            # create a new client object
            client_id = self._client_id_hdl.get_new_id()
            if client_id is None:
                self._write_log('client_connect(): Client Request denied, to many clients connected')
                raise StreamError_Authenticate('client_connect(): Client Request denied, no free id found')
            control_key = uuid.uuid4().bytes
            id_bytes = client_id.to_bytes(address_size, BO)
            client = ServeClient(peername, client_id, id_bytes, client_name,
                                 control_key, reader, writer, rpc_methods_descs)
            # enter client in the client dict
            with self._srv_lock:
                # we add the client two times in the client dict because we like to reach it quick per name or id
                self._clients[client_id] = client
                self._clients[client_name] = client
                client.set_read_task(self._loop.create_task(self._client_task_loop(client)))
                # rescale the writer limits:
                l = len(self._clients) / 2
                self._current_max_writer_size = self._default_limit / l
                self._current_slot_delta_time = self._max_slottime / l
                if self._current_slot_delta_time < self._min_slottime:
                    self._current_slot_delta_time = self._min_slottime
                # asyncio.gather(client.read_task,client.read_task)
            if DEBUG: print('client_connect(): Server related Client object created', client)
            # Create the Client default channel
            # create positive response
            tx_frame = Frame(tx_header, client_id, control_key, self._rpc_methods_descs)
            if DEBUG: print('client_connect(): Positive response pkg build', tx_frame)
            tx_bytes = tx_frame.encode()
            if DEBUG: print('client_connect(): Positive response send', tx_bytes)
            # time.sleep(2)  # give the loops some time to come up
            # give the feedback that client setup is finished
            await send_msg(writer, tx_bytes)
            self._write_log('client_connect(): Client connection established all server internal objects created')
        except Exception as e:
            # Catch exception and if possible send back to the client
            self._write_log('client_connect(): Exception:')
            self._write_log(repr(e))
            self._write_log(traceback.format_exc())
            if rx_header is None:
                # give unspecific feedback
                try:
                    await send_msg(writer,
                                   Frame(FrameHeader(CT_EXCEPTION, CODE_MARSHAL),
                                         repr(e),
                                         traceback.format_exc()).encode())
                except:
                    self._write_log(
                        'client_connect(): Unspecific Exception return not send to client; sub-exception traceback: '
                        '%s' % ''.join(traceback.format_exc()))
            else:
                # give specific feedback
                try:
                    await send_msg(writer,
                                   Frame(FrameHeader(CT_EXCEPTION, CODE_MARSHAL,
                                                     transaction_id=rx_header.transaction_id,
                                                     command_id=rx_header.command_id,
                                                     channel_id=rx_header.channel_id,
                                                     client_id=rx_header.client_id,
                                                     counter=rx_header.counter),
                                         repr(e), ''.join(traceback.format_exc())).encode())
                except:
                    self._write_log('client_connect(): Exception return not send to client; sub-exception traceback: '
                                    '%s' % ''.join(traceback.format_exc()))

    async def _send_encoded(self, writer, tx_data):
        '''
        helper method to encode the send data
        the encoding is done depending on the tx_data type given

        :param writer: writer
        :param tx_data: tx_data can be bytes or tuple header and payload or a Frame() object
        :return:
        '''
        if type(tx_data) is tuple:  # we have header object and payload!
            await send_msg(writer, tx_data[0].encode() + tx_data[1])
        elif type(tx_data) is bytes:  # we have raw bytes
            await send_msg(writer, tx_data)
        else:  # we have a tx_frame
            await send_msg(writer, tx_data.encode())

    async def _client_task_loop(self, client):
        # This is the client reader loop
        # prepare some locals for quicker access
        reader, writer = client.get_async_stream_objects()
        address_size = self._address_size
        id_bytes = client.id_bytes
        if client._alive:
            self._write_log('Client %s read from client loop/task started' % client.name)
        none_cnt = 0
        loop_cnt = 0
        first_id_slice = slice(2, self._address_size + 2)
        second_id_slice = slice(2 + self._address_size, 2 * self._address_size + 2)
        while client.is_alive:
            # loop_cnt+=1
            await asyncio.sleep(0.001)  # this allows other threads to be executed in parallel
            rx_header = None
            try:
                rx_bytes = False
                try:
                    rx_bytes = await asyncio.wait_for(read_msg(reader),
                                                      10)  # timeout is used just to clean the client if it no longer exists
                except:
                    rx_bytes = False
                await asyncio.sleep(0.001)  # this allows other threads to be executed in parallel
                # print('RX',rx_bytes)
                if rx_bytes is None:
                    # in case of None the client might be died but we give the loop some more tries
                    none_cnt += 1
                    if none_cnt > 10:
                        # client died!
                        client.stop()
                        self._write_log('Client %s/%s connection error, connection will be killed from server' % (
                            str(client.name), str(client.id)))
                        break
                    continue
                elif rx_bytes is not False:
                    if DEBUG: print('Frame received:', rx_bytes)
                    # we got a valid frame we reset the none counter:
                    none_cnt = 0
                    # Take data from clients and put it in the write queues
                    # extract the container type
                    container_type = rx_bytes[0] & 0b1111
                    # depending of the container type we react
                    if container_type == CT_DATA or container_type == CT_DATA_END:
                        chl_id = int.from_bytes(rx_bytes[first_id_slice], BO)
                        # channel related data we put into the channel send queue
                        chl = self._channels.get(chl_id)
                        if chl is None:
                            raise StreamError_Channel('Given channel_id %s is unknown or not writeable' % (chl_id))
                        await asyncio.sleep(0.001)  # this allows other threads to be executed in parallel
                        try:
                            chl.put_nowait(container_type, rx_bytes)
                        except:
                            print(''.join(traceback.format_exc()))
                        await asyncio.sleep(0.001)  # this allows other threads to be executed in parallel
                        if DEBUG: print('Data frame received and put in channel %i:' % chl_id, rx_bytes)
                    elif container_type == CT_RETURN or container_type == CT_CMD:
                        # here we route from one client to another client
                        if rx_bytes[1] & 0b10 == 0:
                            # no client given -> server request response
                            # we ignore
                            continue
                        target_client_id = int.from_bytes(rx_bytes[first_id_slice], BO)
                        if target_client_id == 0:
                            # this is a return to a server call return we ignore
                            # e.g. from channel activation calls
                            continue
                        if target_client_id is None or target_client_id == client.id:
                            # no client_id or this client_id is given we just send to the client
                            await asyncio.sleep(0.001)  # this allows other threads to be executed in parallel
                            await send_msg(writer, rx_bytes)
                        else:
                            # the return is target to another client
                            # we exchange the client_id with this client to mark the source and send it ot the target
                            target_client = self._clients.get(target_client_id)
                            if target_client is not None:
                                # in following step client_id is changed to source
                                await asyncio.sleep(0.001)  # this allows other threads to be executed in parallel
                                # exchange target client id with source client id:
                                new_bytes = b''.join([rx_bytes[:first_id_slice.start],
                                                      client.id_bytes,
                                                      rx_bytes[first_id_slice.stop:]
                                                      ])
                                await send_msg(target_client._async_writer, new_bytes)
                            else:
                                raise StreamError_Target(
                                    'Unknown target (%s) given, return cannot be send' % (repr(target_client_id)))
                    elif container_type == CT_SRV_CMD:
                        t = threading.Thread(target=self._execute_rpc_threaded, args=(client, rx_bytes))
                        t.start()
                        continue
                        rx_header = FrameHeader(CT_DECODE, rx_bytes, address_size=address_size)
                        rx_payload = rx_bytes[len(rx_header):]
                        # server rpc calls are executed here
                        try:
                            srv_cmd = self._rpc_methods[rx_header.command_id]
                        except IndexError:
                            srv_cmd = None
                        if srv_cmd is None or srv_cmd == self.rpc_authenticate_client:
                            # the authentication command (cmd_id==0) is protected and is used only in the client_connect()
                            raise StreamError_Command('StreamServer - RPC no/unknown command given')
                        # prepare the command arguments
                        rx_frame = Frame(rx_header)
                        rx_frame.decode_payload(rx_payload)
                        # we replace the client argument with the client object
                        if len(rx_frame.args) > 0:
                            args = [client] + list(rx_frame.args[1:])
                        else:
                            args = [client]
                        kwargs = rx_frame.kwargs
                        if 'client_id' in kwargs:
                            del kwargs['client_id']
                        # execute the command
                        back = srv_cmd(*args, **kwargs)
                        # prepare the return frame
                        tx_header = FrameHeader(container_type=CT_RETURN,
                                                coder_type=CODE_MARSHAL,
                                                transaction_id=rx_header.transaction_id,
                                                address_size=address_size)
                        tx_frame = Frame(tx_header, back)
                        await asyncio.sleep(0.001)  # this allows other threads to be executed in parallel
                        # put it in the write queue
                        await send_msg(writer, tx_frame.encode())
                        if DEBUG: print('tx_frame send to client:', tx_frame)

            except Exception as e:
                e_repr = repr(e)
                tb = ''.join(traceback.format_exc())
                # Catch read exceptions and give feedback
                self._write_log('Client RX-Loop: Exception:')
                self._write_log(e_repr)
                self._write_log(repr(tb))
                # exceptions are send directly not via writer loop to the client
                if rx_header is None:
                    # no header received we give unclear feedback
                    try:
                        await send_msg(writer,
                                       Frame(FrameHeader(CT_EXCEPTION, CODE_MARSHAL), repr(e), repr(tb)).encode())
                    except:
                        self._write_log('LOOP read_from_client(): Unspecific Exception not send to client; '
                                        'sub-exception traceback: %s' % ''.join(traceback.format_exc()))
                else:
                    # header received we give specific feedback
                    try:

                        await send_msg(writer,
                                       Frame(FrameHeader(CT_EXCEPTION, CODE_MARSHAL,
                                                         transaction_id=rx_header.transaction_id,
                                                         command_id=rx_header.command_id,
                                                         channel_id=rx_header.channel_id,
                                                         client_id=rx_header.client_id,
                                                         counter=rx_header.counter),
                                             repr(e), tb).encode())
                    except:
                        self._write_log('LOOP Client: %s: Exception return not send to client; '
                                        'sub-exception traceback: %s' % (client.name, ''.join(traceback.format_exc())))

        self._write_log('Client %s read from client loop/task stopped' % client.name)
        # if this loop is stopped the client is stopped or died
        # we delete the client
        client_id = client.id
        self._write_log('Client %i died' % client_id)
        try:
            self.rpc_delete_client(client, client.control_key)
        except:
            print('Client delete failed:', traceback.format_exc())
            pass

    def _get_writer_bufferlen(self, writer):
        """
        helper delivering the StreamWriter buffer length
        Used for load statistics
        :param writer: StreamWriter object to get the length from
        :return:
        """
        return len(writer.transport._buffer)

    def __get_new_transaction(self, tto=None):
        '''
        a helper function to create a new transaction object
        :param tto: give the transaction_timeout for this transaction
        :return:
        '''
        if tto is None:
            tto = TTO_DEFAULT
        return Transaction(self._loop, self._server_transactions, self._server_transaction_id_hdl, self._address_size, tto,
                           TTO_DEFAULT)

    def _set_channel_slot_times(self):
        """
        helper setting the channel slot explicit
        :return:
        """
        l = len(self._channels) / 2
        if l == 0:
            return
        current_slot_delta_time = self._max_slottime / l
        if current_slot_delta_time < self._min_slottime:
            current_slot_delta_time = self._min_slottime
        for k, c in self._channels.items():
            if type(k) is not int:
                continue
            c.set_slot_time(current_slot_delta_time)

    def _execute_rpc_threaded(self, client, rx_bytes):
        """
        rpc commands on the server are executed in extra thread so that we must not wait in the client read loops
        for the return of the commands
        :param client: client
        :param rx_bytes: bytes that contains the received command content
        :return:
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        rx_header = FrameHeader(CT_DECODE, rx_bytes, address_size=self._address_size)
        rx_payload = rx_bytes[len(rx_header):]
        # server rpc calls are executed here
        try:
            srv_cmd = self._rpc_methods[rx_header.command_id]
        except IndexError:
            srv_cmd = None
        if srv_cmd is None or srv_cmd == self.rpc_authenticate_client:
            # the authentication command (cmd_id==0) is protected and is used only in the client_connect()
            raise StreamError_Command('StreamServer - RPC no/unknown command given')
        # prepare the command arguments
        rx_frame = Frame(rx_header)
        rx_frame.decode_payload(rx_payload)
        # we replace the client argument with the client object
        args = rx_frame.args
        try:
            l = len(args)
        except:
            l = 0
        if l > 0 and (type(args) not in {str, bytes}):
            args = [client] + list(args[1:])
        else:
            args = [client]
        kwargs = rx_frame.kwargs
        if 'client_id' in kwargs:
            del kwargs['client_id']
        # execute the command
        back = srv_cmd(*args, **kwargs)
        # prepare the return frame
        tx_header = FrameHeader(container_type=CT_RETURN,
                                coder_type=CODE_PICKLE,
                                transaction_id=rx_header.transaction_id,
                                address_size=self._address_size)
        # if (hasattr(back, '__iter__')) and (type(back) not in {str,bytes}):
        #    tx_frame = Frame(tx_header, *back)
        # else:
        tx_frame = Frame(tx_header, back)
        # put it in the write queue
        loop.run_until_complete(send_msg(client._async_writer, tx_frame.encode()))
        if DEBUG: print('tx_frame send to client:', tx_frame)

    def close_server(self):
        # Close the server
        if self._server is not None:
            self._server.close()
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(self._server.wait_closed())
                loop.close()
            except:
                pass



    # RPC (Remote Procedure Calls)

    #  rpc methods (exchange via channel 0)
    # the first argument for server rpc commands must be always the client_id this argument will be replaced
    # in the call with the client object so that it is available during the execution

    def rpc_authenticate_client(self, authentication_key, client_name, rpc_methods_descs, raw_key=None):
        '''
        This command is an exception and cannot be called from the client explicitly
        it is just used in the authentication process during the client_connect()
        Note:: Direct usage will lead into an exception!
        :param authentication_key:
        :param client_name:
        :param rpc_methods_descs:
        :param raw_key:
        :return:
        '''
        if authentication_key != raw_key:
            return 'Wrong authentication_key given'
        if DEBUG: print('Client authentication successful')
        return 0, client_name, rpc_methods_descs

    def rpc_echo(self, client_id, *args,**kwargs):
        """
        With this method one can test the rpc connection
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :param args: arguments list to be echoed into the return
        :param kwargs:  keyword arguments dict to be echoed into the return

        :return args,kwargs
        """
        return args,kwargs

    def rpc_create_new_channel(self, client_id, chl_name, public=True, lifetime=None, single_in=True,
                               fixed_slot_time=None):
        """
        This command creates a new channel
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :param chl_name: New channels name
        :param public: Is the channel public or not?
                       * True - is public channel will be shown in channel list
                       * False - channel is not shown in channel list and can only be reached if the name or
                                 ID is known by the client
        :param lifetime: We can create temporary channels too they will automatically deleted after lifetime
        :param single_in: Can the channel have other writers?
                          * True - only owner can write
                          * False - other channel can subscribe for writing
        :param fixed_slot_time: If the parameter is set a fixed slot time in seconds is set.
                                A part of load balancing is switched off in this case.
                                Special channel might be prioritized higher by this parameter
        :return: channel id, channel control key
        """
        client = client_id  # the client object is here already in (see read loop)
        self._write_log('New channel %s request from client %i/%s' % (chl_name, client.id, client.name))
        # the given channel name must be stored as a string (what ever we have received)
        chl_name = str(chl_name)

        if chl_name in self._channels or chl_name == self._statistic_channel_name:
            self._write_log('New channel %s request from client %i/%s - denied; channel exists already' % (
                chl_name, client.id, client.name))
            raise StreamError_Channel('Channel name=%s already in use' % repr(chl_name))
        chl_id = self._channel_id_hdl.get_new_id()
        if chl_id is None:
            self._write_log('New channel %s request from client %i/%s - denied; no free channel' % (
                chl_name, client.id, client.name))
            raise StreamError_Channel('No free channel')
        new_chl = ServeChannel(chl_id, chl_name, client.id, public, single_in, lifetime,
                               self._loop, self._address_size, fixed_slot_time)
        if DEBUG: print('New channel object created', new_chl)
        # we fill the channel dict with both id and name (channel can be accessed via name or id)
        self._channels[chl_id] = new_chl
        self._channels[chl_name] = new_chl
        self._set_channel_slot_times()
        client.register_owned_chl(new_chl)
        new_chl.add_client_writer(client)
        self._write_log('Channel %s creation finished' % chl_name)
        if DEBUG: print('Channel data returned:', chl_id, new_chl.control_key)
        if lifetime is not None:
            #start timer to delete the channel:
            threading.Timer(lifetime,self.rpc_delete_channel,args=('SERVER', new_chl.id, new_chl._control_key))
        return chl_id, new_chl.control_key

    def rpc_subscribe_read_from_channel(self, client_id, chl_id_or_name, buffer_size=100,
                                        full_behavior=RING_BUFFER_FULL, fill_rate=1):
        """
        rpc method to subscribe a read on a channel
        Based on the given parameters a read buffer is created to read the data
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :param chl_id_or_name: channel identification id or name
        :param buffer_size: size of the read buffer
        :param full_behavior: Type of full behavior see Buffer object for details
        :param fill_rate: Integer defines how often we sample,
                          * if fill rate is 1 any channel item is put in the read buffer
                          * if fill rate is 2 any second item will be put in the read buffer
                          * if fill rate is n and n item will be put in the read buffer
        return channel name, channel id
        """
        client = client_id  # the client object is here already in (see read loop)
        self._write_log('Channel read subscription request from client %i/%s' % (client.id, client.name))
        try:
            chl = self._channels[chl_id_or_name]
        except KeyError:
            raise StreamError_Channel('Given channel name=%s is unknown' % repr(chl_id_or_name))
        new_read_buffer = Buffer(buffer_size, full_behavior, fill_rate=fill_rate,
                                 chl_id=chl.id, chl_name=chl.name, client=client)
        chl.add_client_reader(new_read_buffer)
        client._read_bufs.add(new_read_buffer)
        self._write_log('Channel %s(%s) read subscription finished' % (chl.name, chl.id))
        return chl.name, chl.id

    def rpc_unsubscribe_read_from_channel(self, client_id, chl_id_or_name):
        """
        Delete the subscription of a read buffer for a client on the channel
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :param chl_id_or_name: channel identification id or name
        """
        client = client_id  # the client object is here already in (see read loop)
        self._write_log('Channel read subscription delete request from client %i/%s' % (client.id, client.name))
        try:
            chl = self._channels[chl_id_or_name]
        except KeyError:
            raise StreamError_Channel('Given channel name=%s is unknown' % repr(chl_id_or_name))
        found = False
        client_id=client.id
        rb=chl.remove_client_reader(client.id)
        client._read_bufs.remove(rb)
        del rb
        self._write_log('Channel read subscription deleted')
        return True

    def rpc_subscribe_write_to_channel(self, client_id, chl_id_or_name):
        """
        subscribe on a channel as additional writer
        Only for channels which are configured as multi input (no single_in)
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :param chl_id_or_name: channel identification id or name
        :return: channel_name,channel_id
        """
        client = client_id  # the client object is here already in (see read loop)
        self._write_log('Channel write subscription request from client %i/%s' % (client.id, client.name))
        try:
            chl = self._channels[chl_id_or_name]
        except KeyError:
            raise StreamError_Channel('Given channel name=%s is unknown' % repr(chl_id_or_name))
        if chl.is_single_in:
            raise StreamError_Channel('Given channel name=%s; rejected other writers' % repr(chl_id_or_name))
        client._write_chls[chl.id] = chl
        client._write_chls[chl.name] = chl
        chl.add_client_writer(client)
        self._write_log('Channel write subscription finished')
        return chl.name, chl.id

    def rpc_unsubscribe_write_to_channel(self, client_id, chl_id_or_name):
        """
        Delete the subscribe on a channel as additional writer
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :param chl_id_or_name: channel identification id or name
        :return: True
        """
        client = client_id  # the client object is here already in (see read loop)
        self._write_log('Channel read subscription request from client %i/%s' % (client.id, client.name))
        try:
            chl = self._channels[chl_id_or_name]
        except KeyError:
            raise StreamError_Channel('Given channel name=%s is unknown' % repr(chl_id_or_name))
        if chl.is_single_in:
            raise StreamError_Channel('Given channel name=%s; rejected other writers' % repr(chl_id_or_name))
        client._write_chls[chl.id] = chl
        client._write_chls[chl.name] = chl
        chl.remove_client_writer(client.id)
        return True

    def rpc_delete_channel(self, client_id, chl_id_or_name, channel_control_key):
        """
        rpc method to delete a channel
        can only be called by the owner
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :param chl_id_or_name: channel identification id or name
        :param channel_control_key:  key used for authenticate the deletion
        :return:
        """
        client = client_id  # the client object is here already in (see read loop)
        self._write_log('Delete channel %i request received' % (chl_id_or_name))
        chl = self._channels.get(chl_id_or_name)
        if chl is None:
            raise StreamError_Channel('Unknown channel %s' % repr(chl_id_or_name))
        chl.stop_channel()  # stop background thread
        chl_id = chl.id
        chl_name = chl.name
        if client == 'SERVER':
            #Server request the delete!
            client=self._clients[chl._owner]
        if chl_id not in client._owned_chl_ids:
            self._write_log('Delete channel %i request denied, client is not owner' % (chl_id))
            raise StreamError_Authenticate('Authenticate failed, client not owner of the channel')
        if chl.control_key != channel_control_key:
            self._write_log('Delete channel %i request denied, wrong channel control key given' % (chl_id))
            raise StreamError_Authenticate('Authenticate failed, wrong channel control key given')
        # remove all readers in all clients first
        for sub_client in self._clients.values():
            for buf in chl._read_bufs:
                if buf in sub_client._read_bufs:
                    sub_client._read_bufs.remove(buf)
            if chl_id in sub_client._write_chls:
                del sub_client._write_chls[chl_id]
                del sub_client._write_chls[chl_name]
        # remove the channel from all dicts
        client._owned_chl_ids.remove(chl_id)
        del self._channels[chl_id]
        del self._channels[chl_name]
        self._channel_id_hdl.free_id(chl_id)
        self._set_channel_slot_times()
        self._write_log('Channel %i/%s deleted' % (chl_id, chl_name))
        return 'Channel deleted'

    def rpc_delete_client(self, client_id, client_control_key):
        """
        rpc command to delete a client and close the connection
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :param client_control_key: key used for authenticate the deletion
        :return:
        """
        client = client_id  # the client object is here already in (see read loop)
        self._write_log('Delete client (%i,%s) request received' % (client.id, client.name))
        # first we check the control key:
        if client.control_key != client_control_key:
            raise StreamError_Authenticate('Authenticate failed, client cannot be deleted')
        with self._srv_lock:
            for chl_id in list(client._owned_chl_ids):
                chl = self._channels[chl_id]
                self.rpc_delete_channel(client, chl.id, chl.control_key)
            # finally remove the two client entries in the clients dict
            try:
                del self._clients[client.id]
            except:
                pass
            try:
                del self._clients[client.name]
            except:
                pass
            self._client_id_hdl.free_id(client.id)
            self._write_log('Client (%i,%s) removed' % (client.id, client.name))
            return 'Client removed'

    def rpc_get_clients(self, client_id):
        """
        delivers a list off all connected clients on this server
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :return: list of tuples (client name, client id)
        """
        return [(client.name, client.id) for k, client in self._clients.items() if type(k) is int]

    def rpc_get_client_id(self, client_id,client_name):
        """
        Translates the given client name into the client id
        (If id is given id will be given back any way)
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :param client_name: client name from the client from which the related id should be delivered
        :return:
        """
        client = client_id  # the client object is here already in (see read loop)
        if client_name not in self._clients:
            raise StreamError_Target('Given target client (%s) unknown!' % str(client_name))
        return self._clients[client_name].id

    def rpc_get_channels(self, client_id):
        """
        Delivers a list of all public channels that exists on the server
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :return: list of tuples (channel name, channel id)
        """
        return [(chl.name, chl.id) for k, chl in self._channels.items() if (type(k) is int) and chl.is_puplic]

    def rpc_get_client_info(self, client_id, target_client_name_or_id):
        """
        Ask for the client info delivers a dict with client information
        id, name and rpc_method description
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :param target_client_name_or_id: client identification name of id
        :return: dict with 'ID','NAME', 'RPC_METHOD_DESC' keys
        """
        client = client_id  # the client object is here already in (see read loop)
        if target_client_name_or_id not in self._clients:
            raise StreamError_Target('Given target client (%s) unknown!' % str(target_client_name_or_id))
        target_client= self._clients[target_client_name_or_id]
        return_dict={'ID':target_client.id,
                     'NAME':target_client.name,
                     'RPC_METHOD_DESC':target_client.get_rpc_method_desc()
                     }
        return return_dict

    def rpc_start_data_transfer_on_channel(self, client_id, chl_id_or_name):
        """
        rpc method to start a data transfer transaction on a channel
        A transaction id is generated that should be used for the data frames following until the end frame is given
        Note:: The transaction isd is a channel specific number and cannot be reused on other channels!
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :param chl_id_or_name: channel identification name of id
        :return: transaction id
        """
        client = client_id  # the client object is here already in (see read loop)
        chl = client._write_chls.get(chl_id_or_name)
        if chl is None:
            raise StreamError_Channel(
                'Channel (%s) not found or client is no writer on this channel' % repr(chl_id_or_name))
        t_id = chl.get_new_transaction_id()
        return t_id

    def rpc_start_statistic_channel(self, client_id, fill_rate=100, fixed_slot_time=None):
        """
        start the statistic channel to track the load of the server
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :param fill_rate: How often the readers should put their statistic information into the statistic channel
        :param fixed_slot_time: Channel might be prioritized higher (not recommended)
        :return: statistc channel name is returned so that a reader might be subscribed by the client
        """
        if self._statistic_chl is None:
            self._statistic_chl_fill_rate = fill_rate

            new_id = self._channel_id_hdl.get_new_id()
            new_chl = ServeChannel(new_id, self._statistic_channel_name, -1, is_public=True, is_single_in=False,
                                   lifetime=None,
                                   loop=self._loop, address_size=self._address_size,
                                   fixed_slot_time=fixed_slot_time
                                   )
            if DEBUG: print('New channel object created', new_chl)
            # we fill the channel dict with both id and name (channel can be accessed via name or id)
            self._channels[new_id] = new_chl
            self._channels[self._statistic_channel_name] = new_chl
            self._set_channel_slot_times()
            self._statistic_chl = new_chl
            for chl in self._channels.values():
                chl.set_statistic_channel(new_chl, fill_rate)
            return self._statistic_channel_name
        else:
            # channel exists already!
            return self._statistic_channel_name

    def rpc_get_statistic_channel_state(self,client_id):
        """
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)

        :return:
                * True, channel name - channel is active
                * False, channel name  -channel is inactive
        """
        if self._statistic_chl is not None:
            return True,self._statistic_channel_name
        else:
            return False,self._statistic_channel_name

    def rpc_stop_statistic_channel(self, client_id):
        """
        stop the statistic channel
        :param client_id: first parameter on server rpc commands is always the client id
                          (will be replaced by client object after frame is received)
        :return:
        """
        if self._statistic_chl is not None:
            statistic_chl = self._statistic_chl
            statistic_header = FrameHeader(CT_DATA_END, CODE_MARSHAL, channel_id=statistic_chl.id)
            statistic_chl.clear_buffers()
            statistic_chl.put_nowait(CT_DATA_END, Frame(statistic_header, 'END_DATA').encode())
            statistic_chl._task_trigger.set()
            # we wait a short moment before deleting the channel
            time.sleep(2)
            for chl in self._channels.values():
                chl.set_statistic_channel(None, 1)
            del self._channels[statistic_chl.name]
            del self._channels[statistic_chl.id]
            self._set_channel_slot_times()
            self._statistic_chl = None
            self._channel_id_hdl.free_id(statistic_chl.id)
            return True
        else:
            return False
