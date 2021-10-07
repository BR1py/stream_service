"""
This code is taken from PyPi stream_service package
https://github.com/BR1py/stream_service
"""

from __future__ import absolute_import
import marshal
import pickle
import gzip
from collections import OrderedDict
from .helpers import *

# Frame related definitions and classes:

# CONTAINER TYPES:
CT_DATA = 0  # Data frame
CT_RETURN = 0b1  # Return frame for rpc calls
CT_CMD = 0b10  # client related rpc command
CT_SRV_CMD = 0b11  # server related rpc command
CT_EXCEPTION = 0b100  # exception used in case of errors in rpc calls
CT_DATA_END = 0b101  # data stream on channel will be stopped
CT_DECODE = 0b110  # DECODE the given raw data

CONTAINER_TYPE_STR = {CT_DATA: 'DATA', CT_RETURN: 'RETURN', CT_CMD: 'CMD', CT_SRV_CMD: 'SRV_CMD', CT_EXCEPTION:
    'EXCEPTION', CT_DECODE: 'DECODE', CT_DATA_END: 'DATA_END'}

# CODE
CODE_GZIP = 0b1  # zip the bytes after serialization (may help for huge data packages) (This is first bit of the frame!)
CODE_MARSHAL = 0b010  # marshal the given objects for serialization
CODE_PICKLE = 0b100  # pickle the objects for serialization
CODE_STR = {CODE_MARSHAL: 'CODE_MARSHAL', CODE_PICKLE: 'CODE_PICKLE'}

try:
    # optional if numpy can be imported
    import numpy

    NUMPY_FROM_BYTES = numpy.frombuffer
    CODE_NUMPY = 0b110  # serialize the given numpy array (we use numpy serializer to_bytes/from_bytes)
    CODE_STR[CODE_NUMPY] = 'NUMPY'
except:
    CODE_NUMPY = None


    def dummy(*args, **kwargs):
        pass


    NUMPY_FROM_BYTES = dummy

# Data Frame mark end of transmissions via special counter values:
DATA_END_COUNTER = 0


class FrameHeader():
    __slots__ = (
        '_container_type', '_coder_type', '_transaction_id', '_command_id', '_channel_id', '_client_id', '_counter',
        '_address_size', '_transaction_id_bytes', '_command_id_bytes', '_channel_id_bytes', '_client_id_bytes',
        '_counter_bytes', '_id_bits', '_header_len', '_sub_header', '_address_mask', '_max_counter', '_raw_bytes',
    )

    # The order here is important for quicker, easier decoding of information in the server
    BIT_ITEM_RELATION = OrderedDict([(0b1, '_channel_id'),
                                     (0b10, '_client_id'),
                                     (0b100, '_command_id'),
                                     (0b1000, '_transaction_id'),
                                     (0b10000, '_counter')])

    def __init__(self,
                 container_type=CT_DATA,
                 coder_type=CODE_MARSHAL,
                 channel_id=None,
                 command_id=None,
                 client_id=None,
                 transaction_id=None,
                 counter=None,
                 address_size=2):
        '''
        FrameHeader object contains all data that are put in the header of the frame.
        The header does not have a fixed length instead the length is implicit calculated by the bits
        set in the second byte of the header (defines which ids/counters are active or not)

        :param container_type:  Defines what kind of frame should be generated
        :param coder_type: Define how the data given should be codeed (serialized)

        :param transaction_id: Put transaction ID in the frame (None - header will be shortened no info in)
        :param command_id: put command_id in the header (None - header will be shortened no info in)
        :param channel_id: put channel_id into the header (None - header will be shortened no info in)
        :param client_id: put client_id of the target client into the header (None - header will be shortened no info in)
        :param counter: put a counter into the header (None - header will be shortened no info in)
        :param address_size: The address size used of on the server must be given to encode the ids correctly
        '''
        # fill the self.xxx attributes
        self._container_type = container_type
        self._coder_type = coder_type
        self._transaction_id = transaction_id
        self._command_id = command_id
        self._channel_id = channel_id
        self._client_id = client_id
        self._counter = counter
        self._address_size = address_size

        # addresse related data
        mask = 0
        for i in range(address_size):
            mask = (mask << 8) + 0xFF
        self._address_mask = mask
        self._max_counter = 2 ** (address_size * 8)
        if container_type == CT_DECODE:
            # In this special case we expect the raw_bytes in the first argument after the container type!
            # It's the code parameter
            # or the header is initialized without data (empty) might be decoded later on
            if coder_type != CODE_MARSHAL:
                self.decode(coder_type)
        else:
            if coder_type not in CODE_STR:
                raise ValueError('Given coder_type is unknown')
            self._id_bits = 0
            # fill in the id_byte attributes and calculate header length
            l = 2
            for (bit, name) in self.BIT_ITEM_RELATION.items():
                arg = self.__getattribute__(name)
                if arg is not None:
                    self._id_bits = self._id_bits | bit
                    l += address_size
            self._header_len = l
            self._raw_bytes = self.__build_raw_bytes()

    def __build_raw_bytes(self):
        address_size = self._address_size
        mask = self._address_mask
        a = ((self._coder_type & 0b1111) << 4) + (self._container_type & 0b1111)
        first_bytes = a.to_bytes(1, BO) + self._id_bits.to_bytes(1, BO)
        items = [first_bytes]+[int_address_to_bytes(self.__getattribute__(v), address_size, mask) for v in
                 self.BIT_ITEM_RELATION.values() if self.__getattribute__(v) is not None]
        return b''.join(items)

    def __set_id_attr(self, bit, value):
        name = self.BIT_ITEM_RELATION[bit]
        if value is None:
            if self.__getattribute__(name) is not None:
                if self._id_bits & bit != 0:  # normally this should be always the case
                    self._id_bits = self._id_bits ^ bit
                    self._header_len -= self._address_size
                self.__setattr__(name, None)
        else:
            if self.__getattribute__(name) is None:
                self._id_bits = self._id_bits | bit
                self._header_len += self._address_size
            self.__setattr__(name, value)
        self._raw_bytes = self.__build_raw_bytes()

    # make some internals as properties available
    @property
    def container_type(self):
        return self._container_type

    @property
    def coder_type(self):
        return self._coder_type

    @property
    def address_size(self):
        return self._address_size

    @property
    def transaction_id(self):
        return self._transaction_id

    @property
    def command_id(self):
        return self._command_id

    @property
    def channel_id(self):
        return self._channel_id

    @property
    def client_id(self):
        return self._client_id

    @property
    def counter(self):
        return self._counter

    def set_transaction_id(self, value):
        '''
        adapt the transaction id in the header to a new value
        :param value: id to be put in (Give None to delete)
        :return:
        '''
        bit = 0b1000
        return self.__set_id_attr(bit, value)

    def set_command_id(self, value):
        '''
        adapt the command id
        :param value: id to be put in (Give None to delete)
        :return:
        '''
        bit = 0b100
        return self.__set_id_attr(bit, value)

    def set_channel_id(self, value):
        '''
        adapt the channel id
        :param value: id to be put in (Give None to delete)
        :return:
        '''
        bit = 0b1
        return self.__set_id_attr(bit, value)

    def set_client_id(self, value):
        '''
        adapt the client id
        :param value: id to be put in (Give None to delete)
        :return:
        '''
        bit = 0b10
        return self.__set_id_attr(bit, value)

    def set_counter(self, value):
        '''
        adapt the counter
        :param value: id to be put in (Give None to delete)
        :return:
        '''
        bit = 0b10000
        return self.__set_id_attr(bit, value)

    def count_up(self):
        if self._counter is None:
            raise ValueError('No counter value defined')
        self._counter += 1
        if self._counter > self._max_counter:
            self._counter = 1  # we count from one because counter==0 is DATA_END_COUNTER!
        address_size = self._address_size
        # for the raw bytes we do it quick (counter is always in the last bytes
        self._raw_bytes = self._raw_bytes[:-address_size] + int_address_to_bytes(self._counter, address_size)

    def encode(self, tmp_client_id=None, cnt_up=False):
        if cnt_up:
            self.count_up()
        if tmp_client_id is not None:
            old_client_id = self.client_id
            self.set_client_id(tmp_client_id)
            data = self.__build_raw_bytes()
            self.set_client_id(old_client_id)
        else:
            data = self._raw_bytes
        return data

    def decode(self, received_data):
        if received_data is None:
            return
        sub_header = received_data[:2]
        self._container_type = c1 = sub_header[0] & 0b1111
        self._coder_type = c2 = (sub_header[0] & 0b11110000) >> 4
        start = 2
        end = 2
        l = 2
        self._id_bits = sub_header[1]
        for bit, name in self.BIT_ITEM_RELATION.items():
            if self._id_bits & bit != 0:
                end = start + self._address_size
                d = received_data[start:end]
                self.__setattr__(name, int.from_bytes(d, BO))
                start = end
                l += self._address_size
            else:
                self.__setattr__(name, None)
        self._header_len = l
        self._raw_bytes = self.__build_raw_bytes()

    def __repr__(self):
        out = ['FrameHeader(',
               ' container_type = %s,' % CONTAINER_TYPE_STR[self._container_type]]
        coder_type_str = CODE_STR[self._coder_type & 0b1110]
        if self._coder_type & 0b1 != 0:
            coder_type_str = 'CODE_GZIP/' + coder_type_str
        out.append(' coder_type = %s,' % coder_type_str)
        for bit, name in self.BIT_ITEM_RELATION.items():
            d = self.__getattribute__(name)
            if d is not None:
                out.append(' %s = %i,' % (name, d))
        out[-1] = out[-1][:-1] + ')'
        return ''.join(out)

    def __len__(self):
        return self._header_len


# container classes

class Frame():
    __slots__ = ('_header', '_args', '_kwargs')

    def __init__(self, header, *args, **kwargs):
        '''
        The Frame is the abstraction for the data send out to/from the clients
        :param header: FrameHeader object based on this object we decided how to code the given data
        :param args: objects to be codeed
        :param kwargs:objects to be codeed as named arguments
        '''
        if header.container_type == CT_DECODE:
            if len(args) != 1 and len(kwargs) != 0:
                raise ValueError('In decode mode only a bytes argument is accepted')
            if len(args) > 0:
                raw_data = args[0]
                self.decode(raw_data)
            else:
                self._args = tuple()
                self._kwargs = {}

        else:
            self._header = header  # first argument must be the header!
            self._args = args
            self._kwargs = kwargs

    # make some internal data available

    @property
    def header(self):
        return self._header

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs

    def set_arguments(self, *args, **kwargs):
        '''
        reset the arguments of the Frame object
        (reset the payload relevant data)
        :param args:
        :param kwargs:
        :return:
        '''
        self._args = args
        self._kwargs = kwargs

    def encode(self, client_id=None, cnt_up=False):
        '''
        Encode the Frame into bytes
        :param client_id: optional client_id might be temporary exchanged in the header just for this sending
                          (used on server to exchange target and source client)
        '''
        header_bytes = self._header.encode(client_id, cnt_up)
        coder_type = self._header.coder_type
        if self._args is None and self._kwargs is None:
            return header_bytes
        if len(self._args) == 0 and len(self._kwargs) == 0:
            return header_bytes

        payload_bytes = b''
        if coder_type & 0b110 & CODE_MARSHAL != 0:
            payload_bytes = marshal.dumps((self._args, self.kwargs))
        elif coder_type & 0b110 & CODE_PICKLE != 0:
            payload_bytes = pickle.dumps((self._args, self.kwargs))
        elif coder_type & 0b110 & CODE_NUMPY != 0:
            data = ([(arg.to_bytes(), arg.shape) for arg in self._args],
                    [(k, arg.to_bytes(), arg.shape) for k, arg in self._kwargs.items()])
            payload_bytes = marshal.dumps(data)
        if coder_type & CODE_GZIP != 0:
            payload_bytes = gzip.compress(payload_bytes)
        return header_bytes + payload_bytes

    def decode_header_update(self, receive_bytes, address_size=2):
        '''
        Decode the header based on the received bytes and redefine the Frame header
        Note:: The Frame arguments will be resetted to empty (default)
        :param receive_data:
        :param address_size:
        :return:
        '''
        header = FrameHeader(CT_DECODE, address_size=address_size)
        header.decode(receive_bytes)
        self._header = header
        self._args = tuple()
        self._kwargs = {}

    def decode_payload(self, payload_bytes):
        '''
        Decode the payload given based on the Frame header definition
        :param payload_bytes: payload bytes to be decoded
        :return:
        '''
        coder_type = self._header.coder_type
        self._args = tuple()
        self._kwargs = {}
        data = (tuple(), {})
        if len(payload_bytes) == 0 or payload_bytes is None:
            return
        if coder_type & CODE_GZIP != 0:
            payload_bytes = gzip.decompress(payload_bytes)
        if coder_type & 0b110 & CODE_MARSHAL != 0:
            data = marshal.loads(payload_bytes)
        elif coder_type & 0b110 & CODE_PICKLE != 0:
            data = pickle.loads(payload_bytes)
        elif coder_type & 0b110 & CODE_NUMPY != 0:
            data = marshal.loads(payload_bytes)
            args = [NUMPY_FROM_BYTES(i[0], i[1]) for i in data[0]]
            kwargs = dict([(i[0], NUMPY_FROM_BYTES(i[1], i[2])) for i in data[1]])
            data = (args, kwargs)
        # sometimes we get here something like (a,b,c) or (a,) we do ot get the generator (a)!
        # if (a,) we extract a only we guess this is always a single value to extract -> but this might be wrong!?
        # args=
        # if type(args) is tuple and len(args)==1:
        #    args=args[0]
        # if self._header.container_type==CT_RETURN:
        # here I'not sure here have some troubles with returns values ...
        self._args = data[0]
        self._kwargs = data[1]

    def _get_args(self, *args, **kwargs):
        return (locals().values())

    def decode(self, receive_data, address_size=2):
        '''
        Decode the received data and redefine the Frame content
        :param receive_data:
        :param address_size:
        :return:
        '''
        self.decode_header_update(receive_data, address_size)
        l = len(self._header)
        return self.decode_payload(receive_data[l:])

    def __repr__(self):
        out = ['Frame(', ' %s,' % repr(self.header)]
        for v in self._args:
            out.append(' %s,' % (repr(v)))
        for k, v in self._kwargs:
            out.append(' %s = %s,' % (repr(k), repr(v)))
        out[-1] = out[-1][:-1] + ')'
        return ''.join(out)

