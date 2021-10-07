"""
This code is taken from PyPi stream_service package
https://github.com/BR1py/stream_service
"""
import asyncio
import random

#DEFAULT SERVER PORT
DEFAULT_BUFFER_STREAM_MGR_PORT = 50012

#Possible buffer full reactions on server
# Run the buffer queue as a ringbuffer (delete oldest item and put new in)
RING_BUFFER_FULL=0 # Default behavior
# Run the buffer queue in clear mode which means if full delete all items and put new item in
CLEAR_BUFFER_FULL=1
# Run the buffer queue in skip mode which means if full the new item will be ignored and the old items are kept in the buffer queue
SKIP_BUFFER_FULL=2

#Stream Server/Client related Exceptions:

class StreamError_Authenticate(Exception):
    pass
class StreamError_Command(Exception):
    pass
class StreamError_Overload(Exception):
    pass
class StreamError_Connection(Exception):
    pass
class StreamError_RPC(Exception):
    pass
class StreamError_Channel(Exception):
    pass
class StreamError_Target(Exception):
    pass
class StreamError_Coding(Exception):
    pass
class StreamError_TransactionMissmatch(Exception):
    pass

BO='big' # byte order used in the frames

TTO_DEFAULT = 20  # Default RPC timeout (can be reset via StreamClient parameter)
TTO_DEFAULT_PARA_NAME = 'tto'  # Default parameter name for RPC timeout (can be reset via StreamClient parameter)


class Logger():
    MAX_LOG_CHAR = 10000

    def __init__(self, log_file, header=None, start_tag='', quiet=False):
        self.start_tag = start_tag
        self.char_nr = 0
        self.log_file = log_file
        self.quiet = quiet
        if log_file is not None:
            if os.path.exists(log_file):
                self.create_bak(quiet=True)
            if header is not None:
                self.write_log(header)

    def create_bak(self, quiet=False):
        log_file = self.log_file
        if log_file is not None:
            if os.path.exists(log_file):
                if os.path.exists(log_file + '.bak'):
                    os.remove(log_file + '.bak')
                os.rename(log_file, log_file + '.bak')

    def write_log(self, value):
        if self.char_nr > self.MAX_LOG_CHAR:
            self.create_bak()
            self.char_nr = 0
        if not self.quiet:
            print('%s%s' % (self.start_tag, str(value)))
        if self.log_file is not None:
            with open(self.log_file, 'a') as fh:
                fh.write('%f %s%s\n' % (time.time(), self.start_tag, value))
        self.char_nr += len(value)

async def read_msg(stream=asyncio.StreamReader):
    try:
        size_bytes = await stream.readexactly(4)
        size = int.from_bytes(size_bytes, byteorder=BO)
        data = await stream.readexactly(size)
    except:
        return None
    return data

async def send_msg(stream=asyncio.StreamWriter, data=b''):
    size_bytes = len(data).to_bytes(4, byteorder=BO)
    stream.writelines([size_bytes,data])
    await stream.drain()

def int_address_to_bytes(address_int, address_size,mask=None):
    if mask is None:
        mask = 0
        for i in range(address_size):
            mask = mask<<1 + 0xFF
    new_int = address_int & mask
    return new_int.to_bytes(address_size, BO)

def get_args(*args, **kwargs):
    return (locals().values())


def get_raw_key(seed, key):
        '''
        method hides the key in the seed
        (no real encryption but unusual)

        :param seed: seed bytes
        :param key: seed key bytes
        :return: raw_key to be compared
        '''
        # in first step the bytes are mixed
        new = b''
        kl = len(key)
        i = 0
        for i in range(len(seed)):
            if i < kl:
                new = key[i].to_bytes(1, BO) + new
            new = seed[i].to_bytes(1, BO) + new
        if i < kl:
            new = key[i:] + new
        new_int = int.from_bytes(new[:16], BO)
        key_int = int.from_bytes(key, BO)
        raw_int = hash((new_int * key_int))
        if raw_int < 0:
            raw_int = raw_int * -1
        raw_key = (raw_int).to_bytes(8, BO)
        return raw_key


class IdsHandler():

    def __init__(self, address_size):
        """
        This is a helper class for handling (generating and management) of ids
        in the given address size
        The methods deliver ids from a given set and put them back into the set in case they are no longer used
        :param address_size: address size to use for the generation of the ids
        """
        mask = 0
        for i in range(address_size):
            mask = (mask << 8) + 0xFF
        # prepare random generator
        self._choices = {i for i in range(mask)}
        self._random = random.Random()
        self._random.seed()

    def get_new_id(self):
        new_id =  self._random.sample(self._choices,k=1)[0]
        self._choices.remove(new_id)
        return new_id

    def free_id(self, old_id):
        self._choices.add(old_id)
