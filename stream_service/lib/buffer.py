"""
This code is taken from PyPi stream_service package
https://github.com/BR1py/stream_service
"""

#from __future__ import absolute_import
import asyncio
import random
from .helpers import *
#Possible buffer full reactions on server

# Run the buffer queue as a ringbuffer (delete oldest item and put new in)
RING_BUFFER_FULL=0 # Default behavior
# Run the buffer queue in clear mode which means if full delete all items and put new item in
CLEAR_BUFFER_FULL=1
# Run the buffer queue in skip mode which means if full the new item will be ignored and the old items are kept in the buffer queue
SKIP_BUFFER_FULL=2
# We wait endless until the buffer is free (this will block the loop!)
WAIT_BUFFER_FULL=3
# The Full Exception will be raised
RAISE_BUFFER_FULL=4
FULL_BEHAVIOR_STR={RING_BUFFER_FULL:'RING_BUFFER_FULL',CLEAR_BUFFER_FULL:'CLEAR_BUFFER_FULL',
                   SKIP_BUFFER_FULL:'SKIP_BUFFER_FULL',WAIT_BUFFER_FULL:'WAIT_BUFFER_FULL',
                   RAISE_BUFFER_FULL:'RAISE_BUFFER_FULL'}

class Buffer(asyncio.Queue):

    def __init__(self,maxsize=100,full_behavior=RING_BUFFER_FULL,fill_rate=1,loop=None,**kwargs):
        """

        Buffer object used in the server and client to store the data items after receive or before sending
        The objects behaves like a asyncio.Queue() but it is extended.
        Be sides the async calls we have also classical calls to access the queue objects.

        Especially the reaction in case the buffer is full is different and can be modified by the
        full_behavior parameter.

        :param maxsize: the maximum size of the buffer
        :param full_behavior: defines the reaction in case of QueueFull exceptions:
                              - RING_BUFFER_FULL  (=0) - (Default) The buffer is a ring buffer
                                                         Full reaction: delete the oldest item and put the new item in
                              - CLEAR_BUFFER_FULL (=1) - The buffer is a clearing buffer
                                                         Full reaction: delete all old items (clear)
                                                                        and put the new item in
                              - SKIP_BUFFER_FULL  (=2) - The buffer is a resistant buffer
                                                         Full reaction: ignore the new item keep the old ones
                              - WAIT_BUFFER_FULL  (=3) - The buffer is a blocking buffer
                                                         Full reaction: ignore the timeouts and wait until the buffer
                                                                        has place to put the item
                                                         Warning: This will block any calling loop!
                              - RAISE_BUFFER_FULL (=4) - The buffer behaves like a Queue() and raises
                                                         the QueueFull Exception

        :param fill_rate: integer (default == 1 -> no down-sampling) used to down sample the number of items put into
                          this buffer.
        :param loop: optional parameter to give the related event_loop (if not given parameter will be taken via
                     asyncio.get_event_loop() call)
        :param kwargs: Additional parameters that might be stored as attributes in the buffer object (attributes u
                       sed in the super class will be ignored
        """
        # set the additional attributes
        for k, v in kwargs.items():
            self.__setattr__(k,v)

        if loop is not None:
            super().__init__(maxsize,loop=loop)
            self._loop=loop
        else:
            self.loop=loop=asyncio.get_event_loop()
            super().__init__(maxsize,loop=loop)
        
        self._fill_rate=fill_rate
        self._fill_cnt=0
        self._full_behavior=full_behavior
        self._full_cnt=0
        self._on_full={RING_BUFFER_FULL:self.__on_full_ringbuffer,
                       CLEAR_BUFFER_FULL: self.__on_full_clearbuffer,
                       SKIP_BUFFER_FULL: self.__on_full_skipbuffer,
                       WAIT_BUFFER_FULL: self.__on_full_waitbuffer,
                       RAISE_BUFFER_FULL: self.__on_full_raise
                       }.get(full_behavior)
        if self._on_full is None:
            raise ValueError('Given full_behavior = %s is not valid'%(repr(full_behavior)))

    # give access to some internals

    @property
    def full_cnt(self):
        return self._full_cnt

    @property
    def fill_cnt(self):
        return self._fill_cnt

    # reaction methods in case the buffer is full:
    def __on_full_ringbuffer(self,item):
        super().get_nowait()
        super().put_nowait(item)
        self._full_cnt = (self._full_cnt+1)&0b11111111

    def __on_full_clearbuffer(self,item):
        self.clear()
        super().put_nowait(item)
        self._full_cnt = (self._full_cnt + 1) & 0b11111111

    def __on_full_skipbuffer(self,_):
        self._full_cnt = (self._full_cnt + 1) & 0b11111111
        return

    def __on_full_waitbuffer(self,item):
        future = asyncio.run_coroutine_threadsafe(super().put(item), self._loop)
        self._full_cnt = (self._full_cnt + 1) & 0b11111111
        return future.result()


    def __on_full_raise(self,_):
        self._full_cnt = (self._full_cnt + 1) & 0b11111111
        raise asyncio.QueueFull()

    def clear(self):
        """
        delete all items in the queue
        :return:
        """
        self._fill_cnt=self._fill_rate # by this we ensure that the next item will be put into the queue!
        while not self.empty:
            super().get_nowait()


    async def put(self,item,force=False):
        """
        async standard method for put data into the queue
        :param item: item to be put into the queue
        """
        cnt=self._fill_cnt
        cnt = (cnt+1) & 0xFFFFFFFF
        self._fill_cnt=cnt
        if force or not cnt%self._fill_rate:
            await super().put(item)
        else:
            await asyncio.sleep(0)

    async def put_wait_for(self,item,timeout,raise_except=False):
        '''
        short version of: asyncio.wait_for(self.put(item),timeout=timeout)

        Timeout reaction:
            If raise_except parameter is set the TimeoutError exception will be raised
            Else the method will react on the defined full_behavior

        :param item: item to be put in the queue
        :param timeout: timeout in seconds
        :param raise_except: True - raise TimeoutError Exception
                             False (default) - run full_behavior reaction
        '''
        try:
            return await asyncio.wait_for(self.put(item),timeout=timeout)
        except TimeoutError:
            if raise_except:
                raise
            self._on_full(item)


    async def get_wait_for(self, timeout):
        '''
        short version of: asyncio.wait_for(self.get(), timeout=timeout)

        Timeout reaction:
            TimeoutError exception will be raised

        :param timeout: timeout in seconds
        :return: item taken out of the buffer
        '''
        return await asyncio.wait_for(self.get(), timeout=timeout)

    #non async methods
    
    def put_wait(self,item,timeout,raise_except=False):
        '''
        Non async call for an await put with timeout

        ( behaves like put() in a normal queue.Queue() )

        Timeout reaction:
            If raise_except parameter is set the TimeoutError exception will be raised
            Else the method will react on the defined full_behavior

        :param item: item to be put in the queue
        :param timeout: timeout in seconds
        :param raise_except: True - raise TimeoutError Exception
                             False (default) - run full_behavior reaction
        '''
        future = asyncio.run_coroutine_threadsafe(self.put_wait_for(item,timeout,raise_except), self._loop)
        future.result()

    def put_nowait(self,item,raise_except=False,force=False):
        '''
        put a item in the queue and do not wait in case queue is full run timeout reaction

        Timeout reaction:
            If raise_except parameter is set the TimeoutError exception will be raised
            Else the method will react on the defined full_behavior

        :param item: item to be put in the queue
        :param raise_except: True - raise TimeoutError Exception
                             False (default) - run full_behavior reaction

        :param item:
        :return:
        '''
        cnt=self._fill_cnt
        cnt = (cnt+1) & 0xFFFFFFFF
        self._fill_cnt=cnt
        if force or not cnt%self._fill_rate:
            try:
                return super().put_nowait(item)
            except asyncio.QueueFull:
                if raise_except:
                    raise
                self._on_full(item)
        else:
            pass

    def get_wait(self,timeout=None):
        '''
        Non async call for an await get_wait_for() with given timeout
        ( behaves like get() in a normal queue.Queue() )

        Timeout reaction:
            TimeoutError exception will be raised

        :param timeout: timeout in seconds or None for endless waiting
        :return: item taken out of the buffer
        '''
        if timeout is None:
            future = asyncio.run_coroutine_threadsafe(self.get(), self._loop)
        else:
            future = asyncio.run_coroutine_threadsafe(self.get_wait_for(timeout), self._loop)
        return future.result()


class Transaction():
    '''
    The transaction object is mainly used in RPC calls to identify the specific call and to handle the returns
    it creates a receive buffer related to a specific rpc call
    '''

    def __init__(self,loop, transactions,ta_id_hdl, address_size, tto=TTO_DEFAULT, tto_para_name=TTO_DEFAULT_PARA_NAME):
        self.__loop = asyncio.get_event_loop()
        self.__tto = float(tto)
        self.__tto_para_name = str(tto_para_name)
        self.__transactions = transactions
        self.__ta_id_hdl=ta_id_hdl
        self.__address_size = int(address_size)
        # Constants will be overwritten in __init_transaction():
        self.__read_queue = None
        self.__t_id =ta_id= self.__ta_id_hdl.get_new_id()
        self.__buffer = Buffer(1, RAISE_BUFFER_FULL, transaction_id=ta_id)
        # enter this transaction in the transaction dict
        self.__transactions[ta_id] = self


    def set_tto(self, tto):
        self.__tto = tto

    @property
    def id(self):
        return self.__t_id

    @property
    def tto_para_name(self):
        return self.__tto_para_name

    @property
    def buffer(self):
        return self.__buffer

    def delete(self):
        del self.__transactions[self.__t_id]
        self.__ta_id_hdl.free_id(self.__t_id)
