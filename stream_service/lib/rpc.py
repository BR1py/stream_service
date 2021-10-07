"""
This code is taken from PyPi stream_service package
https://github.com/BR1py/stream_service
"""

from __future__ import absolute_import
import asyncio
import copy
from inspect import signature
from .helpers import *
from .frame import Frame,FrameHeader,CT_DECODE,CT_EXCEPTION

DEBUG=False

# RPC related classes

class RPCMethod():

    def __init__(self,name,signature,header,reader,writer,loop,get_transaction_method=None):
        self._name=name
        self._signature=signature
        self._header=header
        self._reader=reader
        self._writer=writer
        self._loop=loop
        self._get_transaction_method=get_transaction_method

    def __call__(self,*args,**kwargs):
        """
        Here we do the remote call!
        :param args: rpc argumnets
        :param kwargs: rpc keyword arguments
        :return:
        """
        task=asyncio.run_coroutine_threadsafe(self.exec_rpc(*args,**kwargs),self._loop)
        return task.result()

    async def exec_rpc(self,*args,**kwargs):
            """
                Here we do the remote call!
                :param args: rpc arguments
                :param kwargs: rpc keyword arguments
                :return:
                """
            # we check if the parameters match
            self._signature.bind(*args, **kwargs)
            # create transaction
            ta = self._get_transaction_method()
            if DEBUG: print('RPC call transaction %i created'%ta.id)
            # check if tto parameter is given
            if ta.tto_para_name in kwargs:
                tto = kwargs.pop(ta.tto_para_name)
            # create header
            tx_header = copy.copy(self._header)
            tx_header.set_transaction_id(ta.id)
            # create frame and send out for remote execution
            tx_frame=Frame(tx_header, *args, **kwargs)
            await send_msg(self._writer,tx_frame.encode())
            if DEBUG: print('RPC call send to server:',self.get_method_string(args,kwargs))
            rx_bytes = await ta.buffer.get()
            # wait for response in case of timeout a timeout exception will be raised
            back = Frame(FrameHeader(CT_DECODE, address_size=tx_header.address_size), rx_bytes)
            if back is None:
                raise StreamError_Command('No valid return received')
            if back.header.container_type == CT_EXCEPTION:
                raise StreamError_Command(repr(back.args))
            if DEBUG: print('RPC call return:', repr(back.args))
            # for returns we extract just the first item of the args tuple
            args=back.args[0]
            return args

    def get_method_string(self,args,kwargs):
        return '%s(%s, %s)'%(self._name,repr(args),repr(kwargs))

class RPCMethodDescription():
    __slots__=('name','method_obj','argspec','method_id','method_id_bytes','address_size')
    def __init__(self,name,method_idx,argspec,address_size=2):
        '''
        Ths objects holds the whole description of an rpc method
        :param name: method name
        :param method_idx: method idx (used in frame headers)
        :param argspec: argument specification (signature)
        :param address_size: address_size used on the server
        '''
        self.name=name
        self.argspec=argspec
        self.method_id=method_idx
        self.method_id_bytes=method_idx.to_bytes(address_size,BO)
        self.address_size=address_size

    def change_name(self,name):
        self.name=name

    def change_method_idx(self,method_idx):
        self.method_id = method_idx
        self.method_id_bytes = method_idx.to_bytes(self.address_size, BO)

    def get_desc_dict(self):
        return {'name':self.name,
                'argspec': self.argspec}

    def __repr__(self):
        out=['RPCMethodDescription(',
             ' name = %s,'%repr(self.name),
             ' method_idx = %s,' % repr(self.method_id),
             ' argspec = %s)' % repr(self.argspec)
            ]
        return ''.join(out)

class RPCEmpty():
    # class is used to instance the sub classes
    pass
class RPCFactory():
    def __init__(self,rpc_methods,header,reader,writer,loop,get_transaction_method):
        self._rpc_methods=rpc_methods
        self._header=header
        self._reader=reader
        self._writer=writer
        self._loop=loop
        self._get_transaction_method=get_transaction_method
        self._id_to_method = {}
        self._populate_methods()

    def _populate_methods(self):
        '''
        helper function that populate the class with the RPCMethod frames
        '''
        for m_id,method_item in enumerate(self._rpc_methods):
            header=copy.copy(self._header)
            header.set_command_id(m_id)
            m_name=method_item.name
            classes=m_name.split('.')
            parent = self
            if len(classes)>1:
                for pre_name in classes[:-1]:
                    if hasattr(parent,pre_name):
                        parent=parent.__getattribute__(pre_name)
                        if callable(parent):
                            # something is wrong we expect a class here!
                            # we ignore the entry
                            continue
                    else:
                        #create a new empty calss to append the methods
                        parent.__setattr__(pre_name,RPCEmpty())
                        parent=parent.__getattribute__(pre_name)
                m_name=classes[-1]
            rpc_m=RPCMethod(m_name,
                           method_item.argspec,
                           header,
                           self._reader,
                           self._writer,
                           self._loop,
                           self._get_transaction_method)
            parent.__setattr__(m_name,rpc_m)
            self._id_to_method[m_id]=rpc_m

    @staticmethod
    def class_init_rpc_functions(class_object,first_items=[],address_size=2):
        '''
        helper function which analysis the available rpc methods and creates the related description
        e.g. used during server or client init
        :param class_object: The main class object that should be analysed (server or client)
        :return:
        '''

        attributes = list(dir(class_object))
        #reorder first items:
        i=0
        for item in first_items:
            if item in attributes:
                attributes.remove(item)
                attributes.insert(i,item)
                i+=1

        rpc_methods_descs=[]
        rpc_methods=[]

        for attribute in attributes:
            try:
                if attribute.startswith('rpc_'):
                    if callable(getattr(class_object, attribute)):
                        # rpc method
                        idx = len(rpc_methods_descs)
                        data = None
                        m = class_object.__getattribute__(attribute)
                        try:
                            data = signature(m)
                        except:
                            pass
                        md = RPCMethodDescription(attribute[4:], idx, data, address_size=address_size)
                        rpc_methods.append(m)
                        rpc_methods_descs.append(md)
                    else:
                        # rpc class analyse the class for rpc commands and append to the description
                        rpc_methods_desc2,rpc_methods2=RPCFactory.class_init_rpc_functions(getattr(class_object, attribute), address_size=address_size)
                        rpc_sub_class=RPCEmpty()
                        for md,m in zip(rpc_methods_desc2,rpc_methods2):
                            rpc_sub_class.__setattr__(md.name,m)
                            md.change_name(attribute[4:]+'.'+md.name)
                            md.change_method_idx(len(rpc_methods_descs))
                            rpc_methods_descs.append(md)
                            rpc_methods.append(m)
            except:
                # we ignore methods which raises exceptions
                pass
        return rpc_methods_descs,rpc_methods

