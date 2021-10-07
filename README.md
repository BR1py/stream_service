# stream_service
A python package for streaming data from one client to another over a channel on a central stream server

Version 0.1.0 

This package is a investigation study regarding the streaming of data from a client to a channel on a distribution server that sends the data to all the clients that have subscribed the channel.

The package builds an extended functionality around the client server functions already available in the asyncio module of python.

At least in the client/server classes the following features are implemented:

Stream_Server:

* Stream_Server can be started as an independent process (inheritate "from multiprocessing import Process" class)
* Stream_Server provides channel and client informations
* Stream_Server provides load statistics
* All functional server requests are handled as RPC calls to the server

Stream_Clients:

* Stream_Client is realized as a Thread and can be integrated in other applications
* Stream_Clients can create channels for data sending (channels can be puplic or private and also only temporary)
* Stream_Clients can subscribe existing channels for reading data
* Stream_Clients can subscribe existing channels for writing data
* The data send is internally serialized via marshall or pickle an gzip packing can be enable for huge data frames
* numpy arrays can be serialized via numpy to_bytes()/from_bytes() method
* The sending and receiving data is available in normal python style or in modern awaited asyncio calls
* Different received data buffers(queue) types are available and can be used directly for data acces (especially reading)

Remote Procedure Calls (RPC):

* Stream_Clients class can be used as a super class for an own STRM_Client that can have extended RPC calls available which can be called from other clients
* RPC method calls are done directly on the rpc_client object on the client (as a local method)
* RPC method parameters are checked on rpc_client side before send to the target client
* For each RPC call the user can define an individual timeout
* RPC methods can be encapsulated in RPC subclasses

An important design goal of the package is that we avoid useless network traffic. This means on the server we can define which type of read buffer we want to connect to a channel. The read buffer can be defined in case of buffer size, over run behavior (ring buffer or other types) and also the fill rate can be set in case not any data should be taken out of the channel. By this mechanism the filtering is done on the server before we send the data to the client.
Second we have implemented a mechanism that triggers the writer clients only to send data in case at least one reader is available. Channels without readers will not request any data from the writer clients.

The package contains some typical examples showing how the client/server functions might be used

## Installation

The PyPI package is not avalable yet!

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the stream_server package.

```bash
pip install stream_service
```

The package has no dependencies to other external packages. 

It should work for all python versions >3.5. To get the package working under python 3.4 all the await decorators in the package must be replaced with yield from decorator.

## License
[MIT](https://choosealicense.com/licenses/mit/)

## Documentation

There is no specific documantation for the apackage available yet. Please have a look in the examples to get an idea how it works. If I see there are multiple requests for a documentation I will add it.

## Package structure and files 

The structure of folder and files related to this package looks like this:

* stream_framework (main folder)

   * __ init __.py
   * strm_server.py
   * strm_lib.py
   * strm_client.py

   * examples

      * run_server.py
      * client_rpc.py
      * client_streams.py

## More detailed informations 

As mentioned in the title I developed this package for some investigation proposes. On the one hand side I wanted to understand the asyncio package in a more deeper way and on the other side the request for a channel based streaming of information in the professional projects I'm working on was very high and I didn't found a matching ready to use package for the requirements we have.

At least the package is used in my projects already and I can say the stability for the use cases we have is quiet good. This means even that the original idea and intension of developing the package was more in the direction of education and learning the result is from my point of few stable enougth to be usable in other projects too.

The core function of the package can be compared to the popular video streaming platforms in the internet. We have aserver where we can place data into a channel and we have clients that subsribe to the channel and take the data out. The STRM_Server is the one who takes the data from the client and distributes it to the clients who are interested in.
The picture shows in an overview the server and clients and the general interaction.

![DiagramPrinciplesOfStreamServices](https://github.com/BR1py/stream_service/blob/main/docs/docs/DiagramPrinciplesOfStreamServices.png?raw=true)

To get a better understanding of the data streaming functionality based on channels we will try to describe all steps that must be performed until a data package can be send by a client to the channel and it is received by the targets.
Let's assume the Stream_Server is already running and three Stream_Clients are already connected to the Stream_Server (see examples stream_data.py).

1. The sending client must create a channel which should be used for the streaming. The client who creates the channel is the ownwer and the channel can only be deleted by the client. Also the channel will be deleted from the server if the related client disconnects (but this might be changed in one of the future versions).

Client1: create_new_channel("ABC")

2. after the channel exists the other clients can subscribe to the channel as readers:
Client2: subscribe_read_channel("ABC")
Client3: subscribe_read_channel("ABC")

During the subscription the client will also define in which kind of buffer structure the data will be collected in the servers channel object. In the channel object a mtching reader queue (buffer) will be created.

3. Now the client can send a data package by putting it into the channel:
Client1: put_data("ABC",'MY data') 
Any data object that can be marshaled or pickled can be transferred into the channel. For numpy arrays (if installed we use the numpy internal translation to bytes to serialize)

4. The data_frame is transfered via asyncio StreamReader to the server and for each client we have a read_from_client() loop running which await the arrival of new data from the StreamReader.

5. The awaited data_frame is received and will be analysed from the server. In the hearder of the frame the server can see the target channel and that the data should be streamed and it is not an RPC call, etc.

6. The server puts the data_frame into the channel and in the channel object the data_frame is put in the different reader queues (buffers) that are create because the Clients have subscripted the channel. Here full queues are handled regarding the queue type (e.g. ringbuffers will delete the oldest entry and put new one in. Other might be blocked by old data and the new data is skipped or the whole buffer might be cleared before put in new data all this depending on the queue type setup.

7. The send_to_client() queue of the different clients awaiting the filling of the related reader queues and in the moment the queues are filled the sending to the clients starts. The data_frame is taken out of the buffer and put in the StreamWriter object of the client. In the send_to_client loop we have a load balancing mechanism that ensures that all the data send to the client is considered with the same weight (all subscripted channels). In case of high server load or bad connections the queue content might not be send completly then sending will be postponed. The filling of the buffers will continue in parallel. And the data might be send with the next sending cycle established. In case of fulll buffers data will be lost. The buffertype setup is important for dealing with this issue.

In the implementation we have a read_from_client() and sent_to_client() loop for each connected client. They where all running in the same event_loop in the server.

8. After the data_frame is put in the StreamWriter of the client. The Client receives the data from the subscripted channels over the network.

As descripted the data streaming over channels multiplies the data to all the clients subscripted to the channel. This is the core function we wanted to realize with this package.

In practice it might be used e.g. for a measurement system. We have an small processor in the system running a data aquisition application. In this application exists an object that takes the values of an input interface (analog input) with a specififc sample rate. And this object takes the collected values and puts them via the STRM_Client into the "AnalogInput1" on an STRM_Server that might run on a connected PC.

On the PC runs another application that should store the measurment data into a file. The application subscribes the channel "AnalogInput1" via an STRM_Client and uses an invinite buffersize to ensure the dataconsistency as good as possible (we do not like to loose any meas data when storing it into a file).

In the same application we have another process that runs analysis on the received data. Again a STRM_Client subsriping the related channel is starte dthe data is taken is in a consistent way even in case of data losts and put into the checking algorithm. A midsized ringbuffer is used for this proposes.

In the lab exists a central control PC and on this PC we like to display the measurement values taken from the analog input interface. Again this application uses the STRM_Client and subsribes to the channel "AnalogInput1" it will use a small buffersize with a ringbuffer. So that not any data will be transfered to the display. This keeps the traffic small and the display might only be updated every second.

The small example shows somehow how the STRM_server and STRM_clients can be used in a distributed system of independent processing units and processes to exchange the data in a broadcasting way over the channels.

## Additional features 

### Channel features:

The channel concept implemented allows not only channels with single writers and multiple readers. Furtehr more multi write multi read is possible.

The channel object can also be setup as private or puplic (puplic channels are distributed in a list from the server and can be seen by all Clients. Subscription to private channels are only possible if the Client nows the channel name from another source and the authentication key is known too.

Finally the a channel can be created as a temporary channel. Then a lifetime parameter must be set to a number of seconds and the channel will only exists for the defined lifetime and then terminate itself.

### RPC features:

The Stream_Client object might be used as a super class of own STRM_Client classes. The new class can be extended by remote procedure calls by adding methods with the pre string "rpc_". Those new methods are identified during the class instanciation. And will be distributed as a RPC service from this client over the Stream_Server (see client_rpc.py in examples folder). 

Another client can connect to the client (over the STRM_server) and ask for a RPC authentication. Then he will receive the rpc service info from the contacted RPC client and a related RPCClient object in the connected client will be created. Via this object the user can easy program functions that will be executed during runtime on the RPC client (remote execution).

The code might look like this:

```python

class MyClient(StreamClient_Thread):
    
    def rpc_my_echo(self,data):
    return 'MY ECHO',data
    
myrpc_client=MyClient('rpc_client',127.0.0.1) # we run the server on local host
myrpc_client.start()
```

The connecting client does the following:

```python

myclient=StreamClient_Thread('myclient',127.0.0.1) # we run the server on local host
myclient.start()

myrpc=myclient.create_rpc_client_service('rpc_client')
```

The execution of the remote call will be done like this:

```python
print(myrpc.my_echo('HALLO'))
> 'MY_ECHO','HALLO'
```

The returned tuple is created in the myrpc_client StreamClient_Thread object. If you do not run on localhost this object might exists on another maschine somewhere in the network in a another process. But the coding is as if it is on the local maschine.

Each RPC call is internally handled by an transaction object which ensures that the target is set correctly and the received return (send via the StreamWriter) can be identified.

The RPC execution is normally protected by a default timeout (STRM_ClientThread parameter), but you can also set individual timeouts for any rpc call by adding a specific parameter to the calling method (default is "tto").

```python
print(myrpc.my_echo('HALLO',tto=10)) # set the timeout for this call to 10 seconds
> 'MY_ECHO','HALLO'
```

In the implementation the matching of the given parameters to the rpc method is already checked on the client side and if a missmatch is detected no call will be send to the rpc client.

