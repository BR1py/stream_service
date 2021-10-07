"""
In this example we create 4 Clients that will connect to the server and setup several channels
Then different type of data is streamed in between the clients


    Client1             Client3
    owns "Chl1"         owns "Chl3"
    reads "Chl2"        reads "Chl1" fill_rate=2 (takes only every second item)
    writes "Chl3"
            \          /
             \        /
              \      /
               Server
              /      \
             /        \
            /          \
    Client2             Client4
    owns "Chl2"         owns "Chl4"
    reads "Chl1"        reads "Chl3"
    reads "Chl4"        reads "Chl1"
    writes "Chl3"

REMARK: To let the example run start the run_server.py script in a parallel process

REMARK: Ensure that the root path of stream_service package is available in the python interpreter search path
"""

import asyncio
import sys
import os
import time
import threading
import ssl
import shutil

if "-root_path" in sys.argv: #started via shell script
    ROOT=os.path.dirname(os.path.dirname(os.path.normpath(sys.argv[(sys.argv.index("-root_path")+1)])))
else:
    ROOT=os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from stream_service import *

START_TIME=None
DEBUG=False
MULTIWRITE_LOOP=500 # Defines how many elements are written in the multi write loops for "Chl3"
USE_SSL=False
COLLECT_STATISTICS=True # Starts the server statistics channel with very high rate
CHL1_ONLY = False  # if True we just run data exchange on "Chl1"
RESULT_FOLDER='results' #name of the folder in which the resultfiles will be stored

receive_stop=threading.Event()

def stream_file(client,chl_name,file,chunk_size=1000,start_event=None):
    if start_event is not None:
        start_event.wait()
    header=client.start_data_transfer(chl_name,CODE_MARSHAL)
    with open(file,'r') as fh:
        cnt = 0
        while 1:
            data=fh.read(chunk_size)
            client.put_data(header,data,True)
            if DEBUG: print('data put on server',client.name,chl_name)
            if data is None or len(data)==0:
                client.end_data_transfer(header)
                break
            cnt += 1
    print('Client %s(Chl %s): Streaming of file %s done.'%(client.name,chl_name,repr(file)),
          '; Number of items transferred:',cnt,
          '; Duration since start:',time.time()-START_TIME )
    return True

def receive_file(client,chl_name,target_file_name,take_stats=None):
    ta_ids=set()
    with open(target_file_name,'w') as fh:
        cnt=0
        ends=0
        while 1:
            if cnt >= 200:
                if take_stats is not None:
                    take_stats.set()
            header,data=client.get_data(chl_name)
            ta_id=header.transaction_id
            if ta_id not in ta_ids:
                ta_ids.add(header.transaction_id)
            if DEBUG: print('received data on:',client.name,chl_name)#,data)
            elif header.container_type == CT_DATA_END:
                if ta_id in ta_ids:
                    ta_ids.remove(ta_id)
                if len(ta_ids)==0:
                    break
                continue
            fh.write(data)
            if receive_stop.is_set():
                break
            cnt+=1
    print('Client %s(Chl %s): file write done'%(client.name,chl_name),
          '; Number of items transferred:',cnt,
          '; Duration since start:',time.time()-START_TIME)
    return True

def multi_write(client,chl_name,start_event=None):
    if start_event is not None:
        start_event.wait()
    header=client.start_data_transfer(chl_name,CODE_MARSHAL)
    name=client.name
    for i in range(MULTIWRITE_LOOP):
        client.put_data(header,'%i,%s\n'%(i,name),True)
        if DEBUG: print('data put on server multi write %s (%s)'%(name,chl_name))
    client.end_data_transfer(header)
    print('Client %s(Chl: %s): multi write done'%(client.name,chl_name),
          '; Number of items transferred:',MULTIWRITE_LOOP,
          '; Duration since start:',time.time()-START_TIME )
    return True

def clean_last_run():
    #delete all files created during last run
    if os.path.exists(RESULT_FOLDER):
        shutil.rmtree(RESULT_FOLDER)
    os.makedirs(RESULT_FOLDER)
    # create the test_file
    with open (RESULT_FOLDER+'/'+'test.txt','w') as fh:
        for i in range(50000):
            fh.write('%i\n'%i)

if __name__ == '__main__':
    # 1. preparations:
    start=threading.Event()
    take_stats=threading.Event()
    clean_last_run()


    # 2. Start the client threads and connect to the server:
    clt1=StreamChannelClient_Thread('Client1','127.0.0.1',server_authenticate=b'STRM_Client')
    clt1.start()
    clt1.wait_until_alive()
    #We clean up the server just to be sure!
    clt1.srv_rpc.stop_statistic_channel(clt1.client_id)

    print('Client1 connected to server')

    clt2 = StreamChannelClient_Thread('Client2','127.0.0.1', server_authenticate=b'STRM_Client')
    clt2.start()
    clt2.wait_until_alive()
    print('Client2 connected to server')

    clt3 = StreamChannelClient_Thread('Client3','127.0.0.1', server_authenticate=b'STRM_Client')
    clt3.start()
    clt3.wait_until_alive()
    print('Client3 connected to server')

    clt4 = StreamChannelClient_Thread('Client4','127.0.0.1', server_authenticate=b'STRM_Client')
    clt4.start()
    clt4.wait_until_alive()
    print('Client4 connected to server')


    # 3. create the channels
    clt1.create_new_channel('Chl1')
    print('Client1 Chl1 created')
    clt2.create_new_channel('Chl2')
    print('Client2 Chl2 created')
    clt3.create_new_channel('Chl3',single_in=False)
    print('Client3 Chl3 (multi write) created')
    clt4.create_new_channel('Chl4')
    print('Client4 Chl4 created')

    print('All clients and channels created')

    # 4. subscribe all the readers and writers to the channels:
    print('Channel subscription follows')
    clt1.subscribe_read_from_channel('Chl2')
    clt1.subscribe_write_to_channel('Chl3')

    clt2.subscribe_read_from_channel('Chl1')
    clt2.subscribe_read_from_channel('Chl4')
    clt2.subscribe_write_to_channel('Chl3')

    clt3.subscribe_read_from_channel('Chl1',fill_rate=2)

    clt4.subscribe_read_from_channel('Chl1')
    clt4.subscribe_read_from_channel('Chl3',server_buffer_size=600)

    #get the channel statistics
    if COLLECT_STATISTICS:
        stat_chl=clt1.srv_rpc.start_statistic_channel(clt1.client_id,1) #put every sampling into statistics file
        clt1.subscribe_read_from_channel(stat_chl,server_buffer_size=10000)
        print('Read also:', stat_chl)

    print('All Channel subscribed')

    # 5. create data writing and reading threads which will create the activity on the server at least:

    print('Setup all the streaming threads')
    #We first setup the readers just to be sure not to miss a package:
    read_client2_chl1 = threading.Thread(target=receive_file, args=(clt2, 'Chl1', RESULT_FOLDER+'/'+'client2_chl1.txt'))
    read_client3_chl1 = threading.Thread(target=receive_file,args=(clt3,'Chl1', RESULT_FOLDER+'/'+'client3_chl1.txt'))
    read_client4_chl1 = threading.Thread(target=receive_file,args=(clt4,'Chl1', RESULT_FOLDER+'/'+'client4_chl1.txt'))
    if not CHL1_ONLY:
        read_client1_chl2 = threading.Thread(target=receive_file, args=(clt1, 'Chl2', RESULT_FOLDER+'/'+'client1_chl2.txt'))
        read_client2_chl4 = threading.Thread(target=receive_file, args=(clt2, 'Chl4', RESULT_FOLDER+'/'+'client2_chl4.txt'))
        read_client4_chl3 = threading.Thread(target=receive_file, args=(clt4, 'Chl3', RESULT_FOLDER+'/'+'client4_multi_chl3.txt',take_stats))

    #writers (owned)
    write_client1_chl1 = threading.Thread(target=stream_file, args=(clt1, 'Chl1', RESULT_FOLDER+'/'+'test.txt',1000,start))
    if not CHL1_ONLY:
        write_client2_chl2 = threading.Thread(target=stream_file, args=(clt2, 'Chl2', RESULT_FOLDER+'/'+'test.txt',1000,start))
        write_client4_chl4 = threading.Thread(target=stream_file, args=(clt4, 'Chl4', RESULT_FOLDER+'/'+'test.txt',1000,start))
        #multi write
        write_client1_chl3 = threading.Thread(target=multi_write, args=(clt1, 'Chl3',start))
        write_client2_chl3 = threading.Thread(target=multi_write, args=(clt2, 'Chl3',start))
        write_client3_chl3 = threading.Thread(target=multi_write, args=(clt3, 'Chl3',start))

    # 6. before we start streaming we start the readers (we like to get all data and we do not want to read only a part)

    read_client2_chl1.start()
    read_client3_chl1.start()
    read_client4_chl1.start()
    print('Stream data read on Chl1 threads started')

    if COLLECT_STATISTICS:
        read_statistics = threading.Thread(target=receive_file,
                                           args=(clt1, stat_chl, RESULT_FOLDER+'/'+'statistics.txt'))
        read_statistics.start()

    if not CHL1_ONLY:
        read_client1_chl2.start()
        read_client2_chl4.start()
        read_client4_chl3.start()
        print('Stream data read on other channels threads started')

    # 7. We start the send streams now but they will wait for the start event
    if not CHL1_ONLY:
        print('Start streaming multi write on Chl3,Chl2,Chl4 threads started')
        # multi write
        write_client1_chl3.start()
        write_client2_chl3.start()
        write_client3_chl3.start()
        #other channels
        write_client2_chl2.start()
        write_client4_chl4.start()

    print('Streaming data from Client1 to Chl1 thread started')
    write_client1_chl1.start()

    # 8. start the streaming by setting the start event!
    START_TIME = time.time()
    start.set()
    print('Start-Event set at:',START_TIME)

    # 9. wait for the streaming to be finished

    #depending on processor speed one  may adapt this timigs so that the statitics taken under high load
    read_client2_chl1.join()
    read_client3_chl1.join()
    read_client4_chl1.join()
    print('Chl1 reader threads finished')

    if not CHL1_ONLY:
        read_client1_chl2.join()
        read_client2_chl4.join()
        read_client4_chl3.join() # This is the slowest
        print('Other channels reader threads finished')

    if COLLECT_STATISTICS:
        print('Clothing statstics channel')
        stat_chl = clt1.srv_rpc.stop_statistic_channel(clt1.client_id)
        time.sleep(2)
        receive_stop.set()
        read_statistics.join()  # This is the last

    print('Last thread finished')

    # 10. close the clients:
    time.sleep(2) #just to be sure we wait a bit longer
    print('Delete the client objects and close the app')
    clt1.stop_client()
    clt2.stop_client()
    clt3.stop_client()
    clt4.stop_client()

    #input('PRESS return to stop') # enable for debbuging proposes
