"""
This code is taken from PyPi stream_service package
https://github.com/BR1py/stream_service
"""

import sys
import os
import time
import ssl

if "-root_path" in sys.argv: #started via shell script
    ROOT=os.path.dirname(os.path.dirname(os.path.normpath(sys.argv[(sys.argv.index("-root_path")+1)])))
else:
    ROOT=os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from stream_service import StreamChannelServer_Process

if __name__ == '__main__':
    srv = StreamChannelServer_Process(ip_address='127.0.0.1',client_authenticate=b'STRM_Client')
    srv.start()
    # let's wait until the server might be finished
    srv.join()
