#!/usr/bin/env python3

#Simple socket client thread sample.
#
#Eli Bendersky (eliben@gmail.com)
#This code is in the public domain
#
#Adapted by Patrick Husson (phusson12@gwu.edu)

import socket
import struct
import threading
import time
import queue

class ClientWriteThread(threading.Thread):
    # Implements the threading.Thread interface (start, join, etc.) and
    # can be controlled via the respQ Queue attribute. Replies are placed in
    # the respQ Queue attribute.
    def __init__(self, respQ, clientSocket, lock):
        super(ClientWriteThread, self).__init__()
        self.respQ  = respQ
        self.socket = clientSocket
        self.lock   = lock

    def run(self):

        while True:
            # Block until message arrives
            curMsg = self.respQ.get()

            respType = 28 # Message type
            lat = curMsg.lat
            lon = curMsg.lon
            val = curMsg.val
            string = curMsg.string

            curData = struct.pack("!Iddds", respType, lat, lon, val, string)

#            self.lock.acquire()
            self.socket.send(curData)
#            self.lock.release()

        # Done with the forever loop
        self.socket.close()

    def join(self, timeout=None):
        threading.Thread.join(self, timeout)

#------------------------------------------------------------------------------
if __name__ == "__main__":

    cs = socket.socket()
    cs.connect( ("127.0.0.1", 5557) );

    q = queue.Queue()
    cwt = ClientWriteThread(q,cs)
    cwt.start()

    while True:

        outMsg = struct
        outMsg.lat = 40.00
        outMsg.lon = -73.5
        outMsg.val = 62626.333

        print('ML: Lat = ',outMsg.lat,', Lon = ',outMsg.lon,', val = ',outMsg.val)
        q.put( outMsg )

        # Avoid spamming
        time.sleep(2)

    cwt.join()
