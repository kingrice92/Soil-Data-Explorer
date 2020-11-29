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
import queue

class ClientReadThread(threading.Thread):
    # Implements the threading.Thread interface (start, join, etc.) and
    # can be controlled via the reqQ Queue attribute. Replies are placed in
    # the respQ Queue attribute.
    def __init__(self, reqQ, clientSocket, lock):
        super(ClientReadThread, self).__init__()
        self.reqQ   = reqQ
        self.socket = clientSocket
        self.lock   = lock

    def run(self):

        while True:
            self.lock.acquire()
            data = self.socket.recv(4);
            self.lock.release()
            # Network ordering
            curType = int.from_bytes(bytearray(data), byteorder='big');

            if curType == 27:
                # Blocking for the correct number of bytes
                self.lock.acquire()
                msgData = self.socket.recv(20);
                self.lock.release()

                lat,lon,recType = struct.unpack("!ddI", bytearray(msgData))

                newReq = struct
                newReq.lat  = lat
                newReq.lon  = lon
                newReq.type = recType
                self.reqQ.put( newReq )

        # Done with the forever loop
        self.socket.close()

    def join(self, timeout=None):
        threading.Thread.join(self, timeout)

#------------------------------------------------------------------------------
if __name__ == "__main__":
    cs = socket.socket()
    cs.connect( ("127.0.0.1", 5557) );

    q = queue.Queue()
    crt = ClientReadThread(q, cs)
    crt.start()

    print('Starting main loop')
    while True:
        curReq = q.get()
        print('ML: Lat = ',curReq.lat,', Lon = ',curReq.lon,', type = ',curReq.type)

    crt.join()
