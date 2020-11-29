#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 15:01:39 2020

@author: Patrick Husson, Neil Rice
"""

from clientread import *
from clientwrite import *
import soilDataRequest as sdr
import threading

if __name__ == "__main__":
    requestQueue = queue.Queue()
    responseQueue = queue.Queue()

    clientSocket = socket.socket()
    clientSocket.connect( ("127.0.0.1", 5557) )

    lock = threading.Lock()

    # The DS reads requests and sends responses
    crt = ClientReadThread(requestQueue, clientSocket, lock)
    cwt = ClientWriteThread(responseQueue, clientSocket, lock)

    crt.start()
    cwt.start()

    # Use the queues as you wish - this is a quick example (inefficient)
    while True:
        inMsg = requestQueue.get()

        soilData = sdr.soilDataRequest(inMsg.lon, inMsg.lat)
        soilData.submitRequest()
        soilData.formatSoilDataString()
        soilData.getFemaData()
        soilData.formatFemaDataString()
        
        outMsg = struct
        
        #outMsg.val = soilData.dataStr
        
        outMsg.lat = inMsg.lat
        outMsg.lon = inMsg.lat
        outMsg.val = 62626.333 # Arbitrary
        outMsg.str = soilData.dataStr

        #print('fakeDs output: Lat = ',outMsg.lat,', Lon = ',outMsg.lon,', val = ',outMsg.val)
        responseQueue.put( outMsg )

        # Avoid spamming
        time.sleep(2)

    # Clean up
    crt.join()
    cwt.join()
