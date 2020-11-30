#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

@author: Neil Rice

This is a script to test the process of generating a pAOI using an Area 
Symbol keyword.
"""
import json
import urllib3

http = urllib3.PoolManager()

#Set the coordinates to be used in the data request.
latitude = 37.326998
longitude = -77.455749

#Define URL of of post.rest web service.    
url = "https://SDMDataAccess.sc.egov.usda.gov/Tabular/SDMTabularService/post.rest"

#Build JSON request.
dRequest = dict()
dRequest["SERVICE"] = "aoi"
dRequest["REQUEST"] = "create"
dRequest["SSA"] = "VA041"
encoded_data = json.dumps(dRequest).encode('utf-8')
#Send the request.
r = http.request('POST', url, body=encoded_data)

#Unpack returned data.
pAOI = json.loads(r.data.decode('utf-8'))

print("pAOI = " + str(pAOI))