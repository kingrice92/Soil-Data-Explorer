#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  8 22:49:57 2020

@author: neilkingrice
"""

import json
import urllib3

http = urllib3.PoolManager()

latitude = 38.896847
longitude = -77.029501
#areaSymbol = "DC001"

#sQuery = ("SELECT mukey AS MUKEY, muname AS Map_unit_name\n"
#+ "FROM mapunit\nWHERE mukey IN (SELECT * from "
#+ "SDA_Get_Mukey_from_intersection_with_WktWgs84('point (" + str(longitude)
#+ " " + str(latitude) + ")'))")

sQuery = ("SELECT L.areasymbol AS Area_symbol, L.areaname AS Area_name, M.musym"
          + " AS Map_unit_symbol, M.muname AS Map_unit_name, M.mukey AS MUKEY\n"
          + "FROM legend AS L\nINNER JOIN mapunit AS M ON M.Lkey = L.Lkey\n"
          + "AND M.mukey IN (SELECT mukey "
          + "FROM SDA_Get_Mukey_from_intersection_with_WktWgs84('point (" 
          + str(longitude) + " " + str(latitude) + ")'))")
        
url = "https://SDMDataAccess.sc.egov.usda.gov/Tabular/SDMTabularService/post.rest"

dRequest = dict()
dRequest["FORMAT"] = "JSON"
dRequest["QUERY"] = sQuery

encoded_data = json.dumps(dRequest).encode('utf-8')
r = http.request('POST', url, body=encoded_data)

data = json.loads(r.data.decode('utf-8'))

areaSym = data['Table'][0][0]
areaName= data['Table'][0][1]
MapUnitSymbol = data['Table'][0][2]
MapUnitName = data['Table'][0][3]
mukey= data['Table'][0][4]
#date = data['Table'][0][2]

#print(areaSym, areaName, date)
print(areaSym, areaName, MapUnitSymbol, MapUnitName, mukey)