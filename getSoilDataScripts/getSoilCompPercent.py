#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 00:15:21 2020

@author: neilkingrice
"""

import json
import urllib3

http = urllib3.PoolManager()

latitude = 37.326998
longitude = -77.455749

#sQuery = ("SELECT mukey AS MUKEY, muname AS Map_unit_name\n"
#+ "FROM mapunit\nWHERE mukey IN (SELECT * from "
#+ "SDA_Get_Mukey_from_intersection_with_WktWgs84('point (" + str(longitude)
#+ " " + str(latitude) + ")'))")

sQuery = ("SELECT L.areasymbol AS Area_symbol, L.areaname AS Area_name, M.musym\n"
          + "AS Map_unit_symbol, M.muname AS Map_unit_name, M.mukey AS MUKEY,\n"
          + "comppct_r AS component_Percent, compname AS Component_name\n"
          + "FROM legend AS L\n"
          + "INNER JOIN mapunit AS M ON M.Lkey = L.Lkey\n"
          + "AND M.mukey IN (SELECT mukey FROM"
          + " SDA_Get_Mukey_from_intersection_with_WktWgs84('point (" 
          + str(longitude) + " " + str(latitude) + ")'))\n"
          + "LEFT OUTER JOIN component AS c ON M.mukey = c.mukey\n"
          + "ORDER BY M.mukey DESC, compname")
        
url = "https://SDMDataAccess.sc.egov.usda.gov/Tabular/SDMTabularService/post.rest"

dRequest = dict()
dRequest["FORMAT"] = "JSON"
dRequest["QUERY"] = sQuery

encoded_data = json.dumps(dRequest).encode('utf-8')
r = http.request('POST', url, body=encoded_data)

data = json.loads(r.data.decode('utf-8'))

areaSym = data['Table'][0][0][:]
areaName= data['Table'][0][1][:]
MapUnitSymbol = data['Table'][0][3][:]
MapUnitName = data['Table'][0][5][:]
mukey= data['Table'][0][6][:]
#date = data['Table'][0][2]

#print(areaSym, areaName, date)
print(areaSym, areaName, MapUnitSymbol, MapUnitName, mukey)