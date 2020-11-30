#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 27 16:14:05 2020

@author: Neil Rice

This is a test script use to debug the process of creating FEMA database
queries using the parsed data from the Soil Data Mart queries. 
"""

import json
import urllib3
import pandas

#Set the coordinates to be used in the data request.
latitude = 38.9
longitude = -76.8

http = urllib3.PoolManager()
    
#Build SQL query for Soil Data Mart.    
sdmQuery = ("SELECT L.areasymbol AS Area_symbol, L.areaname AS Area_name, M.musym\n"
          + "AS Map_unit_symbol, M.muname AS Map_unit_name, M.mukey AS MUKEY,\n"
          + "comppct_r AS component_Percent, compname AS Component_name\n"
          + "FROM legend AS L\n"
          + "INNER JOIN mapunit AS M ON M.Lkey = L.Lkey\n"
          + "AND M.mukey IN (SELECT mukey FROM"
          + " SDA_Get_Mukey_from_intersection_with_WktWgs84('point (" 
          + str(longitude) + " " + str(latitude) + ")'))\n"
          + "LEFT OUTER JOIN component AS c ON M.mukey = c.mukey\n"
          + "ORDER BY M.mukey DESC, comppct_r DESC")

#Define URL of of post.rest web service.         
url = "https://SDMDataAccess.sc.egov.usda.gov/Tabular/SDMTabularService/post.rest"
#Build JSON request.
dRequest = dict()
dRequest["FORMAT"] = "JSON"
dRequest["QUERY"] = sdmQuery
encodedData = json.dumps(dRequest).encode('utf-8')
#Send the request.
r = http.request('POST', url, body=encodedData)
#Unpack returned data.
soilData = json.loads(r.data.decode('utf-8'))

#Create column names for pandas dataframe.
columnHeaders = ['Area Symbol', 'Area Name', 'Map Unit Symbol', 
                         'Map Unit Name', 'Map Unit Key', 'Soil Component Percent', 
                         'Soil Component Name']

#Create a table with the returned SDM data.
formattedTable = pandas.DataFrame(soilData['Table'], columns = columnHeaders)

#Initialize output data string.
outputStr = ("Returned Values;" + str(len(formattedTable['Soil Component Name']))
                     + ";" )

#Construct output data string.      
if len(pandas.unique(formattedTable['Map Unit Symbol']))==1:
    for header in columnHeaders:
        if header=='Soil Component Percent' or header=='Soil Component Name':
            outputStr = (outputStr + header + ';'
                                + ';'.join(list(formattedTable[header].values)) + ';')
        elif header!='Area Symbol' and header!='Map Unit Key':
            outputStr = (outputStr + header + ';'
                                + formattedTable[header][1] + ';')
            
else:   
    for header in columnHeaders:
        outputStr = (outputStr + header + ';'
                     + ';'.join(list(formattedTable[header].values)) + ';')

#Extract the state name.
state = formattedTable['Area Symbol'][0][0:2]
#Extract the area name.
areaName = formattedTable['Area Name'][0]

if areaName.find(' County,') != -1:
    countyName = areaName[0:areaName.find(' County,')]

#Use the State name to build the query for the FEMA database.
baseUrl = ("https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries?$filter=state eq '"
           + state + "'")

#Send the request.
r2 = http.request('GET', baseUrl)
#Unpack returned data.
disasterData = json.loads(r2.data.decode('utf-8'))

#Create a table with the returned FEMA data.
formattedTable2 = pandas.DataFrame(disasterData['DisasterDeclarationsSummaries'])
df1 = formattedTable2[formattedTable2['designatedArea'].str.contains(countyName)]
df1 = df1[~df1['incidentType'].str.contains("Biological")]
final = df1[['fyDeclared','state','designatedArea','incidentType','declarationTitle']]
print(final.values[0])