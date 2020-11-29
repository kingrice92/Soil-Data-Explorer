#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 15:09:21 2020

@author: kingrice
"""

import json
import urllib3
import pandas

class soilDataRequest():
   
    def __init__(self, longitude, latitude):
        self.long   = longitude
        self.lat = latitude
        self.soilData = dict()
        self.dataStr = ""
        self.disasterData = dict()
        self.disasterStr = ""
        
    def submitRequest(self):
        
        http = urllib3.PoolManager()
        
        #Format the Query
        sdmQuery = ("SELECT L.areasymbol AS Area_symbol, L.areaname AS Area_name, M.musym\n"
          + "AS Map_unit_symbol, M.muname AS Map_unit_name, M.mukey AS MUKEY,\n"
          + "comppct_r AS component_Percent, compname AS Component_name\n"
          + "FROM legend AS L\n"
          + "INNER JOIN mapunit AS M ON M.Lkey = L.Lkey\n"
          + "AND M.mukey IN (SELECT mukey FROM"
          + " SDA_Get_Mukey_from_intersection_with_WktWgs84('point (" 
          + str(self.long) + " " + str(self.lat) + ")'))\n"
          + "LEFT OUTER JOIN component AS c ON M.mukey = c.mukey\n"
          + "ORDER BY M.mukey DESC, comppct_r DESC")
        
        url = "https://SDMDataAccess.sc.egov.usda.gov/Tabular/SDMTabularService/post.rest"
        dRequest = dict()
        dRequest["FORMAT"] = "JSON"
        dRequest["QUERY"] = sdmQuery

        encodedData = json.dumps(dRequest).encode('utf-8')
        r = http.request('POST', url, body=encodedData)
        
        self.soilData = json.loads(r.data.decode('utf-8'))
        
    def formatSoilDataString(self):
        
        columnHeaders = ['Area Symbol', 'Area Name', 'Map Unit Symbol', 
                         'Map Unit Name', 'Map Unit Key', 'Soil Component Percent', 
                         'Soil Component Name']
        formattedTable = pandas.DataFrame(self.soilData['Table'], columns = columnHeaders)
        
        outputStr = ("Returned Values;" + str(len(formattedTable['Soil Component Name']))
                     + ";" )
        
        if len(pandas.unique(formattedTable['Map Unit Symbol']))==1:
            for header in columnHeaders:
                if header=='Soil Component Percent' or header=='Soil Component Name':
                    outputStr = (outputStr + header + ';'
                                 + ';'.join(list(formattedTable[header].values)) + ';')
                elif header!='Area Symbol' and header!='Map Unit Key':
                    outputStr = (outputStr + header + ';'
                                 + formattedTable[header][1] + ';')
            
        else :   
            for header in columnHeaders:
                outputStr = (outputStr + header + ';'
                             + ';'.join(list(formattedTable[header].values)) + ';')
                
        self.dataStr = bytes(outputStr, 'utf8')
        
            
if __name__ == "__main__":

    #sdr = soilDataRequest(-76.8, 38.9)
    #sdr.submitRequest()
    #sdr.formatSoilDataString()
    #print(sdr.dataStr)
    
    #Example coordinate values
    randomCoordinates = [[-77.380574, 38.790458],[-79.883852, 39.574043], 
                         [-107.146538, 36.883092],[-96.099318, 35.922848],
                         [-77.098347, 39.023427],[-77.037128, 38.923906]]
    for coords in randomCoordinates:
        sdr = soilDataRequest(coords[0], coords[1])
        sdr.submitRequest()
        sdr.formatSoilDataString()
    
        print(sdr.dataStr)
        print('\n\n')