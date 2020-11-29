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
        #self.soilData = dict()
        #self.dataStr = ""
        #self.femaStr = ""
        #self.disasterData = dict()
        #self.disasterStr = ""
        
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
        
        sdmUrl = "https://SDMDataAccess.sc.egov.usda.gov/Tabular/SDMTabularService/post.rest"
        dRequest = dict()
        dRequest["FORMAT"] = "JSON"
        dRequest["QUERY"] = sdmQuery

        encodedData = json.dumps(dRequest).encode('utf-8')
        r = http.request('POST', sdmUrl, body=encodedData)
        
        self.soilData = json.loads(r.data.decode('utf-8'))
        
    def formatSoilDataString(self):
        
        columnHeaders = ['Area Symbol', 'Area Name', 'Map Unit Symbol', 
                         'Map Unit Name', 'Map Unit Key', 'Soil Component Percent', 
                         'Soil Component Name']
        self.soilTable = pandas.DataFrame(self.soilData['Table'], columns = columnHeaders)
        #self.soilTable[['County', 'State']] = self.soilTable['Area Name'].str.split(",",expand=True)
        
        outputStr = ('Returned Values;' + str(len(self.soilTable['Soil Component Name']))
                     + ";" )
        
        if len(pandas.unique(self.soilTable['Map Unit Symbol']))==1:
            for header in columnHeaders:
                if header=='Soil Component Percent' or header=='Soil Component Name':
                    outputStr = (outputStr + header + ';'
                                 + ';'.join(list(self.soilTable[header].values)) + ';')
                elif header!='Area Symbol' and header!='Map Unit Key':
                    # Split 'Area Name' into seperate County and State Values
                    if header=='Area Name':
                        areaName = self.soilTable['Area Name'][0]
                        if areaName.find('County,') != -1:
                           outputStr = (outputStr + 'County' + ';'
                                     + areaName[0:areaName.find('County,')+6] + ';'
                                     + 'State' + ';' + areaName[areaName.find('County,')+8:] + ';')
                        elif areaName.find('Counties,') != -1:
                           outputStr = (outputStr + 'County' + ';'
                                     + areaName[0:areaName.find('Counties,')+8] + ';'
                                     + 'State' + ';' + areaName[areaName.find('Counties,')+10:] + ';')
            
                        elif areaName.find('District of Columbia') != -1:
                            outputStr = (outputStr + 'County' + ';'
                                     + 'District of Columbia' + ';'
                                     + 'State' + ';' + 'DC' + ';')
                       
                    else:
                        outputStr = (outputStr + header + ';'
                                     + self.soilTable[header][0] + ';')
            
        else:   
            for header in columnHeaders:
                outputStr = (outputStr + header + ';'
                             + ';'.join(list(self.soilTable[header].values)) + ';')
                
        self.dataStr = bytes(outputStr, 'utf8')
        
      
    def getFemaData(self):
        http = urllib3.PoolManager()
        
        if len(pandas.unique(self.soilTable['Map Unit Symbol']))==1:
            
            self.state = self.soilTable['Area Symbol'][0][0:2]
            areaName = self.soilTable['Area Name'][0]

            if areaName.find(' Counties,') != -1:
                self.disasterData = []
            
            else:
                
                if areaName.find(' County,') != -1:
                    self.county = areaName[0:areaName.find(' County,')]
                elif areaName.find('District of Columbia') != -1:
                    self.county = 'District of Columbia'
            
                femaUrl = ("https://www.fema.gov/api/open/v2/DisasterDeclarations"
                           +"Summaries?$filter=state eq '" + self.state + "'")

                r = http.request('GET', femaUrl)
                
                self.disasterData = json.loads(r.data.decode('utf-8'))
        
        else:
            self.disasterData = []         
    
    def formatFemaDataString(self):
        
        if self.disasterData:
            
            self.disasterTable = pandas.DataFrame(self.disasterData['DisasterDeclarationsSummaries'])
            self.disasterTable = self.disasterTable[self.disasterTable['designatedArea'].str.contains(self.county)]
            self.disasterTable = self.disasterTable[~self.disasterTable['incidentType'].str.contains("Biological")]

            self.disasterTable = self.disasterTable[['fyDeclared','state','designatedArea','incidentType','declarationTitle']]
            #self.disasterTable = self.disasterTable[['fyDeclared','incidentType','declarationTitle']]
            
            columnHeaders = ['Year', 'State', 'County', 'Incident Type', 'FEMA Decleration Type']
            self.disasterTable.columns = columnHeaders
            
            outputStr = ('Returned Values;' + str(len(self.disasterTable['Year']))
                         + ";" )
            
            for header in columnHeaders:
                outputStr = (outputStr + header + ';' 
                             + ';'.join(list(str(s) for s in self.disasterTable[header].values)) + ';')

            self.dataStr = (self.dataStr + bytes(outputStr, 'utf8'))
       
        else:
            
            self.disasterTable = []
            outputStr = 'Returned Values;0;Search area too large;'
            self.dataStr = (self.dataStr + bytes(outputStr, 'utf8'))
     
     
if __name__ == "__main__":
    
    #Example coordinate values
    #randomCoordinates = [[-77.380574, 38.790458],[-79.883852, 39.574043], 
    #                     [-107.146538, 36.883092],[-96.099318, 35.922848],
    #                     [-77.098347, 39.023427],[-77.037128, 38.923906]]
    
    demoCoordinates = [[-76.88, 38.81], [-77.01, 38.92],
                       [-77.14, 38.82], [-77.00, 38.70]]
    #for coords in randomCoordinates:
    for coords in demoCoordinates:
        
        sdr = soilDataRequest(coords[0], coords[1])
        sdr.submitRequest()
        sdr.formatSoilDataString()
        sdr.getFemaData()
        sdr.formatFemaDataString()
        print(sdr.dataStr)
        print('\n')