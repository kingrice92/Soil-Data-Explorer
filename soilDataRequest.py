#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 15:09:21 2020

@author: Neil Rice

This class operates as the data storage module. The methods of this class 
communicate with the FEMA and soil databases through the use of REST APIs.
"""

import json
import urllib3
import pandas

class soilDataRequest():
   
    def __init__(self, longitude, latitude):
        self.long   = longitude
        self.lat = latitude
        
    def submitRequest(self):
        #This method constructs SQL queries to be sent to the Soil Data Mart.
        
        http = urllib3.PoolManager()
        #Build SQL query for Soil Data Mart.  
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
        
        #Define URL of the post.rest web service for the soil database. 
        sdmUrl = "https://SDMDataAccess.sc.egov.usda.gov/Tabular/SDMTabularService/post.rest"
        #Build JSON request.
        dRequest = dict()
        dRequest["FORMAT"] = "JSON"
        dRequest["QUERY"] = sdmQuery
        encodedData = json.dumps(dRequest).encode('utf-8')
        #Send the request.
        r = http.request('POST', sdmUrl, body=encodedData)
        #Unpack returned data.
        self.soilData = json.loads(r.data.decode('utf-8'))
        
    def formatSoilDataString(self):
        #This method reduces the data returned from the Soil Data Mart to a
        #single output data string for parsing by the other modules.
        
        #Create column names for pandas dataframe.
        columnHeaders = ['Area Symbol', 'Area Name', 'Map Unit Symbol', 
                         'Map Unit Name', 'Map Unit Key', 'Soil Component Percent', 
                         'Soil Component Name']
        #Create a table with the returned SDM data.
        self.soilTable = pandas.DataFrame(self.soilData['Table'], columns = columnHeaders)
        
        #Construct output data string. 
        if len(pandas.unique(self.soilTable['Map Unit Symbol']))==1:
            #Initialize output data string.
            outputStr = ('Returned Values;' + str(len(self.soilTable['Soil Component Name']))
                         + ";" )
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
            #If too many map unit symbols, create alternate return string.
            outputStr = 'Returned Values;0;Too many map unit symbols;'
        
        #Convert data string to a byte array.
        self.dataStr = bytes(outputStr, 'utf8')
        
    def getFemaData(self):
        #This method constructs url queries to be sent to the FEMA database.
        
        http = urllib3.PoolManager()
        #Extract the state name.
        if len(pandas.unique(self.soilTable['Map Unit Symbol']))==1:
            self.state = self.soilTable['Area Symbol'][0][0:2]
            areaName = self.soilTable['Area Name'][0]

            if areaName.find(' Counties,') != -1:
                self.disasterData = []
            
            else:
                #Extract the area name.
                if areaName.find(' County,') != -1:
                    self.county = areaName[0:areaName.find(' County,')]
                elif areaName.find('District of Columbia') != -1:
                    self.county = 'District of Columbia'
                    
                #Use the State name to build the query for the FEMA database.
                femaUrl = ("https://www.fema.gov/api/open/v2/DisasterDeclarations"
                           +"Summaries?$filter=state+eq+'" + self.state + "'")

                #Send the request.
                r = http.request('GET', femaUrl)
                #Unpack returned data.
                self.disasterData = json.loads(r.data.decode('utf-8'))
        
        else:
            #If more than one Area Name is returned, do not search FEMA database.
            self.disasterData = []         
    
    def formatFemaDataString(self):
        #This method reduces the data returned from the FEMA database to a
        #single output data string for parsing by the other modules.
        
        if self.disasterData:
            #Create a table with the returned FEMA data.
            self.disasterTable = pandas.DataFrame(self.disasterData['DisasterDeclarationsSummaries'])
            #Only keep the data from the relevant county.
            self.disasterTable = self.disasterTable[self.disasterTable['designatedArea'].str.contains(self.county)]
            #Do not include biological disasters.
            self.disasterTable = self.disasterTable[~self.disasterTable['incidentType'].str.contains("Biological")]
            self.disasterTable = self.disasterTable[['fyDeclared','state','designatedArea','incidentType','declarationTitle']]
            
            #Create column names for pandas dataframe.
            columnHeaders = ['Year', 'State', 'County', 'Incident Type', 'FEMA Decleration Type']
            self.disasterTable.columns = columnHeaders
            
            #Initialize output data string.
            outputStr = ('Returned Values;' + str(len(self.disasterTable['Year']))
                         + ";" )
            
            #Construct output data string.
            for header in columnHeaders:
                outputStr = (outputStr + header + ';' 
                             + ';'.join(list(str(s) for s in self.disasterTable[header].values)) + ';')

            #Append FEMA data string to SDM data string.
            self.dataStr = (self.dataStr + bytes(outputStr, 'utf8'))
       
        else:
            
            #If no FEMA data is returned, create alternate return string.
            self.disasterTable = []
            outputStr = 'Returned Values;0;Search area too large;'
            self.dataStr = (self.dataStr + bytes(outputStr, 'utf8'))
     
     
if __name__ == "__main__":
    
    demoCoordinates = [[-76.88, 38.81], [-77.01, 38.92],
                       [-77.14, 38.82], [-77.00, 38.70]]

    for coords in demoCoordinates:
        
        sdr = soilDataRequest(coords[0], coords[1])
        sdr.submitRequest()
        sdr.formatSoilDataString()
        sdr.getFemaData()
        sdr.formatFemaDataString()
        print(sdr.dataStr)
        print('\n')