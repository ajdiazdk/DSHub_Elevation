# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 10:12:13 2022

@author: Adolfo.Diaz
"""

##import numpy as np
##import matplotlib.pyplot as plt
##
##x = np.arange(0, 30, 3)
##y = np.arange(0, 30, 3)

## ===================================================================================
def AddMsgAndPrint(msg, severity=0):
    # prints message to screen if run as a python script
    # Adds tool message to the geoprocessor
    #
    #Split the message on \n first, so that if it's multiple lines, a GPMessage will be added for each line
    try:

        print(msg)
        #for string in msg.split('\n'):
            #Add a geoprocessing message (in case this is run as a tool)
        if severity == 0:
            arcpy.AddMessage(msg)

        elif severity == 1:
            arcpy.AddWarning(msg)

        elif severity == 2:
            arcpy.AddError("\n" + msg)

    except:
        pass

## ===================================================================================
def errorMsg():
    try:

        exc_type, exc_value, exc_traceback = sys.exc_info()
        theMsg = "\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[1] + "\n\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[-1]

        if theMsg.find("exit") > -1:
            AddMsgAndPrint("\n\n")
            pass
        else:
            AddMsgAndPrint(theMsg,2)

    except:
        AddMsgAndPrint("Unhandled error in unHandledException method", 2)
        pass

## ================================================================================================================
def tic():
    """ Returns the current time """

    return time.time()

## ================================================================================================================
def toc(_start_time):
    """ Returns the total time by subtracting the start time - finish time"""

    try:

        t_sec = round(time.time() - _start_time)
        (t_min, t_sec) = divmod(t_sec,60)
        (t_hour,t_min) = divmod(t_min,60)

        if t_hour:
            return ('{} hour(s): {} minute(s): {} second(s)'.format(int(t_hour),int(t_min),int(t_sec)))
        elif t_min:
            return ('{} minute(s): {} second(s)'.format(int(t_min),int(t_sec)))
        else:
            return ('{} second(s)'.format(int(t_sec)))

    except:
        errorMsg()

## ===================================================================================
def getPortalTokenInfo(portalURL):

    try:

        # Returns the URL of the active Portal
        # i.e. 'https://gis.sc.egov.usda.gov/portal/'
        activePortal = arcpy.GetActivePortalURL()

        # {'SSL_enabled': False, 'portal_version': 6.1, 'role': '', 'organization': '', 'organization_type': ''}
        #portalInfo = arcpy.GetPortalInfo(activePortal)

        # targeted portal is NOT set as default
        if activePortal != portalURL:

               # List of managed portals
               managedPortals = arcpy.ListPortalURLs()

               # portalURL is available in managed list
               if activePortal in managedPortals:
                   AddMsgAndPrint("\nYour Active portal is set to: " + activePortal,2)
                   AddMsgAndPrint("Set your active portal and sign into: " + portalURL,2)
                   return False

               # portalURL must first be added to list of managed portals
               else:
                    AddMsgAndPrint("\nYou must add " + portalURL + " to your list of managed portals",2)
                    AddMsgAndPrint("Open the Portals Tab to manage portal connections",2)
                    AddMsgAndPrint("For more information visit the following ArcGIS Pro documentation:",2)
                    AddMsgAndPrint("https://pro.arcgis.com/en/pro-app/help/projects/manage-portal-connections-from-arcgis-pro.htm",1)
                    return False

        # targeted Portal is correct; try to generate token
        else:

            # Get Token information
            tokenInfo = arcpy.GetSigninToken()

            # Not signed in.  Token results are empty
            if not tokenInfo:
                AddMsgAndPrint("\nYou are not signed into: " + portalURL,2)
                return False

            # Token generated successfully
            else:
                return tokenInfo

    except:
        errorMsg()
        return False

## ===================================================================================
def submitImageServiceQuery(INparamsDictVal):
    """ This function will send an XY query to an image service and convert
        the results into a python structure.  If the results from the service is an
        error due to an invalid token then a second attempt will be sent with using
        a newly generated arcgis token.  If the token is good but the request returned
        with an error a second attempt will be made.  The funciion takes in 2 parameters,
        the URL to the web service and a query string in URLencoded format.

        Error produced with invalid token
        {u'error': {u'code': 498, u'details': [], u'message': u'Invalid Token'}}

        The function returns requested data via a python dictionary"""

    try:
        url = NED_10Mservice_URL

        id = INparamsDictVal[0]
        INparams = INparamsDictVal[1]

        # Data should be in bytes; new in Python 3.6
        INparams = INparams.encode('ascii')
        resp = urllib.request.urlopen(NED_10Mservice_URL,INparams)  # A failure here will probably throw an HTTP exception

        responseStatus = resp.getcode()
        responseMsg = resp.msg
        jsonString = resp.read()

        # json --> Python; dictionary containing 1 key with a list of lists
        results = json.loads(jsonString)

        # Check for expired token; Update if expired and try again
        if 'error' in results.keys():
           if results['error']['message'] == 'Invalid Token':
               AddMsgAndPrint("\tRegenerating ArcGIS Token Information")

               # Get new ArcPro Token
               newToken = arcpy.GetSigninToken()

               # Update the original portalToken
               global portalToken
               portalToken = newToken

               # convert encoded string into python structure and update token
               # by parsing the encoded query strting into list of (name, value pairs)
               # i.e [('f', 'json'),('token','U62uXB9Qcd1xjyX1)]
               # convert to dictionary and update the token in dictionary

               queryString = parseQueryString(params)

               requestDict = dict(queryString)
               requestDict.update(token=newToken['token'])

               newParams = urllibEncode(requestDict)
               newParams = newParams.encode('ascii')

               # update incoming parameters just in case a 2nd attempt is needed
               INparams = newParams

               resp = urllib.request.urlopen(NED_10Mservice_URL,newParams)  # A failure here will probably throw an HTTP exception

               responseStatus = resp.getcode()
               responseMsg = resp.msg
               jsonString = resp.read()

               results = json.loads(jsonString)

        # Check results before returning them; Attempt a 2nd request if results are bad.
        if 'error' in results.keys() or len(results) == 0:
            time.sleep(5)

            resp = urllib.request.urlopen(NED_10Mservice_URL,INparams)  # A failure here will probably throw an HTTP exception

            responseStatus = resp.getcode()
            responseMsg = resp.msg
            jsonString = resp.read()

            results = json.loads(jsonString)

            if 'error' in results.keys() or len(results) == 0:
                AddMsgAndPrint("\t2nd Request Attempt Failed - Error Code: " + str(responseStatus) + " -- " + responseMsg + " -- " + str(results),2)
                return (id,False)
            else:
                return (id,results)

        else:
             return (id,results)

    except httpErrors as e:

        if int(e.code) >= 500:
           #AddMsgAndPrint("\n\t\tHTTP ERROR: " + str(e.code) + " ----- Server side error. Probably exceed JSON imposed limit",2)
           #AddMsgAndPrint("t\t" + str(request))
           pass
        elif int(e.code) >= 400:
           #AddMsgAndPrint("\n\t\tHTTP ERROR: " + str(e.code) + " ----- Client side error. Check the following SDA Query for errors:",2)
           #AddMsgAndPrint("\t\t" + getGeometryQuery)
           pass
        else:
           AddMsgAndPrint('HTTP ERROR = ' + str(e.code),2)

    except:
        errorMsg()
        return (id,False)

## ====================================== Main Body ==================================
# Import modules
import sys, string, os, traceback
import urllib, re, time, json, multiprocessing
import arcgisscripting, arcpy
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

from urllib.request import Request, urlopen
from urllib.error import HTTPError as httpErrors

urllibEncode = urllib.parse.urlencode
parseQueryString = urllib.parse.parse_qsl

if __name__ == '__main__':

    try:
        # Use most of the cores on the machine where ever possible
        arcpy.env.parallelProcessingFactor = "100%"

        cellGrid = r'E:\python_scripts\DSHub\Elevation_Grid_API\Elevation_Grid_API\Default.gdb\Huc12_fish_3M_subset'
        #cellCentroid = r'E:\python_scripts\DSHub\Elevation_Grid_API\Elevation_Grid_API\Default.gdb\Huc12_fish_3M_label_subset2'
        cellCentroid = r'E:\python_scripts\DSHub\Elevation_Grid_API\Elevation_Grid_API\Default.gdb\TOP1000000_POINTS'
        totalFeatures = arcpy.GetCount_management(cellCentroid)[0]
        start = tic()

        """ ---------------------------------------------- ArcGIS Portal Information ---------------------------"""
        nrcsPortal = 'https://gis.sc.egov.usda.gov/portal/'
        portalToken = getPortalTokenInfo(nrcsPortal)
        #portalToken = {'token': '5PkSO0ZZcNVv7eEzXz8MTZBxgZbenP71uyMNnYXOefTqYs8rh0TJFGk7VKyxozK1vHOhKmpy2Z2M6mr-pngEbKjBxgIVeQmSnlfANwGXfEe5aOZjgQOU2UfLHEuGEIn1R0d0HshCP_LDtwn1-JPhbnsevrLY2a-LxTQ6D4QwCXanJECA7c8szW_zv30MxX6aordbhxHnugDD1pzCkPKRXkEoHR7r-dQxuaFSczD1jLFyDNB-7vdakAzhLc2xHPidLGt0PNileXzIecb2SA8PLQ..', 'referer': 'http://www.esri.com/AGO/8ED471D4-0B17-4ABC-BAB9-A9433506FD1C', 'expires': 1584646706}

        if not portalToken:
           AddMsgAndPrint("Could not generate Portal Token. Exiting!",2)
           exit()

        """ --------------------------------------------- get Feature Service Metadata -------------------------------"""
        # URL for Identify operation for the bare_earth_10M
        NED_10Mservice_URL = r'https://gis.sc.egov.usda.gov/image/rest/services/elevation/bare_earth_10m/ImageServer/identify'

        demPixelRequests = dict()
        demPixelResults = dict()
        searchCurStart = tic()

        for row in arcpy.da.SearchCursor(cellCentroid, ['OID@','SHAPE@X','SHAPE@Y']):

            params = urllibEncode({'f': 'json',
                                   'geometry': {'x': row[1], 'y': row[2]},
                                   'geometryType':'esriGeometryPoint',
                                   'returnGeometry':'False',
                                   'returnCatalogItems':'False',
                                   'returnCountOnly':'False',
                                   'returnPixelValues':'False',
                                   'token': portalToken['token']})

            demPixelRequests[row[0]] = params

        searchCurEnd = toc(searchCurStart)
        print(f"\nTime to create request URLs: {searchCurEnd}\n")

        serviceReqStart = tic()
        i = 0

        # Create an Executor to manage all tasks.  Using the with statement creates a context
        # manager, which ensures any stray threads or processes get cleaned up properly when done.
        with ThreadPoolExecutor(2) as executor:

            # use a set comprehension to start all tasks.  This creates a future object
            future_to_url = {executor.submit(submitImageServiceQuery, request): request for request in demPixelRequests.items()}

##            # yield future objects as they are done.
##            for future in as_completed(future_to_url):
##                result = future.result()
##                pixelID = result[0]
##                serviceReturn = result[1]
##
##                if 'value' in serviceReturn:
##                    demValue = serviceReturn['value']
##
##                    if not demValue == 'NoData':
##                        demPixelResults[pixelID] = float(demValue)
##                    else:
##                        demPixelResults[pixelID] = 99999

##                i+=1
##                print(f"Completed {i} of {totalFeatures}")

        serviceReqEnd = toc(serviceReqStart)
        print(f"\nTotal Service Request Time: {serviceReqEnd}\n")

        print(f"\nTotal Time ----------------{toc(start)}")

    except:
        errorMsg()



