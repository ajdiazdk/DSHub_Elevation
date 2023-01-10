# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 10:12:13 2022

@author: Adolfo.Diaz


3DEP Elevation Identify service:
https://elevation.nationalmap.gov/arcgis/rest/services/3DEPElevation/ImageServer/identify
use: -10075221.0661,5413262.007700004 coordinates to test
Coordinates are in 3857 Web Mercator.  OLD WKID: 102100
"""




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

#### ===================================================================================
def submitImageServiceQuery(INparamsDictVal):
    """ This function will send an XY query to an elevation service and return the results.
        If the results from the service is an error due to an invalid token then a second
        attempt will be sent using a newly generated arcgis token.  If the token is good
        but the request returned with an error a second attempt will be made.

        The funcition takes in a tuple with 2 values:
            1) OBJECTID of the centroid
            2) encoded JSON request

        Sample of request item within INparamsDictVal:
            (1, b'f=json&geometry=%7B%27x%27%3A+-90.50758637199993%2C+%27y%27%3A+43.661870865000026%7D&
                  geometryType=esriGeometryPoint&returnGeometry=False&returnCatalogItems=False&returnCountOnly=False&
                  returnPixelValues=False&token=EE1Wd9KWaugIkXEcBTqgz9SJYujkpUsMTB3tEf-VnRKjGU9YL8Ct6iDBp9_HnTYENnFoM4YBVNNDxo3oumFWVekvxlKuA-U_fDi-9B9nqPglM8CRh9QrxaZDGmNZos-EWl-3NVCfjBpT1iE8kKIVvtFEx4krq92hqZZa5B1LN4tvxBl0EVHs8CIVlnlPJfweApGqwUfgIa1rjz1-KiSbSyQNYqm_BhwvBGNZCBysk14dvl-dxBSKsrZIPC_xSItg1jgV97kWk1QKuDMVJZ3CVw..')

        The function returns a tuple with 2 values
            1) OBJECTID of the centroid
            2) Dictionary of REST API results

        Sample of REST API results:
            {'objectId': 0, 'name': 'Pixel', 'value': '404.747', 'location': {'x': -90.50751178799999, 'y': 43.661867759000074, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}}, 'properties': None, 'catalogItems': None, 'catalogItemVisibilities': []} """

    try:

        # parse incoming tuple
        id = INparamsDictVal[0]
        urlRequest = INparamsDictVal[1]

        # Send REST API request
        with urllib.request.urlopen(urlRequest) as conn:
            resp = conn.read()

        results = json.loads(resp)
        return(id,results)

        #resp = urllib.request.urlopen(urlRequest)  # A failure here will probably throw an HTTP exception

##        responseStatus = resp.getcode()
##        responseMsg = resp.msg
##        jsonString = resp.read()
##
##        # json --> Python
##        results = json.loads(jsonString)
##        return(id,results)

    except httpErrors as e:

        if int(e.code) >= 500:
            print(f"\n\tHTTP ERROR: {str(e.code)} ----- Server side error")
            BadURLs.append(urlRequest)
            return (id,False)
        elif int(e.code) >= 400:
            print(f"\n\tHTTP ERROR: {str(e.code)} ---- Client side error ")
            BadURLs.append(urlRequest)
            return (id,False)
        else:
            print(f"HTTP ERROR = {str(e.code)}")
            BadURLs.append(urlRequest)
            return (id,False)

    except:
        errorMsg()
        BadURLs.append(urlRequest)
        return (id,False)

## ===================================================================================
def getPixelValue(restAPIresult):
    """This function will isolate the elevation value from the REST API results and
        populate a dictionary with OBJECTID and elevation integer value.

        (1,
        {'objectId': 0,
        'name': 'Pixel',
        'value': '404.747',
        'location': {'x': -90.50758637199993,
        'y': 43.661870865000026,
        'spatialReference': {'wkid': 4326, 'latestWkid': 4326}},
        'properties': None,
        'catalogItems': None,
        'catalogItemVisibilities': []}) """

    try:

        pixelID = restAPIresult[0]
        serviceReturn = restAPIresult[1]

        if serviceReturn == False:
            demPixelResults[pixelID] = 99999

        elif 'value' in serviceReturn:
            demValue = serviceReturn['value']

            if not demValue == 'NoData':
                demPixelResults[pixelID] = float(demValue)
            else:
                demPixelResults[pixelID] = 99999

        else:
            demPixelResults[pixelID] = 99999

    except:
        print(f"======================= {restAPIresult}")
        errorMsg()

## ====================================== Main Body ==================================
# Import modules
import sys, string, os, traceback
import urllib, re, time, json
import arcgisscripting, arcpy, threading
import concurrent.futures

from urllib.request import Request, urlopen
from urllib.error import HTTPError as httpErrors
urllibEncode = urllib.parse.urlencode

if __name__ == '__main__':

    try:

        start = tic()
        cellGrid = r'E:\python_scripts\DSHub\Elevation_Grid_API\Elevation_Grid_API\Default.gdb\Huc12_fish_3M_subset'
        #cellCentroid = r'G:\ESRI_stuff\python_scripts\DSHub\Elevation_Grid_API\Remote.gdb\TOP100_POINTS'
        cellCentroid = r'G:\ESRI_stuff\python_scripts\DSHub\Elevation_Grid_API\Points_3857.gdb\TOP1000000_POINTS'

        totalFeatures = int(arcpy.GetCount_management(cellCentroid)[0])
        maxRequestsPerBatch = 1000000
        pauseTimePerBatch = 10

        # URL for Identify operation for the bare_earth_10M
        #NED_10Mservice_URL = r'https://gis.sc.egov.usda.gov/image/rest/services/elevation/bare_earth_10m/ImageServer/identify'
        NED_10Mservice_URL = r'https://elevation.nationalmap.gov/arcgis/rest/services/3DEPElevation/ImageServer/identify'

        demPixelRequests = dict()  # Dict of API server requests {1:
        demPixelResults = dict()
        BadURLs = list()
        urlList = list()
        i = 0  # counter for TOTAL number of requests compiled
        j = 0  # counter for maxRequestsPerBatch; Resets after number is reached
        k = 1  # counter for batches
        ii = 1 # counter for completed requests

        print(f"\nCOMPILING REQUESTS")
        # Create dictionary of OIDs (keys) and API request (values)
        for row in arcpy.da.SearchCursor(cellCentroid, ['OID@','SHAPE@X','SHAPE@Y']):

            # Formulate API rquest
            params = urllibEncode({'f': 'json',
                                   'geometry': {'x': row[1], 'y': row[2]},
                                   'geometryType':'esriGeometryPoint',
                                   'returnGeometry':'False',
                                   'returnCatalogItems':'False'})
                                   #'returnCountOnly':522222222222222222222222222222'False',
                                   #'returnPixelValues':'False'})
                                   #'token': portalToken['token']})

            # concatenate URL and API parameters
            url = f"{NED_10Mservice_URL}?{params}"
            #urlList.append(url)

            # Data should be in bytes; new in Python 3.6
            #INparams = params.encode('ascii')

            # objectID and JSON request dict
            demPixelRequests[row[0]] = url
            i+=1
            j+=1

            # send requests, reset counts, clear request dictionary
            if j == maxRequestsPerBatch or i == totalFeatures:

                if j == maxRequestsPerBatch:
                    print(f"\nBATCH #{k} STARTING WITH {j} REQUESTS")
                elif k > 1 and i == totalFeatures:
                    print(f"\nSENDING LAST BATCH OF {j} REQUESTS")
                else:
                    print(f"\nSENDING {j} REQUESTS")

                with concurrent.futures.ThreadPoolExecutor() as executor:

                    # use a set comprehension to start all tasks.  This creates a future object
                    future_to_url = {executor.submit(submitImageServiceQuery, request): request for request in demPixelRequests.items()}

                    # yield future objects as they are done.
                    for future in concurrent.futures.as_completed(future_to_url):

                        getPixelValue(future.result())
                        print(f"\tCompleted {ii} of {totalFeatures} requests")
                        ii+=1

                demPixelRequests.clear()

                print(f"\n\tSUCCESSFULLY COMPLETED BATCH #{k}")
                k+=1
                j=0

                if not ii == totalFeatures:
                    print(f"\tPAUSING {pauseTimePerBatch} SECONDS")
                    time.sleep(pauseTimePerBatch)

        if len(BadURLs) > 0:
            print(f"\nThere were {len(BadURLs)} requets that were unsuccessful")

        print(f"\nTotal Time ----------------{toc(start)}")

    except:
        errorMsg()




