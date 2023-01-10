#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Adolfo.Diaz
#
# Created:     13/12/2022
# Copyright:   (c) Adolfo.Diaz 2022
# Licence:     <your licence>
#-------------------------------------------------------------------------------

# Import modules
import sys, string, os, traceback, glob, csv
import urllib, re, time, json, socket, zipfile
import threading, multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime
from osgeo import gdal
from osgeo import osr

def errorMsg(errorOption=1):
    try:

        exc_type, exc_value, exc_traceback = sys.exc_info()
        theMsg = "\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[1] + "\n\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[-1]

        if errorOption==1:
            AddMsgAndPrint(theMsg)
        else:
            return theMsg

    except:
        AddMsgAndPrint("Unhandled error in unHandledException method")
        pass

## ===================================================================================
def getRasterInformation(raster):

    # Raster information will be added to dlStatusList
    # sourceID,prod_title,path,numofFiles,unzipSize,timestamp,downloadstatus --
    # add additional data:
    # columns,rows,cellsize,bandcount,bitDepth,nodatavalue,

    # query srid
    # describe num of bands; what if it is more than 1

    try:

        rds = gdal.Open(raster)
        rdsInfo = gdal.Info(rds,format="json")

        # Raster Properties
        columns = rdsInfo['size'][0]
        rows = rdsInfo['size'][1]
        bandCount = rds.RasterCount
        bitDepth = rdsInfo['bands'][0]['type']
        cellSize = rds.GetGeoTransform()[1]
        rdsFormat = rdsInfo['driverLongName']
        noDataVal = rdsInfo['bands'][0]['noDataValue']

        # Raster Statistics
        minStat = rdsInfo['bands'][0]['min']
        meanStat = rdsInfo['bands'][0]['mean']
        maxStat = rdsInfo['bands'][0]['max']
        stDevStat = rdsInfo['bands'][0]['stdDev']
        blockXsize = rdsInfo['bands'][0]['block'][0]
        blockYsize = rdsInfo['bands'][0]['block'][1]

        # Raster CRS Information
        # What is returned when a raster is undefined??
        prj = rds.GetProjection()  # GDAL returns projection in WKT
        srs = osr.SpatialReference(prj)
        srs.AutoIdentifyEPSG()
        srid = srs.GetAttrValue('AUTHORITY',1)

        if srs.IsProjected:
            srsName = srs.GetAttrValue('projcs')
        else:
            srsName = srs.GetAttrValue('geogcs')

        # 'lowerLeft': [439994.0, 5139994.0]
        #lowerLeft = rdsInfo['cornerCoordinates']['lowerLeft']
        #lowerRight = rdsInfo['cornerCoordinates']['lowerRight']
        #upperRight = rdsInfo['cornerCoordinates']['upperRight']
        #upperLeft = rdsInfo['cornerCoordinates']['upperLeft']

        right,top = rdsInfo['cornerCoordinates']['upperRight']   # Eastern-Northern most extent
        left,bottom = rdsInfo['cornerCoordinates']['lowerLeft']  # Western - Southern most extent

        return columns,rows,bandCount,cellSize,rdsFormat,bitDepth,noDataVal,srid,srsName,top\
               ,left,right,bottom,minStat,meanStat,maxStat,stDevStat,blockXsize,blockYsize

    except:
        errorMsg()

## ===================================================================================
if __name__ == '__main__':

    #try:

    downloadFile = r'E:\python_scripts\DSHub\LinuxWorkflow\TEMP_Update_DL_StatusFile\USGS_3DEP_3M_Metadata_Elevation_11192022_Download_Status.txt'
    updatedFile = r'E:\python_scripts\DSHub\LinuxWorkflow\TEMP_Update_DL_StatusFile\USGS_3DEP_3M_Metadata_Elevation_UPDATED.txt'

    #sourceID,prod_title,path,numofFiles,unzipSize,timestamp,downloadstatus
    headerItems = {
        "sourceID":0,
        "prod_title":1,
        "path":2,
        "numofFiles":3,
        "unzipSize":4,
        "timestamp":5,
        "downloadstatus":6,}

    urlDownloadDict = dict()
    recCount = 0
    badLines = 0

    """ ---------------------------- Open Download File and Parse Information ----------------------------------"""
    with open(downloadFile, 'r') as fp:
        for line in fp:
            items = line.split(',')

            # Skip header line and empty
            if recCount == 0 or line == "\n":
                recCount+=1
                continue

            # Skip if number of items are incorrect
            if len(items) != len(headerItems):
                badLines+=1
                continue

            sourceID = items[headerItems["sourceID"]]
            prod_title = items[headerItems["prod_title"]].strip()
            path = items[headerItems["path"]]
            numofFiles = items[headerItems["numofFiles"]]
            timestamp = items[headerItems["timestamp"]]
            downloadstatus = items[headerItems["downloadstatus"]]
            break

##            if huc8digit in urlDownloadDict:
##                urlDownloadDict[huc8digit].append([downloadURL,sourceID,prod_title,fileFormat])
##            else:
##                urlDownloadDict[huc8digit] = [[downloadURL,sourceID,prod_title,fileFormat]]

            recCount+=1

##    except:
##        errorMsg()