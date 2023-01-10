# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 10:12:13 2022
updated 1/6/2022

@author: Adolfo.Diaz


3DEP Elevation Identify service:
https://elevation.nationalmap.gov/arcgis/rest/services/3DEPElevation/ImageServer/identify
use: -10075221.0661,5413262.007700004 coordinates to test
Coordinates are in 3857 Web Mercator.  OLD WKID: 102100

r'https://tnmaccess.nationalmap.gov/api/v1/products?datasets=National%20Elevation%20Dataset%20(NED)%201/9%20arc-second&polyCode=07060002&polyType=huc8&max=2'

- Need to account for files associated with deprecated zip files.  i.e. if x.zip is no longer part of watershed A,
  files part of x.zip should be deleted.  Partial files can probably be deleted by associating file names with "x"
  but it becomes difficult if x.zip is deleted.
- Need to handle bad zip files.
- Need to handle replacing data.  Similar to first issue.

Things to consider/do:
    1) Rewrite the download status file so that it looks like the input Metadata file with additional information.
       This file can then be registered in the database and will contain all relevant elevation information
    2) Generate dlstatus file after the unzipping happens.  Much cleaner and faster.
    3) Switch to unzip by default.  much cleaner and faster.  Leave bUnzipFiles boolean in main function as
       an override function but remove from user input.

12/01/2022
Added CreateRaster2pgSQLFile function that will create text file containing the linux command
for each tile to be registered.  NOT COMPLETE.  Needed to grab raster information first.

12/09/2022
Created getRasterInformation function that will take in a raster and describe it's properties
and return them.  This will be used to append to the DLStatus file.
"""

# Import modules
import sys, string, os, traceback, glob, csv
import urllib, re, time, json, socket, zipfile
import threading, multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime
from osgeo import gdal
from osgeo import osr

from urllib.request import Request, urlopen, URLError
from urllib.error import HTTPError

urllibEncode = urllib.parse.urlencode

## ===================================================================================
def AddMsgAndPrint(msg):

    # Print message to python message console
    print(msg)

    # Add message to log file
    try:
        h = open(msgLogFile,'a+')
        h.write("\n" + msg)
        h.close
        del h
    except:
        pass

## ===================================================================================
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

## ================================================================================================================
def convert_bytes(sizeInBytes):
    """ integer byte conversion function.  Returns string """

    try:
        outFormat = [ "Bytes", "KB", "MB", "GB", "TB" ]

        i = 0
        double_bytes = sizeInBytes

        while (i < len(outFormat) and  sizeInBytes >= 1024):
            double_bytes = sizeInBytes / 1024.0
            sizeInBytes = sizeInBytes / 1024
            i+=1

        return f"{round(double_bytes, 2)} {outFormat[i]}"

    except:
        errorMsg()

## ===================================================================================
def DownloadElevationTile(itemCollection,downloadFolder):
    # Description
    # This function will open a URL and download the contents to the specified
    # download folder. If bReplaceData is True, delete the local file version of the
    # download file.
    # Total download file size will be tallied.
    # Information to create download Status file will be collected:
    # sourceID,prod_title,downloadPath,numOfFiles,size,now,True if successful download else False
    #
    # It can be used within the ThreadPoolExecutor to send multiple USGS downloads
    #
    # Parameters
    # itemCollection (dictionary):
    #   key:HUC
    #   values:
    #       url: https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n47x75_w120x25_wa_columbiariver_2010.zip
    #       sourceID = 17020010
    #       prod_title = r'USGS NED ned19_n47x75_w120x25_wa_columbiariver_2010 1/9 arc-second 2011 15 x 15 minute IMG'  OR
    #       prod_title = r'USGS 1 Meter 19 x44y515 ME_CrownofMaine_2018_A18'
    #       fileFormat = 'IMG' or 'GeoTIFF'
    #
    # Returns
    # a list of messages that will be printed as a "future" object representing the execution of the callable.

    try:
        messageList = list()
        global dlStatusList

        hucURL = itemCollection[0]
        sourceID = itemCollection[1]
        prod_title = itemCollection[2]
        fileFormat = itemCollection[3]

        theTab = "\t\t"

        # 'https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n30x50_w091x75_la_atchafalayabasin_2010.zip'
        fileName = hucURL.split('/')[-1]

        # set the download's output location and filename
        local_file = f"{downloadFolder}{os.sep}{fileName}"

        now = datetime.today().strftime('%m%d%Y %H:%M:%S')

        # if download file already exists, delete it if bReplaceData is True;
        # otherwise collect information
        if os.path.isfile(local_file):
            if bReplaceData:
                try:
                    os.remove(local_file)
                    #messageList.append(f"{theTab}{'File Exists; Deleted':<35} {fileName:<60}")
                except:
                    messageList.append(f"{theTab:q!}{'File Exists; Failed to Delete':<35} {fileName:<60}")
                    failedDownloadList.append(hucURL)
                    return messageList
            else:
                if zipfile.is_zipfile(local_file):
                    zip = zipfile.ZipFile(local_file)
                    size = sum([zinfo.file_size for zinfo in zip.filelist])
                    numOfFiles = len(zip.filelist)
                else:
                    numOfFiles = 1
                    size = os.stat(local_file).st_size

                dlStatusList.append([sourceID,prod_title,local_file,str(numOfFiles),str(size),now,'True'])
                messageList.append(f"{theTab}{'File Exists, Skipping:':<35} {fileName:<60} {convert_bytes(os.stat(local_file).st_size):>15}")
                return messageList

        # Download elevation zip file
        request = urlopen(hucURL)

        # save the download file to the specified folder
        output = open(local_file, "wb")
        output.write(request.read())
        output.close()

        # Log the size of the file downloaded; could be zip or individual file
        global totalDownloadSize
        dlSize = os.stat(local_file).st_size
        totalDownloadSize+=dlSize

        # TRY REMOVING THIS
        # If the file is a zip file collect the following info:
        # number of files, collective size of files unzipped and downloadpath
        # use the file format to search for raster file in the zipfile
        if zipfile.is_zipfile(local_file):
            zip = zipfile.ZipFile(local_file)
            size=0
            numOfFiles=0
            downloadPath = ""

            # INSERT GDAL FUNCTION

            if fileFormat.lower() == 'geotiff':
                fileType = 'tif'
            else:
                fileType = fileFormat.lower()

            for zinfo in zip.filelist:
                size+=zinfo.file_size
                numOfFiles+=1
                file = zinfo.filename.lower()

                # find the elevation raster file and create path
                if file.endswith(fileType):
                    downloadPath = downloadFolder + os.sep + zinfo.filename

            # file in zipfiles didn't match file format; grab file from prod_title (not ideal)
            if not downloadPath:
                downloadPath = f"{downloadFolder}{os.path}{prod_title.split(' ')[2]}.{fileType}"

            #numOfFiles = str(len(zip.namelist()))
            #size = str(sum([zinfo.file_size for zinfo in zip.filelist]))

        # file is not a zip; probably 1M
        else:
            numOfFiles = 1
            size = dlSize
            downloadPath = downloadFolder + os.sep + fileName

        messageList.append(f"{theTab}{'Successfully Downloaded:':<35} {fileName:<60} {convert_bytes(dlSize):>15}")
        del request, output, dlSize

        dlStatusList.append([sourceID,prod_title,downloadPath,str(numOfFiles),str(size),now,'True'])
        return messageList

    except URLError as e:
        messageList.append(f"{theTab}{'Failed to Download:':<35} {fileName:<60} {str(e):>15}")
        #messageList.append(f"\t{theTab}{e.__dict__}")
        failedDownloadList.append(hucURL)
        try:
            dlStatusList.append([sourceID,prod_title,downloadFolder,'0','0',now,'False'])
        except:
            pass
        return messageList

    except HTTPError as e:
        messageList.append(f"{theTab}{'Failed to Download:':<35} {fileName:<60} {str(e):>15}")
        #messageList.append(f"\t{theTab}{e.__dict__}")
        failedDownloadList.append(hucURL)
        try:
            dlStatusList.append([sourceID,prod_title,downloadFolder,'0','0',now,'False'])
        except:
            pass
        return messageList

##    except socket.timeout as e:
##        messageList.append(f"{theTab}Server Timeout: {e:<35} {fileName:<60}")
##        failedDownloadList.append(hucURL)
##        return messageList
##
##    except socket.error as e:
##        messageList.append(f"{theTab}Connectiono or Write Failure: {e:<35} {fileName:<60}")
##        failedDownloadList.append(hucURL)
##        return messageList

    except:
        messageList.append(f"{theTab}{'Unexpected Error:':<35} {fileName:<60} -- {hucURL}")
        failedDownloadList.append(hucURL)
        messageList.append(f"\t{theTab}{errorMsg(errorOption=2)}")
        try:
            dlStatusList.append([sourceID,prod_title,downloadFolder,'0','0',now,'False'])
        except:
            pass
        return messageList

## ===================================================================================
def getDownloadFolder(huc,res):
    # Create 8-digit huc Directory structure in the designated EBS mount volume
    # '07060002' --> ['07', '06', '00', '02'] --> '07\\06\\00\\02'
    # /data03/gisdata/elev/09/0904/090400/09040004/

    try:

        if len(huc) != 8:
            AddMsgAndPrint(f"\n\tHUC digits are incorrect for {huc}. Should be 8")
            return False

        region = huc[:2]
        subregion = huc[:4]
        basin = huc[:6]
        ld = huc[-1]

        if ld in ["1","9","0"]:
            root = "/data02/gisdata/elev"
        elif ld in ["2","7","8"]:
            root = "/data03/gisdata/elev"
        elif ld in ["3","6"]:
            root = "/data04/gisdata/elev"
        elif ld in ["4","5"]:
            root = "/data05/gisdata/elev"

        downloadFolder = root + os.sep + region + os.sep + subregion + os.sep + basin + os.sep + huc + os.sep + res.lower()

        #temp = r'E:\python_scripts\DSHub\LinuxWorkflow\TEMP_Update_DL_StatusFile\TEMP_testingFiles\3M'
        #downloadFolder = temp + root + os.sep + region + os.sep + subregion + os.sep + basin + os.sep + huc + os.sep + res.lower()

        if not os.path.exists(downloadFolder):
            os.makedirs(downloadFolder)

        return downloadFolder

    except:
        errorMsg()
        return False

## ===================================================================================
def unzip(local_zip,bDeleteZipFiles):
    # Given zip file name, try to unzip it

    try:
        messageList = list()

        if bDeleteZipFiles: leftAlign = 40
        else:               leftAlign = 30

        if zipfile.is_zipfile(local_zip):

            zipSize = os.stat(local_zip).st_size  # size in bytes
            zipName = local_zip.split(os.sep)[-1]
            wcName = zipName.split('.')[0]
            unzipFolder = os.path.dirname(local_zip)

            if zipSize > 0:

                try:
                    with zipfile.ZipFile(local_zip, "r") as z:
                        # a bad zip file returns exception zipfile.BadZipFile
                        z.extractall(unzipFolder)
                    del z

                    unzipTally = 0
                    for file in glob.glob(f"{unzipFolder}{os.sep}{wcName}*"):
                        if file.endswith('.zip'):continue
                        size = os.stat(file).st_size
                        unzipTally+=size
                    global totalUnzipSize
                    totalUnzipSize+=unzipTally

                    if not bDeleteZipFiles:
                        messageList.append(f"\t{'Successfully Unzipped:':<{leftAlign}} {zipName:<55} {convert_bytes(unzipTally):>15}")

                except zipfile.BadZipfile:
                    messageList.append(f"\t{'Corrupt Zipfile:':<{leftAlign}} {zipName:<55}")
                    return messageList

                except:
                    # Error unizpping file; Do not proceed with deleting zip file regardless of bDeleteZipFiles
                    messageList.append(f"\t{'Failed to unzip:':<{leftAlign}} {zipName:<55} {convert_bytes(zipSize):>15}")
                    messageList.append(f"\t\t{errorMsg(errorOption=2)}")
                    return messageList

                # remove zip file after it has been extracted,
                # allowing a little extra time for file lock to clear
                if bDeleteZipFiles:
                    try:
                        os.remove(local_zip)
                        messageList.append(f"\t{'Successfully Unzipped:':<{leftAlign}} {zipName:<55} {convert_bytes(unzipTally):>15}  --  Successfully Deleted Zip File")
                    except:
                        messageList.append(f"\t{'Successfully Unzipped:':<{leftAlign}} {zipName:<55} {convert_bytes(unzipTally):>15} --  Failed to Delete Zip File")

                return messageList

            else:
                messageList.append(f"\t{'Empty Zipfile:':<{leftAlign}} {zipName:<55} {convert_bytes(zipSize):>15}")
                #os.remove(local_zip)
                return messageList

        else:
            # Don't have a zip file, need to find out circumstances and document
            messageList.append(f"\t{'Invalid Zipfile:':<{leftAlign}} {zipName:<60}")
            return messageList

    except:
        messageList.append(f"\tError with {zipName} -- {errorMsg(errorOption=2)}")
        return messageList

## ===================================================================================
def createDLstatusFile(dlstatus,downloadFile):
# function to create download status CSV file
# [sourceID,prod_title,local_file,str(numOfFiles),str(size),now,'True']
    # 5eacf54a82cefae35a24e177,
    # USGS one meter x44y515 ME Eastern B1 2017
    # /data03/gisdata/elev/01/0101/010100/01010002/1m/USGS_one_meter_x44y515_ME_Eastern_B1_2017.tif
    # 1
    # 62690309
    # 11232022 16:19:44
    # True

    try:
        dlStatusLogFile = os.path.basename(downloadFile).split('.')[0] + "_Download_Status.txt"
        dlStatusFile = f"{os.path.dirname(downloadFile)}{os.sep}{dlStatusLogFile}"

        g = open(dlStatusFile,'a+')
        g.write(f"sourceID,prod_title,path,numofFiles,unzipSize,timestamp,downloadstatus")

        for status in dlstatus:
            g.write("\n" + ",".join(status).strip())
        g.close()
        del g

        return dlStatusFile

    except:
        errorMsg()

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
def createRaster2pgSQLFile(dlstatus):

    # Raster processing: Optional parameters used to manipulate input raster dataset
    # -s - <SRID> Assign output raster with specified SRID.
    # -b - Index (1-based) of band to extract from raster.
    # -t - Tile Size; Cut raster into tiles to be inserted one per table row.
    # -R - Register; Register the raster as a filesystem (out-db) raster.

    # Optional parameters used to manipulate database objects
    # -F - Add a column with the name of the file (necessary for elevation but not others)
    #     This will be handy when you merge tilesets together
    # -I - Create a GiST index on the raster column.
    # -R  Register the raster as a filesystem (out-db) raster.
    #     Only the metadata of the raster and path location to the raster is
    #     stored in the database (not the pixels).

    # STDOUT parameters
    # | - A pipe is a form of redirection in Linux used to connect the STDOUT
    #     of one command into the STDIN of a second command.
    # PGPASSWORD = itsnotflat
    # -U elevation - user in the schemas
    # -d elevation - database name
    # -h 10.11.11.10 host - localhost
    # -p port

    try:
        from osgeo import gdal

        # raster2pgsql -s 4269 -b 1 -t 507x507 -R -F -I /data03/gisdata/dsmgroup/aspct_16.tif elevation.$name | PGPASSWORD=itsnotflat psql -U elevation -d elevation -h 10.11.11.10 -p 5432

        # raster2pgsql -s 5070 -b 1 -I -t 560x560 -F -R /data03/gisdata/dsmgroup/aspct_16.tif
        # covariate.conus_aspect_16 | PGPASSWORD=itsnotflat psql -U covariate -d elevation -h 10.11.11.10 -p 5432

        fileName = os.path.basename(downloadFile).split('.')[0] + "_raster2pgsql.txt"
        raster2pgsqlFile = f"{os.path.dirname(downloadFile)}{os.sep}{fileName}"

        g = open(raster2pgsqlFile,'a+')

        for rec in dlstatus:
            filePath = rec[3]


    except:
        errorMsg()

## ===================================================================================
def createErrorLogFile(downloadFile,failedDownloadList,headerItems):

    try:

        errorFileName = os.path.basename(downloadFile).split('.')[0] + "_Download_FAILED.txt"
        errorFile = f"{os.path.dirname(downloadFile)}{os.sep}{errorFileName}"

        AddMsgAndPrint(f"\tDownload Errors Logged to: {errorFile}")
        g = open(errorFile,'a+')

        lineNum = 0
        numOfErrors = 0
        with open(downloadFile, 'r') as fp:
            for line in fp:
                items = line.split(',')

                if bHeader and lineNum == 0:
                    g.write(line.strip())
                    lineNum +=1

                huc8digit = items[headerItems["huc8_digit"]]
                downloadURL = items[headerItems["download_url"]].strip()

                if downloadURL in failedDownloadList:
                    g.write("\n" + line.strip())
                    numOfErrors+=1

        g.close()

        # Not sure why
        if len(failedDownloadList) != numOfErrors:
            AddMsgAndPrint(f"\t\tNumber of errors logged don't coincide--????")

        return errorFile

    except:
        errorMsg()

## ====================================== Main Body ==================================
def main(dlFile,bHead,bdlmt,bReplace,bUnzip,bDltZips):
    try:

        startTime = tic()
        global bHeader
        global bReplaceData
        global downloadFile

        # 6 Tool Parameters
        downloadFile = dlFile
        bHeader = bHead
        bDownloadMultithread = bdlmt
        bReplaceData = bReplace
        bUnzipFiles = bUnzip
        bDeleteZipFiles = bDltZips

        # Pull elevation resolution from file name
        # USGS_3DEP_3M_Metadata_Elevation_11102022.txt
        resolution = downloadFile.split(os.sep)[-1].split('_')[2]

        headerItems = {
            "huc8_digit":0,
            "prod_title":1,
            "pub_date":2,
            "last_updated":3,
            "size":4,
            "format":5,
            "sourceID":6,
            "metadata_url":7,
            "download_url":8}

        urlDownloadDict = dict()
        recCount = 0
        badLines = 0

        """ ---------------------------- Open Download File and Parse Information ----------------------------------"""
        with open(downloadFile, 'r') as fp:
            for line in fp:
                items = line.split(',')

                # Skip header line and empty lines
                if bHeader and recCount == 0 or line == "\n":
                    recCount+=1
                    continue

                # Skip if number of items are incorrect
                if len(items) != len(headerItems):
                    badLines+=1
                    continue

                huc8digit = items[headerItems["huc8_digit"]]
                downloadURL = items[headerItems["download_url"]].strip()
                sourceID = items[headerItems["sourceID"]]
                prod_title = items[headerItems["prod_title"]]
                fileFormat = items[headerItems["format"]]

                if huc8digit in urlDownloadDict:
                    urlDownloadDict[huc8digit].append([downloadURL,sourceID,prod_title,fileFormat])
                else:
                    urlDownloadDict[huc8digit] = [[downloadURL,sourceID,prod_title,fileFormat]]

                recCount+=1

        # subtract header for accurate record count
        if bHeader: recCount = recCount -1

        """ ---------------------------- Establish Console LOG FILE ---------------------------------------------------"""
        today = datetime.today().strftime('%m%d%Y')

        # Log file that captures console messages
        logFile = os.path.basename(downloadFile).split('.')[0] + "_Download_ConsoleMsgs.txt"
        global msgLogFile
        msgLogFile = f"{os.path.dirname(dlFile)}{os.sep}{logFile}"
        h = open(msgLogFile,'a+')
        h.write(f"Executing: Linux_Download_USGSElevation_by_MetadataFile {today}\n\n")
        h.write(f"User Selected Parameters:\n")
        h.write(f"\tDownload File: {downloadFile}\n")
        h.write(f"\tFile has header: {bHeader}\n")
        h.write(f"\tDownload Multithread: {bDownloadMultithread}\n")
        h.write(f"\tReplace Data: {bReplaceData}\n")
        h.write(f"\tUnzip Files: {bUnzipFiles}\n")
        h.write(f"\tDelete Zip Files: {bDeleteZipFiles}\n")
        h.write(f"\tLog File Path: {logFile}\n")
        h.close()

        AddMsgAndPrint(f"\n{'='*125}")
        AddMsgAndPrint((f"Total Number of files to download: {recCount}"))

        """ ----------------------------- DOWNLOAD ELEVATION DATA ----------------------------- """
        global dlStatusList
        global failedDownloadList
        global totalDownloadSize

        dlStatusList = list()
        failedDownloadList = list()
        zipFolders = list() # list of folders that possibly contain zip files
        totalDownloadSize = 0

        if len(urlDownloadDict) > 0:

            dlStart = tic()
            dlTracker = 0

            if bDownloadMultithread: AddMsgAndPrint(f"\nDownloading in Multi-threading Mode - # of Files: {recCount:,}")
            else:                    AddMsgAndPrint(f"\nDownloading in Single Request Mode - # of Files: {recCount:,}")

            for huc,items in urlDownloadDict.items():
                i = 1
                numOfHUCelevTiles = len(items)

                #downloadFolder = getDownloadFolder(huc,resolution)
                #if not downloadFolder: continue
                downloadFolder = r'E:\python_scripts\DSHub\LinuxWorkflow\TEMP_Update_DL_StatusFile\TEMP_testingFiles\3M'
                AddMsgAndPrint(f"\n\tDownloading {numOfHUCelevTiles} elevation tiles for HUC: {huc} ---> {downloadFolder}")

                if bUnzipFiles:
                    if not downloadFolder in zipFolders:
                        zipFolders.append(downloadFolder)

                if bDownloadMultithread:
                    with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:

                        # use a set comprehension to start all tasks.  This creates a future object
                        future_to_url = {executor.submit(DownloadElevationTile, item, downloadFolder): item for item in items}

                        # yield future objects as they are done.
                        for future in as_completed(future_to_url):
                            dlTracker+=1
                            j=1
                            for printMessage in future.result():
                                if j==1:
                                    AddMsgAndPrint(f"{printMessage} -- ({dlTracker} of {recCount:,})")
                                else:
                                    AddMsgAndPrint(printMessage)
                                j+=1

                else:
                    for item in items:
                        #AddMsgAndPrint(f"\t\tDownloading elevation tile: {url.split('/')[-1]} -- {i} of {numOfHUCelevTiles}")
                        downloadMsgs = DownloadElevationTile(item, downloadFolder)

                        for msg in downloadMsgs:
                            AddMsgAndPrint(msg)
                        i+=1
            dlStop = toc(dlStart)

            """ ----------------------------- UNZIP ELEVATION DATA ----------------------------- """
            if bUnzipFiles:
                global totalUnzipSize
                global zipFileList

                unzipStart = tic()
                zipFileList = list()
                totalUnzipSize = 0
                unzipTracker = 0

                if len(zipFolders):
                    for folder in zipFolders:
                        zipFileList += glob.glob(f"{folder}{os.sep}*.zip")

                    if len(zipFileList) > 0:
                        AddMsgAndPrint(f"\nUnzipping {len(zipFileList)} Elevation Files")

                        with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
                            unZipResults = {executor.submit(unzip, zipFile, bDeleteZipFiles): zipFile for zipFile in zipFileList}

                            # yield future objects as they are done.
                            for msg in as_completed(unZipResults):
                                unzipTracker+=1
                                j=1
                                for printMessage in msg.result():
                                    if j==1:
                                        AddMsgAndPrint(f"{printMessage} -- ({unzipTracker:,} of {len(zipFileList):,})")
                                    else:
                                        AddMsgAndPrint(printMessage)
                                    j+=1

                    else:
                        AddMsgAndPrint(f"\nThere are no files to uzip")
                unzipStop = toc(unzipStart)

        else:
            print("\nThere are no elevation tiles to download")

        """ ------------------------------------ SUMMARY -------------------------------------- """
        AddMsgAndPrint(f"\n{'-'*40}SUMMARY{'-'*40}")
        AddMsgAndPrint(f"\nTotal Download Time: {dlStop}")

        if totalDownloadSize > 0:
            AddMsgAndPrint(f"\nTotal Download Size: {convert_bytes(totalDownloadSize)}")

        # Create Download Status File
        if len(dlStatusList):
            dlStatusFile = createDLstatusFile(dlStatusList,downloadFile)
            AddMsgAndPrint(f"\nDownload Status File: {dlStatusFile}")

            # Create raster2pgsql file
            #raster2pgSQLFile = createRaster2pgSQLFile(dlStatusList)
            #AddMsgAndPrint(f"\nDownload Status File: {dlStatusFile}")

        # Create Error
        if len(failedDownloadList):
            AddMsgAndPrint(f"\nFailed to Download {len(failedDownloadList)} elevation files:")
            errorlogFile = createErrorLogFile(downloadFile,failedDownloadList, headerItems)

        if bUnzipFiles:
            if len(zipFileList) > 0:
                AddMsgAndPrint(f"\nTotal Time to unzip data: {unzipStop}")

                try:
                    #totalUnzipSize = sum([os.stat(x).st_size for x in glob.glob(f"{downloadFolder}\\*") if not x.endswith(".zip")])
                    AddMsgAndPrint(f"\tNumber of files to unzip: {len(zipFileList)}")
                    AddMsgAndPrint(f"\tTotal Unzipped Size: {convert_bytes(totalUnzipSize)}")
                except:
                    pass

        AddMsgAndPrint(f"\nTotal Processing Time: {toc(startTime)}")
        AddMsgAndPrint(f"\nAll console messages were logged to: {msgLogFile}")

    except:
        AddMsgAndPrint(errorMsg(errorOption=2))

## ============================================================================================================
if __name__ == '__main__':

##    # DOWNLOAD FILE
##    dlFile =     input("\nEnter full path to USGS Metadata Download Text File: ")
##    while not os.path.exists(dlFile):
##        print(f"{dlFile} does NOT exist. Try Again")
##        dlFile = input("Enter full path to USGS Metadata Download Text File: ")
##
####    # ROOT DIRECTORY
####    rootFolder = input("\nEnter path to the root directory where elevation data will be downloaded to: ")
####    while not os.path.isdir(rootFolder):
####        print(f"{rootFolder} is not a valid directory path")
####        rootFolder = input("Enter path to the root directory where elevation data will be downloaded to: ")
##
##    # CONTAINS HEADER
##    bHead =    input("\nDoes the Metadata text file contain a header? (Yes/No): ")
##    while not bHead.lower() in ("yes","no","y","n"):
##        print(f"Please Enter Yes or No")
##        bHead =    input("Does the Metadata text file contain a header? (Yes/No): ")
##
##    if bHead.lower() in ("yes","y"):
##        bHead = True
##    else:
##        bHead = False
##
##    # MULTI-THREADING MODE
##    bdlmt =      input("\nDo you want to execute the script in multi-threading mode? (Yes/No): ")
##    while not bdlmt.lower() in ("yes","no","y","n"):
##        print(f"Please Enter Yes or No")
##        bdlmt =    input("Do you want to execute the script in multi-threading mode? (Yes/No): ")
##
##    if bdlmt.lower() in ("yes","y"):
##        bdlmt = True
##    else:
##        bdlmt = False
##
##    # REPLACE DATA
##    bReplace =   input("\nDo you want to replace existing data? (Yes/No): ")
##    while not bReplace.lower() in ("yes","no","y","n"):
##        print(f"Please Enter Yes or No")
##        bReplace =    input("Do you want to replace existing data? (Yes/No): ")
##
##    if bReplace.lower() in ("yes","y"):
##        bReplace = True
##    else:
##        bReplace = False
##
##    # UNZIP DATA
##    bUnzip =     input("\nDo you want to unzip any downloaded zip files? (Yes/No): ")
##    while not bUnzip.lower() in ("yes","no","y","n"):
##        print(f"Please Enter Yes or No")
##        bUnzip = input("Do you want to unzip any downloaded zip files? (Yes/No): ")
##
##    if bUnzip.lower() in ("yes","y"):
##        bUnzip = True
##    else:
##        bUnzip = False
##
##    # DELETE ZIP FILES
##    if bUnzip:
##        bDltZips =   input("\nDo you want zip files to be deleted after being unzipped? (Yes/No): ")
##        while not bDltZips.lower() in ("yes","no","y","n"):
##            print(f"Please Enter Yes or No")
##            bDltZips = input("Do you want any zip files to be unzipped? (Yes/No): ")
##
##        if bDltZips.lower() in ("yes","y"):
##            bDltZips = True
##        else:
##            bDltZips = False
##    else:
##        bDltZips = False

    dlFile = r'E:\python_scripts\DSHub\LinuxWorkflow\TEMP_Update_DL_StatusFile\TEMP_testingFiles\USGS_3DEP_3M_Metadata_Elevation_12132022.txt'
    bHead = True
    bdlmt = True
    bReplace = True
    bUnzip = True
    bDltZips = False

    main(dlFile,bHead,bdlmt,bReplace,bUnzip,bDltZips)
    input("\nHit Enter to Continue: ")
    sys.exit()