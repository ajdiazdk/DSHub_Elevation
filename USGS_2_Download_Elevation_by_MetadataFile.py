# -*- coding: utf-8 -*-
"""
Script Name: USGS_Download_Elevation_by_MetadataFile.py
Created on Fri Sep  2 10:12:13 2022
updated 4/5/2023

@author: Adolfo.Diaz
GIS Business Analyst
USDA - NRCS - SPSD - Soil Services and Information
email address: adolfo.diaz@usda.gov
cell: 608.215.7291

This is script #2 in the USGS Elevation acquisition workflow developed for the DS Hub.

The purpose of this script is to download elevation data published by USGS.  The script
takes in a text file (i.e. USGS_3DEP_3M_Metadata_Elevation_12132022.txt) produced by
the 1st script (Create_USGS_API_URL_Reports_multiThreading.py) in the USGS workflow
that contains the download url for the elevation files.

Parameters:
    1) dlFile - path to text file that contains elevation download information
    4) bReplaceData - boolean indicator to replace download file
    5) bDeleteZipFiles - boolean to delete or leave downloaded zipped files (only relevant if dl files are zip files)
                         set to False by default but can be overwritten in main.

Sequence of workflow:
    1) import dlFile information to a dictionary (elevMetadataDict)
    2) isolate download url and sourceID by HUC (urlDownloadDict)
    3) Establish log file (USGS_3DEP_3M_Metadata_Elevation_12132022_Download_ConsoleMsgs.txt)
    4) Determine download folder (data02, data03,data04 or data05)
    4) Download elevation files
        a) determine if local version of dl file exists; honor bReplaceData boolean
        b) download data to appropriate drive
        c) track zip files separate from single files (unzip) in 2 different dicts
    5) Unzip Data - If any zips were donwloaded
        a) unzip files logged in dlZipFileDict
        b) collect size of unzipped files to update total size in the summary portion
        c) Delete zip files if bDeleteZipFiles is True
        d) Add sourceID and DEMfilePath to dlImgFileDict
    6) Create Master Elevation File
    7) Create RASTER2PGSQL File
    8) Provide Summary
        a) Total Download time
        b) # of downloaded files
        c) # of files that failed to download
        d) # of files that were unzipped (if any)
        e) Total processing time

How to QA the outputs.  Look for the following:
    - 'File will be added' - These are only for zip files and not .img or tifs.  If file is a legit
       DEM than look into this.
    - '0 bytes' - empty DEMs.  Perhaps script crashed somewhere leaving ghost files.
    - '#' look for this in the master DB file and the raster2pgsql

---------------- UPDATES
12/01/2022
Added CreateRaster2pgSQLFile function that will create text file containing the linux command
for each tile to be registered.  NOT COMPLETE.  Needed to grab raster information first.

12/09/2022
Created getRasterInformation function that will take in a raster and describe it's properties
and return them.  This will be used to append to the DLStatus file.

1/10/2023
  - Rewrote the download status file so that it looks like the input download file with additional raster information.
    This file can then be registered in the database and will contain all relevant elevation and raster information.
    Additional information includes:
      columns,rows,bandCount,cellSize,rdsFormat,bitDepth,noDataVal,srid,srsName,top,left,right,bottom,minStat,
        meanStat,maxStat,stDevStat,blockXsize,blockYsize
  - Renamed this file to: Master_DB.txt.  Generated after the unzipping happens for better effeciency.
  - Remove bUnzipFiles boolean in main function so that unzipping happens by default.  This can be overriden
    in the main function by the user.

2/8/2023
  - update getDownloadFolder to discern between huc2,4 and 8.

2/24/2023
    -update createRaster2pgSQLFile to print invalid raster2pgsql commands.  It is important
     That the user fixes them here before proceeeding to the next step#3.

2/27/2023
    - Updated headerItems (dictionary of headers with positions) to headerValues (list created by
      opening up the download file and automatically grabbing the header values).  This is a better method
      moving forward in case header items are updated.
    - Bug with unzip function.  zipName was established within the first if statement when it should've been
      established before in case an error with zipName is encountered.
    - Added additional test case scenario in createMasterDBFile when srcID is not in dlImgFileDict.  check
      failedDownloadList to see if URL is in there, if so simply pass.  If not, resulting sourceID should be
      inspected.

2/28/2023
    - Update DownloadElevationTile to check for zero-sized files.  This potentially happens when a download process
      is terminaged and leaves an empty file behind.  When rerunning this process, the file technically exists.
      If so, delete it and redownload it.
    - Added a check for establishing download folder depending on operating system.  NT vs. POSIX
    - Incorporated multi-threading capability in createMasterDBFile for gathering raster information.  The
      problem is that statistics need to be calculated for DEM files and this takes approximately 10 secs
      per file.
    - Added code to determine operating system for get download folder.  This was mainly for testing purpose.
      I didn't want to change it temporarily and forget to change it back.

4/14/2023
    - If script is being called from Linux than downloadfolder is not set; If script is called from windows
      then downloadfolder is set by user.
    - Updated the names of the output files to be more intuitive.
        - USGS_3DEP_1M_Metadata_Elevation_02242023_MASTER_DB.txt -- > USGS_3DEP_1M_Step2_Elevation Metadata.txt
        - USGS_3DEP_1M_Metadata_Elevation_02242023_Download_ConsoleMsgs.txt --> USGS_3DEP_1M_Step2_Download_ConsoleMsgs.txt
        - USGS_3DEP_1M_Metadata_Elevation_02242023_RASTER2PGSQL.txt --> USGS_3DEP_1M_Step2_RASTER2PGSQL.txt

Things to consider/do:
  - rename key sql reserved words:
        - top, bottom, left, right --> rast_top,rast_bottom,rast_left,rast_right
        - size --> rast_size
        - columns,rows --> rast_columns,rast_rows
  - Explore option of converting '#' to 'None' in getRasterInformation function.  This will be more consistent in
    postgres for integer and varchar fields.
  - Add code to change group in POSIX to 'gis' so that .xml containing DEM statistics can be updated correctly.
  - Rewrite the createErrorLogFile function to utilize the elevMetadataDict instead of the original dl file.
    In order to do this, the failedDownloadList will need to be converted to a dictionary and the sourceID used
    as a key; sourceID:dlURL
  - possibly add the sum of the sizes for all files within individual zip files.  Within the unzip function, write
    the unzipTally to a dictionary using sourceID:unzipTally.  This sum can be appended at the very end of the master_db
    file with the header 'unzipSize'.  This will only be added for zipped files.
  - Need to account for files associated with deprecated zip files.  i.e. if x.zip is no longer part of watershed A,
    files part of x.zip should be deleted.  Partial files can probably be deleted by associating file names with "x"
    but it becomes difficult if x.zip is deleted.
  - Need to handle bad zip files.
  - Need to handle replacing data.  Similar to first issue.
  - Modify createMasterDBfile to start the iteration process from the dlImgFileDict instead of the elevMetadataDict.
    This will be a much cleaner approach by only iterating through the downloaded files vs all of the files in the
    original download file (elevMetadataDict). EXAMPLE -- You rerun the Script#1 on USGS 1M data and notice that there are
    an additional 4,000 new files.  You can use this script to download the 4,000 additional files with a bReplace parameter
    set to 'No' however, during the process of creating the master elevation file, you will have to iterate through all 65,000
    files.
  - Add boolean for rewriting raster2pgsql file for all DEM files or only those that are downloaded.
  - In DownloadElevationTile: if zip file already exists then it will get unzipped again.  Check if contents have
    been unzipped before adding it to the dlZipFileDict dictionary.  This should maybe go in the unzip function.

"""

## ========================================== Import modules ===============================================================
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

    """ By default, error messages will be printed and logged immediately.
        If errorOption is set to 2, return the message back to the function that it
        was called from.  This is used in DownloadElevationTile function or in functions
        that use multi-threading functionality"""

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
        global dlZipFileDict     # dict containing zip files that will be unzipped; sourceID:filepath
        global dlImgFileDict     # dict containing DEM files; sourceID:filepath
        global totalDownloadSize

        dlURL = itemCollection[0]
        sourceID = itemCollection[1]

        theTab = "\t\t"

        # Filename could be a zipfile or DEM file (.TIF, .IMG)
        # 'https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n30x50_w091x75_la_atchafalayabasin_2010.zip'
        fileName = dlURL.split('/')[-1]

        # path to where file will be downloaded to
        local_file = f"{downloadFolder}{os.sep}{fileName}"

        now = datetime.today().strftime('%m%d%Y %H:%M:%S')

        # if download file already exists, delete it if bReplaceData is True;
        # otherwise no data will be collected for this elevation file b/c it already exists.
        # File will be excluded from master elevation file.
        if os.path.isfile(local_file):
            if bReplaceData:
                try:
                    os.remove(local_file)
                    #messageList.append(f"{theTab}{'File Exists; Deleted':<35} {fileName:<60}")
                except:
                    messageList.append(f"{theTab:q!}{'File Exists; Failed to Delete':<35} {fileName:<60}")
                    failedDownloadList.append(dlURL)
                    return messageList
            else:
                #messageList.append(f"{theTab}{'File Exists, Skipping:':<35} {fileName:<60} {convert_bytes(os.stat(local_file).st_size):>15}")

                # ---------------------------------These 8 lines were added temporary to remove duplicates from 1M data
                dlSize = os.stat(local_file).st_size

                if dlSize > 0:
                    totalDownloadSize+=dlSize
                    # Add downloaded file to appropriate dictionary; sourceID:local_file
                    # if file is a zip file it will be unzipped again and it's unzipped content overwritten.
                    if zipfile.is_zipfile(local_file):
                        dlZipFileDict[sourceID] = local_file
                        messageList.append(f"{theTab}{'File will be added to Raster2pgsql file:':<40} {fileName:<60} {convert_bytes(dlSize):>20}")
                    else:
                        dlImgFileDict[sourceID] = local_file
                        messageList.append(f"{theTab}{'DEM will be added to Raster2pgsql file:':<40} {fileName:<60} {convert_bytes(dlSize):>20}")
                    return messageList

                else:
                    try:
                        os.remove(local_file)
                        messageList.append(f"{theTab}{'File was 0 bytes; Deleted':<35} {fileName:<60}")
                    except:
                        messageList.append(f"{theTab:q!}{'File was 0 bytes; Failed to Delete':<35} {fileName:<60}")
                        failedDownloadList.append(dlURL)
                        return messageList

        # Download elevation zip file
        request = urlopen(dlURL)

        # save the download file to the specified folder
        output = open(local_file, "wb")
        output.write(request.read())
        output.close()

        # Log the size of the file downloaded; could be zip or individual file
        dlSize = os.stat(local_file).st_size
        totalDownloadSize+=dlSize

        # Add downloaded file to appropriate dictionary; sourceID:local_file
        if zipfile.is_zipfile(local_file):
            dlZipFileDict[sourceID] = local_file
        else:
            dlImgFileDict[sourceID] = local_file

        messageList.append(f"{theTab}{'Successfully Downloaded:':<35} {fileName:<60} {convert_bytes(dlSize):>15}")
        del request, output, dlSize

        return messageList

    except URLError as e:
        messageList.append(f"{theTab}{'Failed to Download:':<35} {fileName:<60} {str(e):>15}")
        #messageList.append(f"\t{theTab}{e.__dict__}")
        failedDownloadList.append(dlURL)
        return messageList

    except HTTPError as e:
        messageList.append(f"{theTab}{'Failed to Download:':<35} {fileName:<60} {str(e):>15}")
        #messageList.append(f"\t{theTab}{e.__dict__}")
        failedDownloadList.append(dlURL)
        return messageList

    except:
        messageList.append(f"{theTab}{'Unexpected Error:':<35} {fileName:<60} -- {dlURL}")
        failedDownloadList.append(dlURL)
        messageList.append(f"\t{theTab}{errorMsg(errorOption=2)}")
        return messageList

## ===================================================================================
def getDownloadFolder(huc,res):
    # Create 8-digit huc Directory structure in the designated EBS mount volume
    # '07060002' --> ['07', '06', '00', '02'] --> '07\\06\\00\\02'
    # /data03/gisdata/elev/09/0904/090400/09040004/

    try:

        hucLength = len(huc)
        if not hucLength in (2,4,8):
            AddMsgAndPrint(f"\n\tHUC digits are incorrect for {huc}. Should be 2,4 or 8")
            return False

        if hucLength == 4:
            region = huc[:2]
        elif hucLength == 8:
            region = huc[:2]
            subregion = huc[:4]
            basin = huc[:6]
        else:
            pass

        # last digit
        ld = huc[-1]

        if ld in ["1","9","0"]:
            root = "/data02/gisdata/elev"
        elif ld in ["2","7","8"]:
            root = "/data03/gisdata/elev"
        elif ld in ["3","6"]:
            root = "/data04/gisdata/elev"
        elif ld in ["4","5"]:
            root = "/data05/gisdata/elev"

        if hucLength == 2:
            downloadFolder = root + os.sep + huc + os.sep + res.lower()
        elif hucLength == 4:
            downloadFolder = root + os.sep + region + os.sep + huc + os.sep + res.lower()
        else:
            downloadFolder = root + os.sep + region + os.sep + subregion + os.sep + basin + os.sep + huc + os.sep + res.lower()

        if not os.path.exists(downloadFolder):
            os.makedirs(downloadFolder)

        return downloadFolder

    except:
        errorMsg()
        return False

## ===================================================================================
def unzip(itemCollection,bDeleteZipFiles):
    # Unzip files
    # itemCollection = ('581d2d68e4b08da350d665a5',r'E:\python_scripts\DSHub\LinuxWorkflow\TEMP_testingFiles\ned19_n42x75_w091x00_ia_northeast_2007.zip')

    try:
        messageList = list()
        sourceID = itemCollection[0]
        local_zip = itemCollection[1]
        zipName = local_zip.split(os.sep)[-1]

        global totalUnzipSize
        global elevMetadataDict
        global headerValues
        global dlImgFileDict

        if bDeleteZipFiles: leftAlign = 40
        else:               leftAlign = 30

        # File is valid zip file
        if zipfile.is_zipfile(local_zip):

            zipFile = zipfile.ZipFile(local_zip)
            zipFileList = zipFile.filelist           # list of files within the zipfile
            zipSize = os.stat(local_zip).st_size     # size in bytes
            unzipFolder = os.path.dirname(local_zip)

            # Isolate DEM file based on the format
            demFilePath = ""

            # Lookup the file format from the elevMetadataDict
            fileFormat = elevMetadataDict[sourceID][headerValues.index("format")].lower()

            if fileFormat == 'geotiff':
                fileType = 'tif'        # 1M DEMs
            else:
                fileType = fileFormat   # 3M DEMs (img)

            # Zip file is not empty
            if zipSize > 0:

                try:
                    # Unzip the zip file
                    with zipfile.ZipFile(local_zip, "r") as z:
                        # a bad zip file returns exception zipfile.BadZipFile
                        z.extractall(unzipFolder)
                    del z

                except zipfile.BadZipfile:
                    messageList.append(f"\t{'Corrupt Zipfile:':<{leftAlign}} {zipName:<55}")
                    return messageList

                except zipfile.LargeZipFile:
                    messageList.append(f"\t{'Large Zipfile; May require ZIP64 Functionality:':<{leftAlign}} {zipName:<45}")
                    return messageList

                except:
                    # Error unizpping file; Might be caused by permission issue related to the simultaneous
                    # unzipping of files; typically a pdf file. Proceed only if it is permission denied related to pdf
                    messageList.append(f"\t{'Failed to unzip:':<{leftAlign}} {zipName:<55} {convert_bytes(zipSize):>15}")
                    errorMessage = errorMsg(errorOption=2)

                    if not errorMessage.find('.pdf'):

                        print("No pdf error detected.")
                        messageList.append(f"\t\t{errorMessage}")
                        return messageList

                # tally size of all recently unzipped files.
                # capture path of DEM file in dlImgFileDict
                unzipTally = 0
                for zinfo in zipFileList:
                    unzippedFilePath = f"{unzipFolder}{os.sep}{zinfo.filename}"

                    if os.path.exists(unzippedFilePath):
                        size = os.stat(unzippedFilePath).st_size
                        unzipTally+=size
                    else:
                        messageList.append(f"\t\t{zinfo.filename} wasn't properly unzipped...bizarre")

                    # DEM file
                    if unzippedFilePath.endswith(fileType):
                        demFilePath = unzippedFilePath
                        dlImgFileDict[sourceID] = unzippedFilePath

                totalUnzipSize+=unzipTally

                # remove zip file after it has been extracted,
                if bDeleteZipFiles:
                    try:
                        os.remove(local_zip)
                        messageList.append(f"\t{'Successfully Unzipped:':<{leftAlign}} {zipName:<55} {convert_bytes(unzipTally):>15}  --  Successfully Deleted Zip File")
                    except:
                        messageList.append(f"\t{'Successfully Unzipped:':<{leftAlign}} {zipName:<55} {convert_bytes(unzipTally):>15} --  Failed to Delete Zip File")
                else:
                    messageList.append(f"\t{'Successfully Unzipped:':<{leftAlign}} {zipName:<55} {convert_bytes(unzipTally):>15}")

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
def print_progress_bar(index, total, label):

    "prints generic percent bar to indicate progress. Cheesy but works."

    n_bar = 50  # Progress bar width
    progress = index / total
    sys.stdout.write('\r')
    sys.stdout.write(f"\t[{'=' * int(n_bar * progress):{n_bar}s}] {int(100 * progress)}%  {label}")
    sys.stdout.flush()

## ===================================================================================
def createMasterDBfile_MT(dlImgFileDict,elevMetadataDict):

    # dlImgFileDict: sourceID = rasterPath

    try:
        demStatDict = dict()
        goodStats = 0
        badStats = 0

        # Step #1 Gather Statistic Information for all rasters
        AddMsgAndPrint(f"\n\tGathering Individual DEM Statistical Information")
        with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:

            # use a set comprehension to start all tasks.  This creates a future object
            rasterStatInfo = {executor.submit(getRasterInformation_MT, rastItem): rastItem for rastItem in dlImgFileDict.items()}

            # yield future objects as they are done.
            for stats in as_completed(rasterStatInfo):
                resultDict = stats.result()
                for results in resultDict.items():
                    ID = results[0]
                    rastInfo = results[1]

                    if rastInfo.find('#')>-1:
                        badStats+=1
                    else:
                        goodStats+=1
                    demStatDict[ID] = rastInfo

        AddMsgAndPrint(f"\t\t\tSuccessfully Gathered stats for {goodStats:,} DEMs")
        AddMsgAndPrint(f"\t\t\tProblems with Gathering stats for {badStats:,} DEMs")
        global downloadFile

        # Create Master Elevation File
        dlMasterFilePath = f"{os.path.dirname(downloadFile)}{os.sep}USGS_3DEP_{resolution}_Step2_Elevation_Metadata.txt"

        g = open(dlMasterFilePath,'a+')
##        header = ('huc_digit,prod_title,pub_date,last_updated,size,format,sourceID,metadata_url,'
##                  'download_url,DEMname,DEMpath,columns,rows,bandCount,cellSize,rdsFormat,bitDepth,noDataVal,srType,'
##                  'EPSG,srsName,top,left,right,bottom,minStat,meanStat,maxStat,stDevStat,blockXsize,blockYsize')

        # rast_size, rast_columns, rast_rows, rast_top, rast_left, rast_right, rast_bottom
        header = ('huc_digit,prod_title,pub_date,last_updated,rast_size,format,sourceID,metadata_url,'
                  'download_url,DEMname,DEMpath,rast_columns,rast_rows,bandCount,cellSize,rdsFormat,bitDepth,noDataVal,srType,'
                  'EPSG,srsName,rast_top,rast_left,rast_right,rast_bottom,minStat,meanStat,maxStat,stDevStat,blockXsize,blockYsize')

        g.write(header)

        total = len(elevMetadataDict)
        index = 1

        # Iterate through all of the sourceID files in the download file (elevMetadatDict)
        for srcID,demInfo in elevMetadataDict.items():

            # 9 item INFO: huc_digit,prod_title,pub_date,last_updated,size,format,sourceID,metadata_url,download_url
            firstPart = ','.join(str(e) for e in demInfo)

            # srcID must exist in dlImgFileDict (successully populated during download)
            if srcID in dlImgFileDict:
                demFilePath = dlImgFileDict[srcID]
                demFileName = os.path.basename(demFilePath)
                secondPart = demStatDict[srcID]

                g.write(f"\n{firstPart},{demFileName},{os.path.dirname(demFilePath)},{secondPart}")

            # srcID failed during the download process.  Pass since it will be accounted for in error file
            elif demInfo[headerValues.index("download_url")] in failedDownloadList:
                continue

            else:
                AddMsgAndPrint(f"\n\t\tSourceID: {srcID} NO .TIF OR .IMG FOUND -- Inspect this process")

        g.close()
        return dlMasterFilePath

    except:
        errorMsg()
        return False

## ===================================================================================
def createMasterDBfile(dlImgFileDict,elevMetadataDict):
    """ This function creates the Master Database CSV file for the DEM files
        that were successfully downloaded.  Successfully downloaded DEM files
        are logged to the dlImgFileDict dictionary, which is passed to this
        function.

        The Master Database CSV file is created in the same directory as the input
        download file.  The original information from the download file is automatically
        appended to the Master Database CSV file. 20 additional raster information items
        are also added to the master CSV file.  These items are collected within the
        getRasterInformation function.

        When completed, this function will retrun the file path of the master CSV file.
    """
    try:
        global downloadFile

        dlMasterFilePath = f"{os.path.dirname(downloadFile)}{os.sep}USGS_3DEP_{resolution}_Step2_Elevation_Metadata.txt"

        g = open(dlMasterFilePath,'a+')
##        header = ('huc_digit,prod_title,pub_date,last_updated,size,format,sourceID,metadata_url,'
##                  'download_url,DEMname,DEMpath,columns,rows,bandCount,cellSize,rdsFormat,bitDepth,noDataVal,srType,'
##                  'EPSG,srsName,top,left,right,bottom,minStat,meanStat,maxStat,stDevStat,blockXsize,blockYsize')

        # rast_size, rast_columns, rast_rows, rast_top, rast_left, rast_right, rast_bottom
        header = ('huc_digit,prod_title,pub_date,last_updated,rast_size,format,sourceID,metadata_url,'
                  'download_url,DEMname,DEMpath,rast_columns,rast_rows,bandCount,cellSize,rdsFormat,bitDepth,noDataVal,srType,'
                  'EPSG,srsName,rast_top,rast_left,rast_right,rast_bottom,minStat,meanStat,maxStat,stDevStat,blockXsize,blockYsize')

        g.write(header)

        total = len(elevMetadataDict)
        index = 1
        label = "Gathering Raster Metadata Information"

        # Iterate through all of the sourceID files in the download file (elevMetadatDict)
        for srcID,demInfo in elevMetadataDict.items():

            # huc_digit,prod_title,pub_date,last_updated,rast_size,format,sourceID,metadata_url,download_url
            firstPart = ','.join(str(e) for e in demInfo)

            # srcID must exist in dlImgFileDict (successully populated during download)
            if srcID in dlImgFileDict:
                demFilePath = dlImgFileDict[srcID]
                demFileName = os.path.basename(demFilePath)
                secondPart = getRasterInformation(demFilePath)

                if secondPart:
                    g.write(f"\n{firstPart},{demFileName},{os.path.dirname(demFilePath)},{secondPart}")
                else:
                    # Raster information will have a #; need to revisit this error.
                    AddMsgAndPrint(f"\tError in getting raster information for sourceID: {srcID}")
                    g.write(f"\n{firstPart},{demFileName},{os.path.dirname(demFilePath)},{','.join('#'*20)}")

            # srcID failed during the download process.  Pass since it will be accounted for in error file
            elif demInfo[headerValues.index("download_url")] in failedDownloadList:
                continue

            else:
                AddMsgAndPrint(f"SourceID: {srcID} NO .TIF OR .IMG FOUND -- Inspect this process")

            print_progress_bar(index, total, label)
            index+=1

        g.close()
        del g

        print("\n")
        return dlMasterFilePath

    except:
        errorMsg()

#### ===================================================================================
def getRasterInformation_MT(rasterItem):

    # Raster information will be added to dlStatusList
    # sourceID,prod_title,path,numofFiles,unzipSize,timestamp,downloadstatus --
    # add additional data:
    # columns,rows,cellsize,bandcount,bitDepth,nodatavalue,

    # query srid
    # describe num of bands; what if it is more than 1

    # Input example
    # rasterItem = (sourceID, raster path)
    # ('60d2c0ddd34e840986528ae4', 'E:\\DSHub\\Elevation\\1M\\USGS_1M_19_x44y517_ME_CrownofMaine_2018_A18.tif')

    # Return example
    # rasterStatDict
    # '5eacfc1d82cefae35a250bec' = '10012,10012,1,1.0,GeoTIFF,Float32,-999999.0,PROJECTED,26919,NAD83 / UTM zone 19N,5150006.0,439994.0,450006.0,5139994.0,366.988,444.228,577.808,34.396,256,256'

    try:
        srcID = rasterItem[0]
        raster = rasterItem[1]
        rasterStatDict = dict()  # temp dict that will return raster information

        # Raster doesn't exist; download error
        if not os.path.exists(raster):
            AddMsgAndPrint(f"\t\t{os.path.basename(raster)} DOES NOT EXIST. Could not get Raster Information")
            rasterStatDict[srcID] = ','.join('#'*20)
            return rasterStatDict

        # Raster size is 0 bytes; download error
        if not os.stat(raster).st_size > 0:
            AddMsgAndPrint(f"\t\t{os.path.basename(raster)} Is EMPTY. Could not get Raster Information")
            rasterStatDict[srcID] = ','.join('#'*20)
            return rasterStatDict

        gdal.UseExceptions()    # Enable exceptions

        rds = gdal.Open(raster)
        rdsInfo = gdal.Info(rds,format="json")

        # Raster Properties
        columns = rdsInfo['size'][0]
        rows = rdsInfo['size'][1]
        bandCount = rds.RasterCount
        bitDepth = rdsInfo['bands'][0]['type']
        cellSize = rds.GetGeoTransform()[1]
        rdsFormat = rdsInfo['driverLongName']

        try:
            noDataVal = rdsInfo['bands'][0]['noDataValue']
        except:
            noDataVal = '#'

        # Raster Statistics
        # ComputeStatistics vs. GetStatistics(0,1) vs. ComputeBandStats
        # bandInfo = rds.GetRasterBand(1).ComputeStatistics(0) VS.
        # bandInfo = rds.GetRasterBand(1).GetStatistics(0,1)
        # bandInfo = rds.GetRasterBand(1).ComputeBandStats
        # (Min, Max, Mean, StdDev)

        # Take stat info from JSON info above; This should work for 1M
        # May not work for 3M or 10M
        try:
            minStat = rdsInfo['bands'][0]['min']
            meanStat = rdsInfo['bands'][0]['mean']
            maxStat = rdsInfo['bands'][0]['max']
            stDevStat = rdsInfo['bands'][0]['stdDev']
            blockXsize = rdsInfo['bands'][0]['block'][0]
            blockYsize = rdsInfo['bands'][0]['block'][1]

            # stat info is included in JSON info but stats are not calculated;
            # calc statistics if min,max or mean are not greater than 0.0
            # this can add significant overhead to the process
            if not minStat > 0 or not meanStat > 0 or not maxStat > 0:
                print(f"\t\t{os.path.basename(raster)} - Stats are set to 0 -- Calculating")
                bandInfo = rds.GetRasterBand(1)
                bandStats = bandInfo.ComputeStatistics(0)
                minStat = bandStats[0]
                maxStat = bandStats[1]
                meanStat = bandStats[2]
                stDevStat = bandStats[3]
                blockXsize = bandInfo.GetBlockSize()[0]
                blockYsize = bandInfo.GetBlockSize()[1]

        # Stat info is not included in JSON info above.
        # Force calculation of Raster Statistics
        # this can add significant overhead to the process
        except:
            print(f"\t\t{os.path.basename(raster)} Stats not present in info -- Forcing Calc'ing of raster info")
            bandInfo = rds.GetRasterBand(1)
            bandStats = bandInfo.ComputeStatistics(0)
            minStat = bandStats[0]
            maxStat = bandStats[1]
            meanStat = bandStats[2]
            stDevStat = bandStats[3]
            blockXsize = bandInfo.GetBlockSize()[0]
            blockYsize = bandInfo.GetBlockSize()[1]

        # Raster CRS Information
        # What is returned when a raster is undefined??
        prj = rds.GetProjection()  # GDAL returns projection in WKT
        srs = osr.SpatialReference(prj)
        srs.AutoIdentifyEPSG()
        epsg = srs.GetAttrValue('AUTHORITY',1)

        if not srs.GetAttrValue('projcs') is None:
            srsType = 'PROJECTED'
            srsName = srs.GetAttrValue('projcs')
        else:
            srsType = 'GEOGRAPHIC'
            srsName = srs.GetAttrValue('geogcs')

        # Returns 0 or 1; opposite would be IsGeographic
        if srs.IsProjected():
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

        rasterInfoList = [columns,rows,bandCount,cellSize,rdsFormat,bitDepth,noDataVal,srsType,epsg,srsName,
                          top,left,right,bottom,minStat,meanStat,maxStat,stDevStat,blockXsize,blockYsize]

        rasterStatDict[srcID] = ','.join(str(e) for e in rasterInfoList)
        return rasterStatDict

    except:
        errorMsg()
        rasterStatDict[srcID] = ','.join('#'*20)
        return rasterStatDict

#### ===================================================================================
def getRasterInformation(raster):

    # Raster information will be added to dlStatusList
    # sourceID,prod_title,path,numofFiles,unzipSize,timestamp,downloadstatus --
    # add additional data:
    # columns,rows,cellsize,bandcount,bitDepth,nodatavalue,

    # query srid
    # describe num of bands; what if it is more than 1

    try:

        if not os.path.exists(raster):
            print("\t{raster} DOES NOT EXIST. Could not get Raster Information")
            return False
            #return ','.join('#'*20)

        gdal.UseExceptions()    # Enable exceptions

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
        # ComputeStatistics vs. GetStatistics(0,1) vs. ComputeBandStats
        # bandInfo = rds.GetRasterBand(1).ComputeStatistics(0) VS.
        # bandInfo = rds.GetRasterBand(1).GetStatistics(0,1)
        # bandInfo = rds.GetRasterBand(1).ComputeBandStats
        # (Min, Max, Mean, StdDev)

        # Take stat info from JSON info above; This should work for 1M
        # May not work for 3M or 10M
        try:
            minStat = rdsInfo['bands'][0]['min']
            meanStat = rdsInfo['bands'][0]['mean']
            maxStat = rdsInfo['bands'][0]['max']
            stDevStat = rdsInfo['bands'][0]['stdDev']
            blockXsize = rdsInfo['bands'][0]['block'][0]
            blockYsize = rdsInfo['bands'][0]['block'][1]

            # stat info is included in JSON info but stats are not calculated;
            # calc statistics if min,max or mean are not greater than 0.0
            # this can add significant overhead to the process
            if not minStat > 0 or not meanStat > 0 or not maxStat > 0:
                AddMsgAndPrint(f"\t\t{os.path.basename(raster)} - Stats are set to 0 -- Calculating")
                bandInfo = rds.GetRasterBand(1)
                bandStats = bandInfo.ComputeStatistics(0)
                minStat = bandStats[0]
                maxStat = bandStats[1]
                meanStat = bandStats[2]
                stDevStat = bandStats[3]
                blockXsize = bandInfo.GetBlockSize()[0]
                blockYsize = bandInfo.GetBlockSize()[1]

        # Stat info is not included in JSON info above.
        # Force calculation of Raster Statistics
        # this can add significant overhead to the process
        except:
            print(f"{os.path.basename(raster)} Stats not present in info -- Forcing Calc'ing of raster info")
            bandInfo = rds.GetRasterBand(1)
            bandStats = bandInfo.ComputeStatistics(0)
            minStat = bandStats[0]
            maxStat = bandStats[1]
            meanStat = bandStats[2]
            stDevStat = bandStats[3]
            blockXsize = bandInfo.GetBlockSize()[0]
            blockYsize = bandInfo.GetBlockSize()[1]

        # Raster CRS Information
        # What is returned when a raster is undefined??
        prj = rds.GetProjection()  # GDAL returns projection in WKT
        srs = osr.SpatialReference(prj)
        srs.AutoIdentifyEPSG()
        epsg = srs.GetAttrValue('AUTHORITY',1)

        if not srs.GetAttrValue('projcs') is None:
            srsType = 'PROJECTED'
            srsName = srs.GetAttrValue('projcs')
        else:
            srsType = 'GEOGRAPHIC'
            srsName = srs.GetAttrValue('geogcs')

        # Returns 0 or 1; opposite would be IsGeographic
        if srs.IsProjected():
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

        rasterInfoList = [columns,rows,bandCount,cellSize,rdsFormat,bitDepth,noDataVal,srsType,epsg,srsName,
                          top,left,right,bottom,minStat,meanStat,maxStat,stDevStat,blockXsize,blockYsize]

        return ','.join(str(e) for e in rasterInfoList)

    except:
        errorMsg()
        return False

## ===================================================================================
def createRaster2pgSQLFile(masterElevFile):

    # Mutually exclusive options
    # -a Append raster(s) to an existing table.

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

    # raster2pgsql -s 4269 -b 1 -t 507x507 -F -R -I /data03/gisdata/dsmgroup/aspct_16.tif elevation.$name | PGPASSWORD=itsnotflat psql -U elevation -d elevation -h 10.11.11.10 -p 5432

    # raster2pgsql -s 5070 -b 1 -I -t 560x560 -F -R /data03/gisdata/dsmgroup/aspct_16.tif
    # covariate.conus_aspect_16 | PGPASSWORD=itsnotflat psql -U covariate -d elevation -h 10.11.11.10 -p 5432

    # Before

    try:
        global resolution
        masterElevRecCount = len(open(masterElevFile).readlines()) - 1  # subtract header

        recCount = 0
        r2pgsqlFilePath = f"{os.path.dirname(downloadFile)}{os.sep}USGS_3DEP_{resolution}_Step2_RASTER2PGSQL.txt"

        g = open(r2pgsqlFilePath,'a+')

        total = sum(1 for line in open(masterElevFile)) -1
        #label = "Generating Raster2PGSQL Statements"

        invalidCommands = list()

        """ ------------------- Open Master Elevation File and write raster2pgsql statements ---------------------"""
        with open(masterElevFile, 'r') as fp:
            for line in fp:

                # Skip header line and empty lines
                if recCount == 0 or line == "\n":
                    recCount+=1
                    continue

                items = line.split(',')

                # Raster2pgsql parameters
                srid = items[19]
                tileSize = '507x507'
                demPath = f"{items[10]}{os.sep}{items[9]}"
                dbName = 'elevation'
                dbTable = f"elevation_{resolution.lower()}"  # elevation_3m
                demName = items[9]
                password = 'itsnotflat'
                localHost = '10.11.11.10'
                port = '6432'

                r2pgsqlCommand = f"raster2pgsql -s {srid} -b 1 -t {tileSize} -F -a -R {demPath} {dbName}.{dbTable} | PGPASSWORD={password} psql -U {dbName} -d {dbName} -h {localHost} -p {port}"

                # Add check to look for # in r2pgsqlCommand
                if r2pgsqlCommand.find('#') > -1:
                    invalidCommands.append(r2pgsqlCommand)

                if recCount == masterElevRecCount:
                    g.write(r2pgsqlCommand)
                else:
                    g.write(r2pgsqlCommand + "\n")

                print(f"\t\tSuccessfully wrote raster2pgsql command -- ({recCount:,} of {total:,})")
                recCount+=1

        g.close()
        del masterElevRecCount

        # Inform user about invalid raster2pgsql commands so that they can be fixed.
        numOfInvalidCommands = len(invalidCommands)
        if numOfInvalidCommands:
            AddMsgAndPrint(f"\tThere are {numOfInvalidCommands:,} invalid raster2pgsql commands or that contain invalid parameters:")
            for invalidCmd in invalidCommands:
                AddMsgAndPrint(f"\t\t{invalidCmd}")

        return r2pgsqlFilePath

    except:
        errorMsg()


## ===================================================================================
def createErrorLogFile(downloadFile,failedDownloadList,headerValues):

    try:
        errorFile = f"{os.path.dirname(downloadFile)}{os.sep}USGS_3DEP_{resolution}_Step2_Download_FAILED.txt"

        AddMsgAndPrint(f"\tDownload Errors Logged to: {errorFile}")
        g = open(errorFile,'a+')

        lineNum = 0
        numOfErrors = 0
        with open(downloadFile, 'r') as fp:
            for line in fp:
                items = line.split(',')

                # Duplicate header from dlFile and write it to errorFile
                if bHeader and lineNum == 0:
                    g.write(line.strip())
                    lineNum +=1

                huc8digit = items[headerValues.index("huc_digit")]
                downloadURL = items[headerValues.index("download_url")].strip()

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
def main(dlFile,dlDir,bReplace):

    try:

        startTime = tic()
        global bHeader
        global bReplaceData
        global downloadFile
        global elevMetadataDict
        global headerValues
        global resolution

        # 6 Tool Parameters
        downloadFile = dlFile         # Download File
        dlFolder = dlDir              # Download Directory
        bHeader = True
        bDownloadMultithread = True
        bReplaceData = bReplace
        bUnzipFiles = True
        bDeleteZipFiles = False

        # Pull elevation resolution from file name
        # USGS_3DEP_1M_Step1B_ElevationDL_04132023.txt
        resolution = downloadFile.split(os.sep)[-1].split('_')[2]

        # ['huc_digit','prod_title','pub_date','last_updated','size','format'] ...etc
        headerValues = open(downloadFile).readline().rstrip().split(',')

        urlDownloadDict = dict()  # contains download URLs and sourceIDs grouped by HUC; 07040006:[[ur1],[url2]]
        elevMetadataDict = dict() # contains all input info from input downloadFile.  sourceID:dlFile items
        recCount = 0
        badLines = 0
        uniqueSourceIDList = list()

        """ ---------------------------- Open Download File and Parse Information into dictionary ------------------------"""
        with open(downloadFile, 'r') as fp:
            for line in fp:
                items = line.split(',')

                # Skip header line and empty lines
                if bHeader and recCount == 0 or line == "\n":
                    recCount+=1
                    continue

                # Skip if number of items are incorrect
                if len(items) != len(headerValues):
                    badLines+=1
                    recCount+=1
                    continue

                hucDigit = items[headerValues.index("huc_digit")]
                prod_title = items[headerValues.index("prod_title")]
                pub_date = items[headerValues.index("pub_date")]
                last_updated = items[headerValues.index("last_updated")]
                size = items[headerValues.index("rast_size")]
                fileFormat = items[headerValues.index("format")]
                sourceID = items[headerValues.index("sourceID")]
                metadata_url = items[headerValues.index("metadata_url")]
                downloadURL = items[headerValues.index("download_url")].strip()

                # Add info to urlDownloadDict
                if hucDigit in urlDownloadDict:
                    urlDownloadDict[hucDigit].append([downloadURL,sourceID])
                else:
                    urlDownloadDict[hucDigit] = [[downloadURL,sourceID]]

                # Add info to elevMetadataDict
                elevMetadataDict[sourceID] = [hucDigit,prod_title,pub_date,last_updated,
                                              size,fileFormat,sourceID,metadata_url,downloadURL]
                recCount+=1

        # subtract header for accurate record count
        if bHeader: recCount = recCount -1

        """ ---------------------------- Establish Console LOG FILE ---------------------------------------------------"""
        today = datetime.today().strftime('%m%d%Y')

        # Log file that captures console messages
        #logFile = os.path.basename(downloadFile).split('.')[0] + "_Download_ConsoleMsgs.txt"
        global msgLogFile
        msgLogFile = f"{os.path.dirname(downloadFile)}{os.sep}USGS_3DEP_{resolution}_Step2_Download_ConsoleMsgs.txt"

        h = open(msgLogFile,'a+')
        h.write(f"Executing: USGS_2_Download_Elevation_by_MetadataFile {today}\n\n")
        h.write(f"User Selected Parameters:\n")
        h.write(f"\tDownload File: {downloadFile}\n")
        h.write(f"\tFile has header: {bHeader}\n")
        h.write(f"\tDownload Multithread: {bDownloadMultithread}\n")
        h.write(f"\tReplace Data: {bReplaceData}\n")
        h.write(f"\tUnzip Files: {bUnzipFiles}\n")
        h.write(f"\tDelete Zip Files: {bDeleteZipFiles}\n")
        h.write(f"\tLog File Path: {msgLogFile}\n")
        h.close()

        AddMsgAndPrint(f"\n{'='*125}")
        AddMsgAndPrint((f"Total Number of files to download: {recCount:,}"))

        """ ----------------------------- DOWNLOAD ELEVATION DATA ----------------------------- """
        global failedDownloadList
        global dlZipFileDict
        global dlImgFileDict
        global totalDownloadSize

        failedDownloadList = list()
        dlZipFileDict = dict()  # sourceID:path to downloaded zip file
        dlImgFileDict = dict()  # sourceID:path to downloaded image file (single)
        totalDownloadSize = 0

        if len(urlDownloadDict) > 0:

            dlStart = tic()
            dlTracker = 0

            if bDownloadMultithread: AddMsgAndPrint(f"\nDownloading in Multi-threading Mode - # of Files: {recCount:,}")
            else:                    AddMsgAndPrint(f"\nDownloading in Single Request Mode - # of Files: {recCount:,}")

            # 01010201:[URL,sourceID]
            for huc,items in urlDownloadDict.items():
                i = 1
                numOfHUCelevTiles = len(items)

                # if OS is Linux then downloadfolder will have to be set
                # if OS is Windows then downloadfolder was passed in.
                if not dlFolder:
                    downloadFolder = getDownloadFolder(huc,resolution)
                    if not downloadFolder:
                        AddMsgAndPrint(f"\n\tFailed to set download folder for {huc}. {numOfHUCelevTiles:,} will NOT be downloaded")
                        continue
                else:
                    downloadFolder = dlFolder

                AddMsgAndPrint(f"\n\tDownloading {numOfHUCelevTiles:,} elevation tiles for HUC: {huc} ---> {downloadFolder}")

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
                                    AddMsgAndPrint(f"{printMessage} -- ({dlTracker:,} of {recCount:,})")
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

                totalUnzipSize = 0
                unzipStart = tic()
                unzipTracker = 0

                if len(dlZipFileDict):
                    AddMsgAndPrint(f"\nUnzipping {len(dlZipFileDict)} Elevation Files")

                    with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
                        unZipResults = {executor.submit(unzip, item, bDeleteZipFiles): item for item in dlZipFileDict.items()}

                        # yield future objects as they are done.
                        for msg in as_completed(unZipResults):
                            unzipTracker+=1
                            j=1
                            for printMessage in msg.result():
                                if j==1:
                                    AddMsgAndPrint(f"{printMessage} -- ({unzipTracker:,} of {len(dlZipFileDict):,})")
                                else:
                                    AddMsgAndPrint(printMessage)
                                j+=1

                else:
                    AddMsgAndPrint(f"\nThere are no files to uzip")
                unzipStop = toc(unzipStart)

        else:
            print("\nThere are no elevation tiles to download")


        """ ----------------------------- Create Elevation Metadata File ----------------------------- """
        if len(dlImgFileDict):
            AddMsgAndPrint(f"\nCreating Elevation Metadata File")
            dlMasterFileStart = tic()
            dlMasterFile = createMasterDBfile_MT(dlImgFileDict,elevMetadataDict)
            AddMsgAndPrint(f"\n\tElevation Metadata File Path: {dlMasterFile}")
            dlMasterFileStop = toc(dlMasterFileStart)

            """ ----------------------------- Create Raster2pgsql File ---------------------------------- """
            if os.path.exists(dlMasterFile):
                AddMsgAndPrint(f"\nCreating Raster2pgsql File")
                r2pgsqlStart = tic()
                r2pgsqlFile = createRaster2pgSQLFile(dlMasterFile)
                AddMsgAndPrint(f"\tRaster2pgsql File Path: {r2pgsqlFile}")
                AddMsgAndPrint(f"\tIMPORTANT: Make sure dbTable variable (elevation_3m) is correct in Raster2pgsql file!!")
                r2pgsqlStop = toc(r2pgsqlStart)
            else:
                AddMsgAndPrint(f"\nRaster2pgsql File will NOT be created")
        else:
            AddMsgAndPrint(f"\nNo information available to produce Master Database Elevation File")
            AddMsgAndPrint(f"\nNo information available to produce Raster2pgsql File")

        """ ------------------------------------ SUMMARY -------------------------------------------- """
        AddMsgAndPrint(f"\n{'-'*40}SUMMARY{'-'*40}")

        AddMsgAndPrint(f"\nTotal Processing Time: {toc(startTime)}")
        AddMsgAndPrint(f"\tDownload Time: {dlStop}")
        if len(dlZipFileDict) > 0:
                AddMsgAndPrint(f"\tUnzip Data Time: {unzipStop}")
        AddMsgAndPrint(f"\tCreate Master Elevation File Time: {dlMasterFileStop}")
        AddMsgAndPrint(f"\tCreate Raster2pgsql File Time: {r2pgsqlStop}")

        if totalDownloadSize > 0:
            AddMsgAndPrint(f"\nTotal Download Size: {convert_bytes(totalDownloadSize)}")

        # Report number of DEMs downloaded
        if len(dlImgFileDict) == recCount:
            AddMsgAndPrint(f"\nSuccessfully Downloaded ALL {len(dlImgFileDict):,} DEM files")
        elif len(dlImgFileDict) == 0:
            AddMsgAndPrint(f"\nNo DEM files were downloaded")
        else:
            AddMsgAndPrint(f"\nDownloaded {len(dlImgFileDict):,} out of {recCount:,} DEM files")

        # Create Download Error File
        if len(failedDownloadList):
            AddMsgAndPrint(f"\nFailed to Download {len(failedDownloadList):,} elevation files:")
            errorlogFile = createErrorLogFile(downloadFile,failedDownloadList,headerValues)

        if bUnzipFiles:
            if len(dlZipFileDict) > 0:
                #totalUnzipSize = sum([os.stat(x).st_size for x in glob.glob(f"{downloadFolder}\\*") if not x.endswith(".zip")])
                AddMsgAndPrint(f"\tNumber of files to unzip: {len(dlZipFileDict):,}")
                AddMsgAndPrint(f"\tTotal Unzipped Size: {convert_bytes(totalUnzipSize)}")

        AddMsgAndPrint(f"\nAll console messages were logged to: {msgLogFile}")

    except:
        AddMsgAndPrint(errorMsg(errorOption=2))

## ============================================================================================================
if __name__ == '__main__':

    # DOWNLOAD FILE
    dlFile = input("\nEnter full path to USGS Metadata Download Text File: ")
    while not os.path.exists(dlFile):
        print(f"{dlFile} does NOT exist. Try Again")
        dlFile = input("Enter full path to USGS Metadata Download Text File: ")

    # DOWNLOAD FOLDER
    # Windows (nt) vs Linux (posix)
    if os.name == 'nt':
        dlFolder = input("\nEnter path where elevation files will be download to: ")
        while not os.path.exists(dlFolder):
            print(f"{dlFolder} does NOT exist. Try Again")
            dlFolder = input("Enter path where elevation files will be download to: ")
    else:
        dlFolder = False

    # REPLACE DATA
    bReplace = input("\nDo you want to replace existing data? (Yes/No): ")
    while not bReplace.lower() in ("yes","no","y","n"):
        print(f"Please Enter Yes or No")
        bReplace = input("Do you want to replace existing data? (Yes/No): ")

    if bReplace.lower() in ("yes","y"):
        bReplace = True
    else:
        bReplace = False

##    dlFile = r'D:\projects\DSHub\reampling\USGS_3DEP_10M_Metadata_Elevation_03142023.txt'
##    dlFolder = r''
##    bReplace = False

    main(dlFile,dlFolder,bReplace)
    input("\nHit Enter to Continue: ")
    exit()