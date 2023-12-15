# -*- coding: utf-8 -*-
"""
Script Name: USGS_2_Download_Elevation_by_MetadataFile.py
Created on Fri Sep  2 10:12:13 2022
updated 8/28/2023

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
    4) bReplaceData - boolean indicator to replace existing elevation file
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

5/19/2023
    - Updated the following header values to remove issues with shapefile field constraints.
      The following header field names changed:
        last_updated > lastupdate
        sourceID     > sourceid
        metadata_url > meta_url
        download_url > downld_url
        DEMname      > dem_name
        DEMpath      > dem_path
        rast_columns > rds_column
        rast_rows    > rds_rows
        bandCount    > bandcount
        cellSize     > cellsize
        rdsFormat    > rdsformat
        bitDepth     > bitdepth
        noDataVal    > nodataval
        srType       > srs_type
        EPSG         > epsg_code
        srsName      > srs_name
        rast_top     > rds_top
        rast_left    > rds_left
        rast_right   > rds_right
        rast_bottome > rds_bottom
        minStat      > min
        meanStat     > mean
        maxStat      > max
        stDevStat    > stdev
        blockXsize   > blck_xsize
        blockYsize   > blck_ysize

6/2/2023
    - Modified DownloadElevationTile function to handle zip files from 3M

7/7/2023
    - Updated getRasterInformation_MT function to provide error handling for EPSG
      codes.  AK 5M DEMs were erroring when describing SRS.

      File "USGS_2_Download_Elevation_by_MetadataFile.py", line 870, in getRasterInformation_MT
      srs.AutoIdentifyEPSG()

      RuntimeError: OGR Error: Unsupported SRS

      Also added functionality to pass raster information that had already been described
      vs. returning all '#'s

7/11/2023
    - Updated the srsName in rasterInformation function to remove commas.  This gets translated as
      a separate value when importing the master metadata file in the postgres database.
    - Replaced '#' with 'None' in repsonse to DBeaver import errors.  Turns out I could've left it alone
      and simply set the NULL mark value to '#' but None is more intuitive and pythonic.

7/27/2023
    - Updated the unzip function to look for specific DEMs when dealing with the 5M_AK_DSM
    - Replaced the usage of 'HUC' and
    - Modified the createErrorLogFile to add an increment to the newly created error log file in case
      the errorFile might be the downloadFile being used as a 2nd run to download failed DEMs from the
      first run; Not doing so will produce an empty file b/c the downloadfile and errorFile are the same.

8/28/2023
    - Updated the AddMsgAndPrint function to accept a list of messages instead of a single message.
      This will reduce the number of text operations when downloading and unzipping files.
    - Updated the getRasterInformation_MT function to remove multiple attempts of retrieving
      statistical data from rasters.  This was causing each raster to compute statistics.  Might
      have to add this back in.
    - Remove option to download in single thread option.  All downloading will only happen in
      using multi-threading.
      
12/11/2023
    - Added poly_name to metadata

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
import sys, os, traceback, glob, fnmatch
import urllib, time, zipfile, psutil
import numpy as np
from datetime import datetime

import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed

from osgeo import gdal
from osgeo import osr

from urllib.request import urlopen, URLError
from urllib.error import HTTPError
urllibEncode = urllib.parse.urlencode


## ===================================================================================
def AddMsgAndPrint(msg,msgList=list()):

    # Add message to log file
    try:
        if msgList:
            h = open(msgLogFile,'a+')
            for msg in msgList:
                print(msg)
                h.write("\n" + msg)
            h.close
            del h

        else:
            print(msg)
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
def getDownloadFolder2(filename):
    """ This function returns a directory that was created in LINUX.  Directory
        structure was determined by parsing the filename for each unique USGS
        Dataset product.  The following describes the product and dir determination
         - 1M: isolate lat long from name (xy) and take the last digit
               'USGS_one_meter_x41y492_ME_NRCS_Lot4_2013.tif' --> x41y492 --> 2
         - 3M: isolate upper left xy and take the first digit
               'ned19_n27x25_w082x25_fl_wcnt5co04_eastsarasota_2007.zip' --> n27x25 --> 2
         - 10M: isolate 1-x1 degree block and take the last digit
               'USGS_13_n58w171_20200415.tif' --> n58w171 --> 5
         - 30M: isolate 1-x1 degree block and take the last digit
               'USGS_1_n07e158_20130911.tif' --> n07e158 --> 0
         - Other: take the last digit
    """

    try:
        import random
        # ['USGS', '1M', '19', 'x44y515', 'ME', 'CrownofMaine', '2018', 'A18.tif']
        fileElements = filename.split('_')

        # Remove USGS from the name
        if fileElements[0] == 'USGS':
            del fileElements[0]

        # Lists of unique naming patterns by dataset product
        oneM = ['1M', 'one','ONE','One', '1m']
        threeM = ['ned19','imgned19']
        tenM = ['13']
        thirtyM = ['1']
        ak_opr = ['ak','AK','opr','OPR']

        firstElement = fileElements[0]

        # ------------------------ File is a 1M USGS File
        if firstElement in oneM:
            i=0
            block = ''
            ld = ''
            for element in fileElements:

                # x41y492
                if element.startswith('x'):
                    block = fileElements[i]
                    break
                i+=1

            if block[-1].isdigit():
                ld = block[-1]

            res = '1m'

        # ------------------------ File is a 3M USGS File
        # The EBS distribution of 3M files is very uneven.
        # Need to develop a better way to distribute files
        elif firstElement in threeM:
            i=0
            block = ''
            ld = ''

            if fileElements[0] in threeM:
                del fileElements[0]

            for element in fileElements:
                # n39x00
                if element.startswith('n'):
                    block = fileElements[i]
                    break
                i+=1

            for digit in block:
                if digit.isdigit():
                    ld = digit
                    break

            res = '3m'

        # ------------------------ File is a 10M USGS File
        elif firstElement in tenM:
            i=0
            block = ''
            ld = ''

            if fileElements[0] in tenM:
                del fileElements[0]

            for element in fileElements:

                # n61w166
                if element.startswith('n') or element.startswith('s'):
                    block = fileElements[i]
                    break
                i+=1

            for digit in reversed(block):
                if digit.isdigit():
                    ld = digit
                    break

            res = '10m'

        # ------------------------ File is a 30M USGS File
        elif firstElement in thirtyM:
            i=0
            block = ''
            ld = ''

            if fileElements[0] in thirtyM:
                del fileElements[0]

            for element in fileElements:

                # n61w166 or s14w170
                if element.startswith('n') or element.startswith('s'):
                    block = fileElements[i]
                    break
                i+=1

            for digit in reversed(block):
                if digit.isdigit():
                    ld = digit
                    break

            res = '30m'

        # ------------------------ File is an Alaska OPR USGS_OPR_AL_25Co_B5_2017_16R_FV_7230.tif
        elif firstElement in ak_opr:
            ld = ''
            
            for c in reversed(filename):
                if c.isdigit():
                    ld = c
                    break
                
            # assign random drive
            if ld is None:
                dataDrives = [2,3,4,5,6,7,8,9]
                ld = random.choice(dataDrives)
                
            res = 'ak_opr'
                
        # ------------------------ oddball files that don't follow the majority convention
        # i.e. n65w158.zip
        else:
            ld = None

            for char in reversed(filename):
                if char.isdigit():
                    ld = char
                    break

            # assign random drive
            if ld is None:
                dataDrives = [2,3,4,5,6,7,8,9]
                ld = random.choice(dataDrives)
            res = resolution.lower()

        # last digit
        if ld in ["1","9","0"]:
            if res in ('1m','30m'):
                drive = '06'
            else:
                drive = '02'

        elif ld in ["2","7","8"]:
            if res in ('1m','30m'):
                drive = '07'
            else:
                drive = '03'

        elif ld in ["3","6"]:
            if res in ('1m','30m'):
                drive = '08'
            else:
                drive = '04'

        elif ld in ["4","5"]:
            if res in ('1m','30m'):
                drive = '09'
            else:
                drive = '05'

        else:
            AddMsgAndPrint("\t\t\tLook into download directory for {filename}")
            drive = '09'

        root = f"{os.sep}data{drive}{os.sep}gisdata{os.sep}elev"
        downloadFolder = f"{root}{os.sep}{res}{os.sep}{ld}"

        # In case another multi-thread is creating the same directory
        try:
            if not os.path.exists(downloadFolder):
                os.makedirs(downloadFolder)
        except:
            pass

        return downloadFolder

    except:
        errorMsg()
        AddMsgAndPrint("\t\t\tFailed to determine download directory for {filename}")
        return False

## ===================================================================================
def DownloadElevationTile(itemCollection):
    """This function will open a URL and download the contents to the specified
     download folder. If bReplaceData is True, delete the local file version of the
     download file.
     Total download file size will be tallied.
     Information to create download Status file will be collected:
     sourceID,prod_title,downloadPath,numOfFiles,size,now,True if successful download else False

     It can be used within the ThreadPoolExecutor to send multiple USGS downloads

     Parameters
     itemCollection (dictionary):
       key:poly_code
       values:
           url: https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n47x75_w120x25_wa_columbiariver_2010.zip
           sourceID = 17020010
           fileFormat = 'IMG' or 'GeoTIFF'

     Returns
     a list of messages that will be printed as a "future" object representing the execution of the callable.
    """

    try:
        messageList = list()
        global dlZipFileDict     # dict containing zip files that will be unzipped; sourceID:filepath
        global dlImgFileDict     # dict containing DEM files; sourceID:filepath
        global totalDownloadSize

        dlURL = itemCollection[0]
        sourceID = itemCollection[1]
        fileFormat = itemCollection[2]

        theTab = "\t\t"

        # Filename could be a zipfile or DEM file (.TIF, .IMG)
        # 'https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n30x50_w091x75_la_atchafalayabasin_2010.zip'
        fileName = dlURL.split('/')[-1]

        # ------------------  Determine Download Directory
        # if OS is Linux then downloadfolder will have to be set
        # if OS is Windows then downloadfolder was passed in.
        if not dlFolder:
            downloadFolder = getDownloadFolder2(fileName)
            if not downloadFolder:
                messageList.append(f"{theTab}Failed to set download folder for {fileName}")
                return False
        else:
            downloadFolder = dlFolder

        # path to where file will be downloaded to - zip or DEM
        local_file = f"{downloadFolder}{os.sep}{fileName}"

        # ====================================================================
        def searchDirforDEM(filename,returnFile=False):
            """ This function searches for DEM(s) within a directory and either retuns
                TRUE or FALSE or returns a list of filenames.
                - Return False even if 1 DEM is missing from zipfile"""

            if fileFormat in ('geotiff','tiff','tif'):
                ext = '.tif'
            else:
                ext = '.img'

            # 5M_AK_DSM product returns zipfiles with multiple DEMs with arbitrary names
            if resolution == '5M_AK_DSM':

                zipFile = zipfile.ZipFile(filename)
                zipFileList = zipFile.filelist
                localDEMlist = list() # list of local DEM files that exist based on the existing zipfile

                for zinfo in zipFileList:
                    if zinfo.filename.endswith(ext):
                        localDEMpath = f"{downloadFolder}{os.sep}{zinfo.filename}"
                        if os.path.exists(localDEMpath):
                            localDEMlist.append(localDEMpath)
                        else:
                            return False

                if localDEMlist:
                    return localDEMlist
                else:
                    return False

            # Return DEM path
            demWildCard = f"{downloadFolder}{os.sep}{fileName.split('.')[0]}*"

            for file in glob.glob(demWildCard):
                if file.endswith(ext):
                    if returnFile:
                        return file
                    else:
                        return True
            return False
        # ====================================================================

        # DL file exists: delete it if bReplaceData is True
        if os.path.isfile(local_file):

            # bReplaceData = True: Delete existing local_file before redownloading
            if bReplaceData:
                try:
                    os.remove(local_file)
                    messageList.append(f"{theTab}{'File Exists; Deleted':<40} {fileName:<60}")
                except:
                    messageList.append(f"{theTab:q!}{'File Exists; Failed to Delete':<40} {fileName:<60}")
                    failedDownloadList.append(dlURL)
                    return messageList

            # bReplaceData = False: Capture
            else:
                dlSize = os.stat(local_file).st_size
                if dlSize > 0:
                    totalDownloadSize+=dlSize

                    # local_file is a ZIP FILE: verify if DEM(s) exists within dlFolder.
                    # DEM exists: don't unzip --- DEM doesn't exist: unzip
                    if zipfile.is_zipfile(local_file):

                        # DEM already exists in directory; no need to unzip it again.
                        result = searchDirforDEM(fileName,returnFile=True)
                        if result:
                            dlImgFileDict[sourceID] = result
                            messageList.append(f"{theTab}{'Zipfile and DEM exist. Adding to Raster2pgsql file:':<40} {os.path.basename(result):<60} {convert_bytes(os.stat(result).st_size):>20}")

                        # DEM(s) are missing; unzip it again to be safe
                        else:
                            dlZipFileDict[sourceID] = local_file
                            messageList.append(f"{theTab}{'Zipfile will be unzipped:':<40} {fileName:<60} {convert_bytes(dlSize):>20}")

                    # IMG or TIF; add to dlImgFileDict
                    else:
                        dlImgFileDict[sourceID] = local_file
                        messageList.append(f"{theTab}{'DEM will be added to Raster2pgsql file:':<40} {fileName:<60} {convert_bytes(dlSize):>20}")
                    return messageList

                # rare possibility - local file is corrup
                else:
                    try:
                        os.remove(local_file)
                        messageList.append(f"{theTab}{'File was 0 bytes; Deleted':<35} {fileName:<60}")
                    except:
                        messageList.append(f"{theTab:q!}{'File was 0 bytes; Failed to Delete':<35} {fileName:<60}")
                        failedDownloadList.append(dlURL)
                        return messageList

        # DL file doens't exist: download file - could be a IMG, TIF or Zip file.
        # 3M zip files don't exist but unzipped DEM may exist
        elif fileName.endswith('.zip'):

            # DEM already exists in directory inspite of zipfile being absent
            result = searchDirforDEM(fileName,returnFile=True)
            if result:
                dlSize = os.stat(result).st_size
                totalDownloadSize+=dlSize
                dlImgFileDict[sourceID] = result
                messageList.append(f"{theTab}{'Zipfile is absent but DEM is present. Adding to Raster2pgsql file:':<40} {os.path.basename(result):<60} {convert_bytes(dlSize):>20}")
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

        messageList.append(f"{theTab}{'Successfully Downloaded:':<40} {fileName:<60} {convert_bytes(dlSize):>15}")
        del request, output, dlSize

        return messageList

    except URLError as e:
        messageList.append(f"{theTab}{'Failed to Download:':<40} {fileName:<60} {str(e):>15}")
        #messageList.append(f"\t{theTab}{e.__dict__}")
        failedDownloadList.append(dlURL)
        return messageList

    except HTTPError as e:
        messageList.append(f"{theTab}{'Failed to Download:':<40} {fileName:<60} {str(e):>15}")
        #messageList.append(f"\t{theTab}{e.__dict__}")
        failedDownloadList.append(dlURL)
        return messageList

    except:
        messageList.append(f"{theTab}{'Unexpected Error:':<40} {fileName:<60} -- {dlURL}")
        failedDownloadList.append(dlURL)
        messageList.append(f"\t{theTab}{errorMsg(errorOption=2)}")
        return messageList



## ===================================================================================
def unzip(itemCollection):
    """ This function will unzip a list of zipfiles

        itemCollection:
       ('581d2d68e4b08da350d665a5',r'E:\python_scripts\DSHub\LinuxWorkflow\TEMP_testingFiles\ned19_n42x75_w091x00_ia_northeast_2007.zip')

       returns a collection of messages to be added to log file

    """
    try:
        # Collection of messages to return
        messageList = list()
        sourceID = itemCollection[0]
        local_zip = itemCollection[1]
        zipName = local_zip.split(os.sep)[-1]

        global totalUnzipSize
        global elevMetadataDict
        global headerValues
        global dlImgFileDict

        leftAlign = 30

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

            if fileFormat in ('geotiff','tiff','tif'):
                fileType = 'tif'        # 1M DEMs, AK 5M DSM
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
                        messageList.append(f"\t\t{errorMessage}")
                        return messageList

                # iterate through zipfile contents and tally the size.
                # capture path of DEM file in dlImgFileDict
                unzipTally = 0
                for zinfo in zipFileList:
                    unzippedFilePath = f"{unzipFolder}{os.sep}{zinfo.filename}"

                    if os.path.exists(unzippedFilePath):
                        size = os.stat(unzippedFilePath).st_size
                        unzipTally+=size
                    else:
                        messageList.append(f"\t\t{zinfo.filename} wasn't properly unzipped...bizarre")

                    # Actual DEM file
                    if unzippedFilePath.endswith(fileType):

                        # Added this to ensure only 'DSM' files are captured and not ORI files for 5M_AK_DSM
                        if resolution == '5M_AK_DSM':
                            if zinfo.filename.startswith('DSM'):
                                demFilePath = unzippedFilePath
                                dlImgFileDict[sourceID] = unzippedFilePath
                            else:
                                messageList.append(f"\t\t{zinfo.filename} is not a valid DEM for the {resolution} product")

                        else:
                            demFilePath = unzippedFilePath
                            dlImgFileDict[sourceID] = unzippedFilePath

                totalUnzipSize+=unzipTally
                messageList.append(f"\t{'Successfully Unzipped:':<{leftAlign}} {zipName:<60} {convert_bytes(unzipTally):>15}")

            else:
                messageList.append(f"\t{'Empty Zipfile:':<{leftAlign}} {zipName:<60} {convert_bytes(zipSize):>15}")

        else:
            # Don't have a zip file, need to find out circumstances and document
            messageList.append(f"\t{'Invalid Zipfile:':<{leftAlign}} {zipName:<60}")

        return messageList

    except:
        messageList.append(f"\tError with {zipName} -- {errorMsg(errorOption=2)}")
        return messageList

## ===================================================================================
def del_zipFiles(zipFileDict):
    """ This function takes in a dictionary containing sourceID:zipFile
        and will delete zip files

        returns Nothing - only prints messages
    """

    leftAlign = 40

    for item in zipFileDict.items():

        #sourceID = item[0]
        local_zip = item[1]
        zipName = local_zip.split(os.sep)[-1]

        try:
            os.remove(local_zip)
            AddMsgAndPrint(f"\t{'Successfully Deleted Zip File:':<{leftAlign}} {zipName:<55} ")
        except:
            AddMsgAndPrint(f"\t{'Failed to Delete Zip File:':<{leftAlign}} {zipName:<55} ")
            AddMsgAndPrint(f"\tError with {zipName} -- {errorMsg()}")


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
    """ This function creates a master elevation text file: USGS_3DEP_1M_Step2_Elevation_Metadata.txt
        that is a combination of the input dlFile AND raster statistics for each DEM.
        The raster statistics are gathered by invoking the 'getRasterInformation_MT' function.

        This function takes in a dictionary of sourceID(key) = path to DEM(value) and a
        dictionary containing the dlFile information.

        dlImgFileDict: sourceID = rasterPath

        returns the path to Master Elevation File
    """
    try:
        demStatDict = dict()
        goodStats = 0
        badStats = 0

        totalFiles = len(dlImgFileDict)
        counter = 0

        if os.name == 'nt':
            numOfCores = int(psutil.cpu_count(logical = False))         # 16 workers
        else:
            numOfCores = int(psutil.cpu_count(logical = True) / 2)      # 32 workers

        """ ----------------------------- Step 1: Gather Statistic Information for all rasters ----------------------------- """
        AddMsgAndPrint("\n\tGathering Individual DEM Statistical Information")
        with ThreadPoolExecutor(max_workers=numOfCores) as executor:

            # use a set comprehension to start all tasks.  This creates a future object
            rasterStatInfo = {executor.submit(getRasterInformation_MT, rastItem): rastItem for rastItem in dlImgFileDict.items()}

            # yield future objects as they are done.
            for stats in as_completed(rasterStatInfo):
                resultDict = stats.result()
                for results in resultDict.items():
                    ID = results[0]
                    rastInfo = results[1]
                    counter +=1

                    if rastInfo.find('#')>-1:
                        badStats+=1
                        print(f"\t\tFailed to retrieve DEM Statistical Information {counter:,} of {totalFiles:,}")
                    else:
                        goodStats+=1
                        print(f"\t\tSuccessfully retrieved DEM Statistical Information {counter:,} of {totalFiles:,}")

                    demStatDict[ID] = rastInfo

        if goodStats:AddMsgAndPrint(f"\n\t\tSuccessfully Gathered stats for {goodStats:,} DEMs")
        if badStats:AddMsgAndPrint(f"\n\t\tProblems with Gathering stats for {badStats:,} DEMs")

        global downloadFile

        """ ----------------------------- Step 2: Create Master Elevation File ----------------------------- """
        dlMasterFilePath = f"{os.path.dirname(downloadFile)}{os.sep}USGS_3DEP_{resolution}_Step2_Elevation_Metadata.txt"

        # Add headers to the beginning of the file if the file is new.
        if not os.path.exists(dlMasterFilePath):

            g = open(dlMasterFilePath,'a+')

            # rast_size, rast_columns, rast_rows, rast_top, rast_left, rast_right, rast_bottom
            header = ('poly_code,poly_name,prod_title,pub_date,lastupdate,rds_size,format,sourceid,meta_url,'
                      'downld_url,dem_name,dem_path,rds_column,rds_rows,bandcount,cellsize,rdsformat,bitdepth,nodataval,srs_type,'
                      'epsg_code,srs_name,rds_top,rds_left,rds_right,rds_bottom,rds_min,rds_mean,rds_max,rds_stdev,blk_xsize,blk_ysize')
            g.write(header)

        # else master elevation file exists
        else:
            g = open(dlMasterFilePath,'a+')

        total = len(elevMetadataDict)
        index = 1

        # Iterate through all of the sourceID files in the download file (elevMetadatDict) and combine with stat info
        for srcID,demInfo in elevMetadataDict.items():

            # 9 item INFO: poly_code,prod_title,pub_date,last_updated,size,format,sourceID,metadata_url,download_url
            firstPart = ','.join(str(e) for e in demInfo)

            # srcID must exist in dlImgFileDict (successully populated during download)
            if srcID in dlImgFileDict:
                demFilePath = dlImgFileDict[srcID]
                demFileName = os.path.basename(demFilePath)
                secondPart = demStatDict[srcID]

                g.write(f"\n{firstPart},{demFileName},{os.path.dirname(demFilePath)},{secondPart}")

            # srcID failed during the download process.  Pass since it will be accounted for in error file
            elif demInfo[headerValues.index("downld_url")] in failedDownloadList:
                continue

            else:
                AddMsgAndPrint(f"\n\t\tSourceID: {srcID} NO .TIF OR .IMG FOUND -- Inspect this process")

        g.close()
        return dlMasterFilePath

    except:
        # print("==================================")
        # print(f"\tSourceID: {srcID}")
        # print(f"\tdemInfo: {demInfo}")
        # print(f"\tHeaderValues: {headerValues}")
        errorMsg()
        return False

#### ===================================================================================
def getRasterInformation_MT(rasterItem):

    """ This function retrieves the following raster statistical information:
            rds_column,
            rds_rows,
            bandcount,
            cellsize,
            rdsformat,
            bitdepth,
            nodataval,
            srs_type,'
            epsg_code,
            srs_name,
            rds_top,
            rds_left,
            rds_right,
            rds_bottom,
            rds_min,
            rds_mean,
            rds_max,
            rds_stdev,
            blk_xsize,
            blk_ysize')

    It is invoked by the 'createMasterDBfile_MT' function.  The function takes in a tuple
    containing 2 values: (sourceID, raster path)
    ('60d2c0ddd34e840986528ae4', 'E:\\DSHub\\Elevation\\1M\\USGS_1M_19_x44y517_ME_CrownofMaine_2018_A18.tif')

    Return a dict (rasterStatDict) containing the following key,value
    '5eacfc1d82cefae35a250bec' = '10012,10012,1,1.0,GeoTIFF,Float32,-999999.0,PROJECTED,26919,NAD83 / UTM zone 19N,5150006.0,439994.0,450006.0,5139994.0,366.988,444.228,577.808,34.396,256,256'
    """

    try:
        srcID = rasterItem[0]
        raster = rasterItem[1]
        rasterStatDict = dict()  # temp dict that will return raster information
        rasterInfoList = list()

        gdal.SetConfigOption('GDAL_PAM_ENABLED', 'TRUE')
        gdal.UseExceptions()    # Enable exceptions

        # Raster doesn't exist; download error
        if not os.path.exists(raster):
            AddMsgAndPrint(f"\t\t{os.path.basename(raster)} DOES NOT EXIST. Could not get Raster Information")
            rasterStatDict[srcID] = ','.join('#'*20).replace('#','None')
            return rasterStatDict

        # Raster size is 0 bytes; download error
        if not os.stat(raster).st_size > 0:
            AddMsgAndPrint(f"\t\t{os.path.basename(raster)} Is EMPTY. Could not get Raster Information")
            rasterStatDict[srcID] = ','.join('#'*20).replace('#','None')
            return rasterStatDict

        rds = gdal.Open(raster)
        #rdsInfo = gdal.Info(rds,format="json",computeMinMax=True,stats=True,showMetadata=True)
        rdsInfo = gdal.Info(rds,format="json")
        bandInfo = rds.GetRasterBand(1)

        # ------------------------- Raster Properties -----------------------------
        columns = rdsInfo['size'][0]
        rows = rdsInfo['size'][1]
        bandCount = rds.RasterCount
        cellSize = rds.GetGeoTransform()[1]
        rdsFormat = rdsInfo['driverLongName']
        bitDepth = rdsInfo['bands'][0]['type']
    
        try:
            noDataVal = rdsInfo['bands'][0]['noDataValue']
        except:
            try:
                noDataVal = bandInfo.GetNoDataValue()
            except:
                noDataVal = 'None'
                
        for stat in (columns,rows,bandCount,cellSize,rdsFormat,bitDepth,noDataVal):
            rasterInfoList.append(stat)

        # -------------------- Raster Spatial Reference Information ------------------------
        # What is returned when a raster is undefined??
        prj = rds.GetProjection()  # GDAL returns projection in WKT
        srs = osr.SpatialReference(prj)

        # If no valid EPSG is found, an error will be thrown
        try:
            srs.AutoIdentifyEPSG()
            epsg = srs.GetAttrValue('AUTHORITY',1)
        except:
            epsg = 'None'

        # Returns 0 or 1; opposite would be IsGeographic
        if srs.IsProjected():
            srsType = 'PROJECTED'
            srsName = srs.GetAttrValue('projcs')
        else:
            srsType = 'GEOGRAPHIC'
            srsName = srs.GetAttrValue('geogcs')

        rasterInfoList.append(srsType)
        rasterInfoList.append(epsg)
        rasterInfoList.append(srsName.replace(',','-'))  # replace commas with dashes

        # -------------------- Coordinate Information ------------------------
        # 'lowerLeft': [439994.0, 5139994.0]

        right,top = rdsInfo['cornerCoordinates']['upperRight']   # Eastern-Northern most extent
        left,bottom = rdsInfo['cornerCoordinates']['lowerLeft']  # Western - Southern most extent
        rasterInfoList.append(top)
        rasterInfoList.append(left)
        rasterInfoList.append(right)
        rasterInfoList.append(bottom)

        # ---------------------- Raster Statistics ------------------------
        # ComputeStatistics vs. GetStatistics(0,1) vs. ComputeBandStats
        # bandInfo = rds.GetRasterBand(1).ComputeStatistics(0) VS.
        # bandInfo = rds.GetRasterBand(1).GetStatistics(0,1)
        # bandInfo = rds.GetRasterBand(1).ComputeBandStats
        # (Min, Max, Mean, StdDev)

        # Take stat info from JSON info above; This should work for 1M
        # May not work for 3M or 10M
        try:
            stats = bandInfo.GetStatistics(True, True)
        except:
            stats = bandInfo.ComputeStatistics(0)
        minStat = stats[0]
        maxStat = stats[1]
        meanStat = stats[2]
        stDevStat = stats[3]
        blockXsize = bandInfo.GetBlockSize()[0]
        blockYsize = bandInfo.GetBlockSize()[1]
        
        # stat info is included in JSON info but stats are not calculated;
        # calc statistics if min,max or mean are not greater than 0.0
        # this can add significant overhead to the process
        # if not minStat > noDataVal or not meanStat > noDataVal or not maxStat > noDataVal:
        #     AddMsgAndPrint(f"\t\t{os.path.basename(raster)} - Stats are not greater than {noDataVal} -- Calculating")
        #     bandStats = bandInfo.ComputeStatistics(0)
        #     minStat = bandStats[0]
        #     maxStat = bandStats[1]
        #     meanStat = bandStats[2]
        #     stDevStat = bandStats[3]
        #     blockXsize = bandInfo.GetBlockSize()[0]
        #     blockYsize = bandInfo.GetBlockSize()[1]

        for stat in (minStat,meanStat,maxStat,stDevStat,blockXsize,blockYsize):
            rasterInfoList.append(stat)

        # close raster dataset
        rds = None

        rasterStatDict[srcID] = ','.join(str(e) for e in rasterInfoList)
        return rasterStatDict

    except:
        AddMsgAndPrint(f"\t\tFailed: {os.path.basename(raster)}")
        errorMsg()

        # Return the info that was collected and pad the rest with 'None'
        if len(rasterInfoList) > 0:
            if len(rasterInfoList) < 20:
                while len(rasterInfoList) < 20:
                    rasterInfoList.append('None')
            rasterStatDict[srcID] = ','.join(str(e) for e in rasterInfoList)
        else:
            rasterStatDict[srcID] = ','.join('#'*20).replace('#','None')
        return rasterStatDict

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
        headers = open(masterElevFile).readline().rstrip().split(',')

        recCount = 0
        r2pgsqlFilePath = f"{os.path.dirname(downloadFile)}{os.sep}USGS_3DEP_{resolution}_Step2_RASTER2PGSQL.txt"

        # Recreates raster2pgsql file b/c metadata file is passed over; don't want to double up
        g = open(r2pgsqlFilePath,'w')

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
                srid = items[headers.index('epsg_code')]
                tileSize = '256x256'
                demPath = f"{items[headers.index('dem_path')]}{os.sep}{items[headers.index('dem_name')]}"
                dbName = 'elevation'
                dbTable = f"elevation_{resolution.lower()}"  # elevation_3m
                demName = items[headers.index('dem_name')]
                password = 'itsnotflat'
                localHost = '10.11.11.10'
                port = '6432'

                r2pgsqlCommand = f"raster2pgsql -s {srid} -b 1 -t {tileSize} -F -a -R {demPath} {dbName}.{dbTable} | PGPASSWORD={password} psql -U {dbName} -d {dbName} -h {localHost} -p {port}"

                # Add check to look for # in r2pgsqlCommand
                if r2pgsqlCommand.find('#') > -1 or r2pgsqlCommand.find('None') > -1:
                    invalidCommands.append(r2pgsqlCommand)

                if recCount == masterElevRecCount:
                    g.write(r2pgsqlCommand)
                else:
                    g.write(f"{r2pgsqlCommand}\n")

                print(f"\t\tSuccessfully wrote raster2pgsql command for {demName} -- ({recCount:,} of {total:,})")
                recCount+=1

        g.close()
        del masterElevRecCount

        # Inform user about invalid raster2pgsql commands so that they can be fixed.
        numOfInvalidCommands = len(invalidCommands)
        if numOfInvalidCommands:
            AddMsgAndPrint(f"\n\tThere are {numOfInvalidCommands:,} invalid raster2pgsql commands or that contain invalid parameters:")
            for invalidCmd in invalidCommands:
                AddMsgAndPrint(f"\t\t{invalidCmd}")

        return r2pgsqlFilePath

    except:
        errorMsg()

## ===================================================================================
def createErrorLogFile(downloadFile,failedDownloadList,headerValues):

    try:
        errorFile = f"{os.path.dirname(downloadFile)}{os.sep}USGS_3DEP_{resolution}_Step2_Download_FAILED.txt"

        # errorFile might be the downloadFile being used as a 2nd run to download failed
        # DEMs from the first run.  If so, create another errorFile with an increment at the end.
        i=2
        while os.path.exists(errorFile):
            errorFile = f"{os.path.dirname(downloadFile)}{os.sep}USGS_3DEP_{resolution}_Step2_Download_FAILED_{i}.txt"
            i+=1

        AddMsgAndPrint(f"\tDownload Errors Logged to: {errorFile}")
        g = open(errorFile,'w')

        lineNum = 0
        numOfErrors = 0
        with open(downloadFile, 'r') as fp:
            for line in fp:
                items = line.split(',')

                # Duplicate header from dlFile and write it to errorFile
                if bHeader and lineNum == 0:
                    g.write(line.strip())
                    lineNum +=1

                downloadURL = items[headerValues.index("downld_url")].strip()

                if downloadURL in failedDownloadList:
                    g.write("\n" + line.strip())
                    numOfErrors+=1

        g.close()

        # Not sure why
        if len(failedDownloadList) != numOfErrors:
            AddMsgAndPrint("\t\tNumber of errors logged don't coincide--????")

        return errorFile

    except:
        errorMsg()

## ====================================== Main Body ==================================
def main(dlFile,dlDir,bReplace):

    try:

        startTime = tic()
        global msgLogFile
        global bHeader
        global bReplaceData
        global downloadFile
        global elevMetadataDict
        global headerValues
        global resolution
        global dlFolder

        # 6 Tool Parameters
        downloadFile = dlFile         # Download File
        dlFolder = dlDir              # Download Directory
        bHeader = True
        bReplaceData = bReplace
        bUnzipFiles = True
        bDeleteZipFiles = False

        # Pull elevation resolution from file name
        # USGS_3DEP_5M_AK_DSM_Step1B_ElevationDL_07262023.txt --> 5M_AK_DSM
        tempList = downloadFile.split(os.sep)[-1].split('_')
        startPos = tempList.index(fnmatch.filter(tempList, '3DEP*')[0]) + 1
        endPos = tempList.index(fnmatch.filter(tempList, 'Step*')[0])
        resolution = '_'.join(tempList[startPos:endPos])
        #resolution = downloadFile.split(os.sep)[-1].split('_')[2]

        # ['polyCode', 'poly_name', 'prod_title','pub_date','last_updated','size','format'] ...etc
        headerValues = open(downloadFile).readline().rstrip().split(',')
        #try:
        #headerValues.remove('poly_name')
        # except:
        #     pass
        
        urlDownloadDict = dict()  # contains download URLs and sourceIDs grouped by poly_code; 07040006:[[ur1],[url2]]
        elevMetadataDict = dict() # contains all input info from input downloadFile.  sourceID:dlFile items
        recCount = 0
        badLines = 0

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

                poly_code = items[headerValues.index("poly_code")]
                poly_name = items[headerValues.index("poly_name")]
                prod_title = items[headerValues.index("prod_title")]
                pub_date = items[headerValues.index("pub_date")]
                last_updated = items[headerValues.index("lastupdate")]
                size = items[headerValues.index("rds_size")]
                fileFormat = items[headerValues.index("format")]
                sourceID = items[headerValues.index("sourceid")]
                metadata_url = items[headerValues.index("meta_url")]
                downloadURL = items[headerValues.index("downld_url")].strip()

                # Add info to urlDownloadDict
                if poly_code in urlDownloadDict:
                    urlDownloadDict[poly_code].append([downloadURL,sourceID,fileFormat])
                else:
                    urlDownloadDict[poly_code] = [[downloadURL,sourceID,fileFormat]]

                # Add info to elevMetadataDict
                elevMetadataDict[sourceID] = [poly_code,poly_name,prod_title,pub_date,last_updated,
                                              size,fileFormat,sourceID,metadata_url,downloadURL]
                recCount+=1

        # subtract header for accurate record count
        if bHeader: recCount = recCount -1

        """ ---------------------------- Establish Console LOG FILE ---------------------------------------------------"""
        today = datetime.today().strftime('%m%d%Y')

        # Log file that captures console messages
        #logFile = os.path.basename(downloadFile).split('.')[0] + "_Download_ConsoleMsgs.txt"
        msgLogFile = f"{os.path.dirname(downloadFile)}{os.sep}USGS_3DEP_{resolution}_Step2_Download_ConsoleMsgs.txt"

        h = open(msgLogFile,'a+')
        h.write(f"Executing: USGS_2_Download_Elevation_by_MetadataFile {today}\n\n")
        h.write("User Selected Parameters:\n")
        h.write(f"\tDownload File: {downloadFile}\n")
        h.write(f"\tFile has header: {bHeader}\n")
        h.write(f"\tReplace Data: {bReplaceData}\n")
        h.write(f"\tUnzip Files: {bUnzipFiles}\n")
        h.write(f"\tDelete Zip Files: {bDeleteZipFiles}\n")
        h.write(f"\tLog File Path: {msgLogFile}\n")
        h.close()

        AddMsgAndPrint(f"\n{'='*125}")
        AddMsgAndPrint((f"Total Number of files to download: {recCount:,}"))

        """ ----------------------------- DOWNLOAD ELEVATION DATA - Multi-threading mode ----------------------------- """
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

            AddMsgAndPrint(f"\nDownloading in Multi-threading Mode - # of Files: {recCount:,}")

            # 01:[URL,sourceID]
            for polycode,items in urlDownloadDict.items():
                numOfPolyElevFiles = len(items) # Number of elev files in this poly_code (State or HUC)

                AddMsgAndPrint(f"\n\tDownloading {numOfPolyElevFiles:,} elevation tiles for Code: {polycode}")

                with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:

                    # use a set comprehension to start all tasks.  This creates a future object
                    future_to_url = {executor.submit(DownloadElevationTile, item): item for item in items}

                    # yield future objects as they are done.
                    for future in as_completed(future_to_url):
                        dlTracker+=1
                        j=1

                        returnMsgs = future.result()
                        batchMsgs = list()

                        for printMessage in returnMsgs:
                            if j==1:
                                batchMsgs.append(f"{printMessage} -- ({dlTracker:,} of {recCount:,})")
                            else:
                                batchMsgs.append(printMessage)
                            j+=1

                        AddMsgAndPrint(None,msgList=batchMsgs)

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
                        unZipResults = {executor.submit(unzip, item): item for item in dlZipFileDict.items()}

                        # yield future objects as they are done.
                        for future in as_completed(unZipResults):
                            unzipTracker+=1
                            j=1

                            returnMsgs = future.result()
                            batchMsgs = list()

                            for printMessage in returnMsgs:
                                if j==1:
                                    batchMsgs.append(f"{printMessage} -- ({unzipTracker:,} of {len(dlZipFileDict):,})")
                                else:
                                    batchMsgs.append(printMessage)
                                j+=1

                            AddMsgAndPrint(None,msgList=batchMsgs)

                else:
                    AddMsgAndPrint("\nThere are no files to uzip")
                unzipStop = toc(unzipStart)

                if bDeleteZipFiles:
                    if len(dlZipFileDict):
                        AddMsgAndPrint(f"\nDeleting {len(dlZipFileDict)} Zip Files")
                        del_zipFiles(dlZipFileDict)

        else:
            print("\nThere are no elevation tiles to download")


        """ ----------------------------- Create Elevation Metadata File ----------------------------- """
        bMasterFile = False
        if len(dlImgFileDict):
            AddMsgAndPrint("\nCreating Elevation Metadata File")
            dlMasterFileStart = tic()
            dlMasterFile = createMasterDBfile_MT(dlImgFileDict,elevMetadataDict)
            AddMsgAndPrint(f"\n\tElevation Metadata File Path: {dlMasterFile}")
            dlMasterFileStop = toc(dlMasterFileStart)
            bMasterFile = True

            """ ----------------------------- Create Raster2pgsql File ---------------------------------- """
            if os.path.exists(dlMasterFile):
                AddMsgAndPrint("\nCreating Raster2pgsql File")
                r2pgsqlStart = tic()
                r2pgsqlFile = createRaster2pgSQLFile(dlMasterFile)
                AddMsgAndPrint(f"\n\tRaster2pgsql File Path: {r2pgsqlFile}")
                AddMsgAndPrint(f"\tIMPORTANT: Make sure dbTable variable (elevation_{resolution.lower()}) is correct in Raster2pgsql file!!")
                r2pgsqlStop = toc(r2pgsqlStart)
            else:
                AddMsgAndPrint("\nRaster2pgsql File will NOT be created")
        else:
            AddMsgAndPrint("\nNo information available to produce Master Database Elevation File")
            AddMsgAndPrint("\nNo information available to produce Raster2pgsql File")

        """ ------------------------------------ SUMMARY -------------------------------------------- """
        AddMsgAndPrint(f"\n{'-'*40}SUMMARY{'-'*40}")

        AddMsgAndPrint(f"\nTotal Processing Time: {toc(startTime)}")
        AddMsgAndPrint(f"\tDownload Time: {dlStop}")
        if len(dlZipFileDict) > 0:
                AddMsgAndPrint(f"\tUnzip Data Time: {unzipStop}")

        if bMasterFile:
            AddMsgAndPrint(f"\tCreate Master Elevation File Time: {dlMasterFileStop}")
            AddMsgAndPrint(f"\tCreate Raster2pgsql File Time: {r2pgsqlStop}")

        if totalDownloadSize > 0:
            AddMsgAndPrint(f"\nTotal Download Size: {convert_bytes(totalDownloadSize)}")

        # Report number of DEMs downloaded
        if len(dlImgFileDict) == recCount:
            AddMsgAndPrint(f"\nSuccessfully Downloaded ALL {len(dlImgFileDict):,} DEM files")
        elif len(dlImgFileDict) == 0:
            AddMsgAndPrint("\nNo DEM files were downloaded")
        else:
            AddMsgAndPrint(f"\nDownloaded {len(dlImgFileDict):,} out of {recCount:,} DEM files")

        # Create Download Error File
        if len(failedDownloadList):
            AddMsgAndPrint(f"\nFailed to Download {len(failedDownloadList):,} elevation files:")
            createErrorLogFile(downloadFile,failedDownloadList,headerValues)

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

    gdal.SetConfigOption('GDAL_PAM_ENABLED', 'TRUE')

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
        print("Please Enter Yes or No")
        bReplace = input("Do you want to replace existing data? (Yes/No): ")

    if bReplace.lower() in ("yes","y"):
        bReplace = True
    else:
        bReplace = False

##    dlFile = r'E:\GIS_Projects\DS_Hub\Elevation\DSHub_Elevation\USGS_Text_Files\1M\20230728_windows\USGS_3DEP_1M_Step1B_ElevationDL_08022023_test.txt'
##    dlFolder = r'F:\DSHub\Elevation\USGS_Elevation\1M'
##    bReplace = False

    main(dlFile,dlFolder,bReplace)
    input("\nHit Enter to Continue: ")