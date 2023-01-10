# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 10:12:13 2022
Updated on 1/6/2023

@author: Adolfo.Diaz

This is script #1 in the USGS Elevation workflow developed for the DS Hub.

The purpose of this script is to gather elevation metadata information from
the USGS API for an 8-digit watershed dataset.  The metadata text files produced by this
script will directly feed the 'Linux_Download_USGSElevation_by_MetadataFile_PostgisDirStructure'
script.

Parameters:
    1) huc8Boundaries - path to HUC 8 Boundary Dataset feature class
    2) metadataPath - path to where all text files will be written to
    3) tnmResolution - Elevation resolution (1M, 3M, 10M, 30M, 5M_AK, 60M_AK)
    4) DOWNLOAD ELEVATION DATA PARAMETERS -- REMOVE THESE

- This script takes in a HUC-8 feature class along with the USGS Elevation API
  'https://tnmaccess.nationalmap.gov/api/v1/products?' to retrieve metadata information about
  the elevation tiles that intersect these watersheds.
- Watershed feature class must contain 2 fields: 'HUC8' & 'Name'.  It is best to download
  WBD_National_GDB.gdb FGDB directly from USGS.  You can only use HUC2, HUC4 or HUC8 with the USGS API.
  You can use other polygons.  For more info: https://apps.nationalmap.gov/tnmaccess/#/product
- The script tracks elevation files that overlap multiple watersheds to avoid logging duplicate elevation
  files.  The USGS sourceID is used to track duplicates.
- All messages are logged in a text file called USGS_3DEP_3M_Metadata_ConsoleMsgs_12132022.txt

Metadata information is organized into 2 text files:
    1) USGS_3DEP_3M_Metadata_API_12132022.txt
    2) USGS_3DEP_3M_Metadata_Elevation_12132022.txt

Text file #1 - USGS_3DEP_3M_Metadata_API_12132022.txt
This file contains the following information:
    1. HUC 8 Digit (07040006)
    2. HUC 8 Name (La Crosse-Pine)
    3. Number of elevation tiles that intersect watershed (14)
    4. USGS API URL to elevation metadata information for above tiles in JSON format

Text file #2 - USGS_3DEP_3M_Metadata_Elevation_12132022.txt
This file directly feeds the 'Linux_Download_USGSElevation_by_MetadataFile_PostgisDirStructure' script.
This file contains the following information:
    1. HUC 8 Digit (07040006)
    2. Product Title (USGS NED ned19_n43x75_w091x00_wi_lacrosseco_2008 1/9 arc-second 2009 15 x 15 minute IMG)
    3. Publication Date (2009-01-01)
    4. Last Updated (2020-12-16)
    5. Size in bytes (7572953)
    6. File Format (IMG)
    7. USGS unique identifier Source ID (581d2d68e4b08da350d665a5)
    8. URL to Metadata (https://www.sciencebase.gov/catalog/item/581d2d68e4b08da350d665a5)
    9. URL to download elevation tile (https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n43x75_w091x00_wi_lacrosseco_2008.zip)

When converted to a table, these 2 files can be related using the HUC 8 Digit field to establish a one-to-many
relationship from table1 to table2 to relate a watershed to its corresponding elevation files.

3DEP Elevation Identify service:
https://elevation.nationalmap.gov/arcgis/rest/services/3DEPElevation/ImageServer/identify
use: -10075221.0661,5413262.007700004 coordinates to test
Coordinates are in 3857 Web Mercator.  OLD WKID: 102100

r'https://tnmaccess.nationalmap.gov/api/v1/products?datasets=National%20Elevation%20Dataset%20(NED)%201/9%20arc-second&polyCode=07060002&polyType=huc8&max=2'

Things to consider/do:
    2) Need to incorporate GDAL to make this script arcpy independent
        - Will this script ever be executed from within Linux?  If so, this is a priority
    3) Remove donwload capability and let 'Linux_Download_USGSElevation_by_MetadataFile_PostgisDirStructure'
       script handle all downlaoding functionality.
    4) Embed multi-threading capability for watershed elevations
    5) Investigate the situation when the total number of tiles for a watershed are neither unique nor duplicate.
    6) Write directly to the Postgres database
        - Determine naming convetion
        - Is the master table updated or overwritten.
        - If overwritten, does the previous table get archived?
        - How will these 2 tables be used?
    7) Need to develop a master polygon footprint of elevation extent and associate it with 2nd metadata file.
        - Bounding box is probably good enough
"""
## ===================================================================================
def AddMsgAndPrint(msg):

    # Print message to python message console
    print(msg)

    # Add message to log file
    try:
        h = open(logFilePath,'a+')
        h.write("\n" + msg)
        h.close
        del h
    except:
        pass


## ===================================================================================
def errorMsg(errorOption=1):
    """ Capture Traceback error messages """

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
def DownloadElevationTile(hucURL):
    # Description
    # This function will open a URL, read the lines and send back the response.
    # It is used within the ThreadPoolExecutor to send multiple NASIS server
    # requests.  The primary URL passed to this function from this script will be:
    # https://nasis.sc.egov.usda.gov/NasisReportsWebSite/limsreport.aspx?report_name=WEB_AnalysisPC_MAIN_URL_EXPORT&pedonid_list=14542
    # This function also replaces the 'getPedonHorizon' function that not only opened
    # the URL but also organized the contents into a dictionary that followed the NASIS schema.
    # The function of organizing the URL content is now handled by the 'organizeFutureInstance' function

    # Parameters
    # url - the url that connection will be establised to and whose contents will be returned.
    # 1 global variable will be updated within this function.

    # Returns
    # This function returns the contents of a URL.  However, within this script, the openURL
    # function is being called within the ThreadPoolExecutor asynchronous callables which returns
    # a "future" object representing the execution of the callable.

    try:
        messageList = list()
        theTab = "\t\t"

        # 'https://websoilsurvey.sc.egov.usda.gov/DSD/Download/Cache/SSA/wss_SSA_WI021_[2021-09-07].zip'
        fileName = hucURL.split('/')[-1]

        # set the download's output location and filename
        local_file = f"{downloadFolder}\\{fileName}"

        # make sure the output zip file doesn't already exist
        if os.path.isfile(local_file):
            if bReplaceData:
                try:
                    os.remove(local_file)
                    messageList.append(f"{theTab}{'File Exists; Deleted':<35} {fileName:<60}")
                except:
                    messageList.append(f"{theTab}{'File Exists; Failed to Delete':<35} {fileName:<60}")
                    failedDownloadList.append(hucURL)
                    return messageList
            else:
                messageList.append(f"{theTab}{'Already Exists, Skipping:':<35} {fileName:<60} {convert_bytes(os.stat(local_file).st_size):>15}")
                return messageList
                pass

        # Open request to Web Soil Survey for that zip file
        request = urlopen(hucURL)

        # save the download file to the specified folder
        output = open(local_file, "wb")
        output.write(request.read())
        output.close()

        messageList.append(f"{theTab}{'Successfully Downloaded:':<35} {fileName:<60} {convert_bytes(os.stat(local_file).st_size):>15}")
        #messageList.append(f"{theTab}{'Successfully Downloaded:':<30} {fileName:<55} {convert_bytes(os.stat(local_file).st_size):>15}")
        del request, output
        return messageList

    except URLError as e:
        messageList.append(f"{theTab}{e.__dict__} -- {hucURL}")
        #messageList.append(f"{theTab}URL ERROR: {e:<35} {fileName:<60}")
        failedDownloadList.append(hucURL)
        return messageList

    except HTTPError as e:
        messageList.append(f"{theTab}{e.__dict__} -- {hucURL}")
        #messageList.append(f"{theTab}HTTP ERROR: {e:<35} {fileName:<60}")
        failedDownloadList.append(hucURL)
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
        messageList.append(f"{theTab}{'Unexpected Error:'<35} {fileName:<60} -- {hucURL}")
        failedDownloadList.append(hucURL)
        messageList.append(f"{theTab}{errorMsg(errorOption=2)}")
        return messageList

## ===================================================================================
def unzip(local_zip,bDeleteZipFiles):
    # Given zip file name, try to unzip it

    try:
        messageList = list()

        if bDeleteZipFiles: leftAlign = 40
        else:               leftAlign = 30

        if zipfile.is_zipfile(local_zip):

            zipSize = os.stat(local_zip).st_size  # size in bytes
            zipName = local_zip.split('\\')[-1]
            wcName = zipName.split('.')[0]

            if zipSize > 0:

                try:
                    with zipfile.ZipFile(local_zip, "r") as z:
                        # a bad zip file returns exception zipfile.BadZipFile
                        z.extractall(downloadFolder)
                    del z

                    unzipTally = 0
                    for file in glob.glob(f"{downloadFolder}//{wcName}*"):
                        if file.endswith('.zip'):continue
                        size = os.stat(file).st_size
                        unzipTally+=size

                    messageList.append(f"\t{'Successfully Unzipped:':<{leftAlign}} {zipName:<60} {convert_bytes(unzipTally):>15}")
                    del unzipTally

                except:
                    messageList.append(f"\t{'Failed to unzip:':<{leftAlign}} {zipName:<60} {convert_bytes(zipSize):>15}")
                    errorMsg()

                # remove zip file after it has been extracted,
                # allowing a little extra time for file lock to clear
                if bDeleteZipFiles:
                    try:
                        os.remove(local_zip)
                        messageList.append(f"\t\t{'Successfully deleted zip File':<{leftAlign}}")
                    except:
                        messageList.append(f"\t\t{'Failed to delete zip file':<{leftAlign}}")

                return messageList

            else:
                messageList.append(f"\t{'Empty Zipfile:':<{leftAlign}} {zipName:<60} {convert_bytes(zipSize):>15}")
                #os.remove(local_zip)
                return messageList

        else:
            # Don't have a zip file, need to find out circumstances and document
            messageList.append(f"\t{'Invalid Zipfile:':<{leftAlign}} {zipName:<60}")
            return messageList

    except zipfile.BadZipfile:
        messageList.append(f"\t{'Corrupt Zipfile:':<{leftAlign}} {zipName:<60}")
        return messageList

    except:
        messageList.append(f"\tError with {zipName} -- {errorMsg(errorOption=2)}")
        return messageList

## ====================================== Main Body ==================================
# Import modules
import sys, string, os, traceback, glob
import urllib, re, time, json, socket, zipfile
import arcgisscripting, arcpy, threading, multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime

from urllib.request import Request, urlopen, URLError
from urllib.error import HTTPError

urllibEncode = urllib.parse.urlencode

if __name__ == '__main__':

    try:
        # Start the clock
        startTime = tic()

        # 9 Tool Parameters
        #huc8Boundaries = r'E:\GIS_Projects\DS_Hub\hydrologic_units\HUC12.gdb\WBDHU8'
        huc8Boundaries = r'E:\GIS_Projects\DS_Hub\hydrologic_units\HUC12.gdb\WBD_TEST4'
        metadataPath = r'E:\python_scripts\DSHub\LinuxWorkflow\TEMP_Update_DL_StatusFile\TEMP_testingFiles'
        tnmResolution = '3M'

        bDownloadData = False
        downloadFolder = r'E:\python_scripts\DSHub\LinuxWorkflow\TEMP_Update_DL_StatusFile\Junk\3M'
        bDownloadMultithread = True
        bReplaceData = True
        bUnzipFiles = True
        bDeleteZipFiles = True

        # Constant variables
        totalWatersheds = int(arcpy.GetCount_management(huc8Boundaries)[0])
        tnmAPIurl = r'https://tnmaccess.nationalmap.gov/api/v1/products?'

        tnmProductAlias = {'1M':'Digital Elevation Model (DEM) 1 meter',
                           '3M':'National Elevation Dataset (NED) 1/9 arc-second',
                           '10M':'National Elevation Dataset (NED) 1/3 arc-second',
                           '30M': 'National Elevation Dataset (NED) 1 arc-second',
                           '5M_AK':'Alaska IFSAR 5 meter DEM',
                           '60M_AK':'National Elevation Dataset (NED) Alaska 2 arc-second'}

        """ ---------------------------- Establish Metdata and log FILES ---------------------------------------------------"""
        today = datetime.today().strftime('%m%d%Y')  #11192022

        # Metadata file#1:
        # contains huc8-digit, Number of tiles associated with huc8, URL for TNM API
        metadataFile1 = f"USGS_3DEP_{tnmResolution}_Metadata_API_{today}.txt"
        metadataFile1path = f"{metadataPath}\\{metadataFile1}"
        f = open(metadataFile1path,'a+')
        f.write(f"huc8_digit,huc8_name,num_of_tiles,API_URL") # log headers

        # Metadata file#2:
        # contains huc8_digit,prod_title,pub_date,last_updated,size,format,sourceID,metadata_url,download_url
        metadataFile2 = f"USGS_3DEP_{tnmResolution}_Metadata_Elevation_{today}.txt"
        metadataFile2path =  f"{metadataPath}\\{metadataFile2}"
        g = open(metadataFile2path,'a+')
        g.write(f"huc8_digit,prod_title,pub_date,last_updated,size,format,sourceID,metadata_url,download_url") # log headers

        # Log file that captures console messages
        logFile = f"USGS_3DEP_{tnmResolution}_Metadata_ConsoleMsgs_{today}.txt"
        logFilePath = f"{metadataPath}\\{logFile}"
        h = open(logFilePath,'a+')
        h.write(f"Executing: Create_USGS_API_URL_Reports_multiThreading Script {today}\n\n")
        h.write(f"User Selected Parameters:\n")
        h.write(f"\tTNM Product: {tnmProductAlias[tnmResolution]}\n")
        h.write(f"\tHUC 8 Boundary Dataset: {huc8Boundaries}\n")
        h.write(f"\tMetadata File Path: {metadataPath}\n")

        if bDownloadData:
            h.write(f"\tDownload Folder: {downloadFolder}\n")
            h.write(f"\tDownload Data Boolean: {bDownloadData}\n")
            h.write(f"\tDownload Multithread: {bDownloadMultithread}\n")
            h.write(f"\tReplace Data: {bReplaceData}\n")
            h.write(f"\tUnzip Files: {bUnzipFiles}\n")
            h.write(f"\tDelete Zip Files: {bDeleteZipFiles}\n")
        h.close()

        badAPIurls = dict()
        emptyHUCs = list()
        uniqueTileList = list()   # list of sourceIDs that are duplicated
        unaccountedHUCs = list()
        urlDownloadDict = dict()  # created only for single download mode
        numOfTotalTiles = 0
        numOfQualifiedTiles = 0
        totalSizeBytes = 0

        AddMsgAndPrint(f"\nCOMPILING USGS API URL REQUESTS FOR {totalWatersheds:,} HUC-8 Watershed(s)")

        # Iterate through every watershed to gather elevation info
        # create dictionary of Watershed digits (keys) and API request (values)
        for row in arcpy.da.SearchCursor(huc8Boundaries, ['HUC8','Name'], sql_clause=(None, 'ORDER BY HUC8 ASC')):

            huc8digit = row[0]
            huc8name = row[1]

            AddMsgAndPrint(f"\t8-digit HUC {huc8digit}: {huc8name}")

            # Formulate API rquest
            params = urllibEncode({'f': 'json',
                                   'datasets': tnmProductAlias[tnmResolution],
                                   'polyType': 'huc8',
                                   'polyCode': huc8digit,
                                   'max':150})

            # concatenate URL and API parameters
            tnmURLhuc8 = f"{tnmAPIurl}{params}"

            # Send REST API request
            try:
                with urllib.request.urlopen(tnmURLhuc8) as conn:
                    resp = conn.read()

                results = json.loads(resp)
            except:
                badAPIurls[huc8digit] = tnmURLhuc8
                AddMsgAndPrint(f"\t\tBad Request")
                f.write(f"\n{huc8digit},{huc8name},-999,{tnmURLhuc8}")
                continue

            i = 0  # Counter for qualified tiles within a unique HUC
            j = 0  # Counter for duplicate tiles within a unique HUC

            if 'errorMessage' in results:
                AddMsgAndPrint(f"\t\tError Message from server: {results['errorMessage']}")

            elif results['errors']:
                AddMsgAndPrint(f"\t\tError Message in results: {results['errors']}")

            # JSON results
            elif 'total' in results:
                if results['total'] > 0:

                    totalNumOfTiles = results['total']
                    numOfTotalTiles += totalNumOfTiles
                    AddMsgAndPrint(f"\t\tNumber of {tnmResolution} elevation tiles: {totalNumOfTiles}")
                    f.write(f"\n{huc8digit},{huc8name},{totalNumOfTiles},{tnmURLhuc8}")

                    # collect info from unique elevation files
                    for itemInfo in results['items']:
                        sourceID = itemInfo['sourceId']
                        title = itemInfo['title']

                        # use the sourceID to avoid duplicate tiles
                        if not sourceID in uniqueTileList:
                            uniqueTileList.append(sourceID)
                        else:
                            #AddMsgAndPrint(f"\t\t\tTile already exists {huc8digit} -- {sourceID}")
                            j+=1
                            continue

                        pubDate = itemInfo['publicationDate']
                        lastModified = itemInfo['modificationInfo']
                        size = itemInfo['sizeInBytes']
                        fileFormat = itemInfo['format']
                        metadataURL = itemInfo['metaUrl']
                        downloadURL = itemInfo['downloadURL']

                        if bDownloadData:

                            if huc8digit in urlDownloadDict:
                                urlDownloadDict[huc8digit].append(downloadURL)
                            else:
                                urlDownloadDict[huc8digit] = [downloadURL]

                        # Ran into a situation where size was incorrectly populated as none
                        # https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/NH_CT_RiverNorthL6_P2_2015/TIFF/USGS_one_meter_x31y491_NH_CT_RiverNorthL6_P2_2015.tif
                        if size: totalSizeBytes += size
                        else: size = 0

                        g.write(f"\n{huc8digit},{title},{pubDate},{lastModified},{size},{fileFormat},{sourceID},{metadataURL},{downloadURL}")
                        numOfQualifiedTiles += 1
                        i+=1

                    # total number of tiles for this watershed are neither unique nor duplicate.
                    if totalNumOfTiles != (i+j):
                        AddMsgAndPrint(f"\t\t\tThere are {totalNumOfTiles - (i+j)} tiles that are not duplicate or unique")

                else:
                    AddMsgAndPrint(f"\t\tThere are NO {tnmResolution} elevation tiles")
                    f.write(f"\n{huc8digit},{huc8name},0,{tnmURLhuc8}")
                    emptyHUCs.append(huc8digit)

            else:
                AddMsgAndPrint(f"\t\tUnaccounted Scenario - HUC {huc8digit}: {huc8name}")
                unaccountedHUCs.append(huc8digit)

        f.close()
        g.close()

        """ ----------------------------- DOWNLOAD ELEVATION DATA ----------------------------- """
        if bDownloadData:

            if len(urlDownloadDict) > 0:

                dlStart = tic()
                failedDownloadList = list()

                if bDownloadMultithread:
                    AddMsgAndPrint(f"\nDOWNLOADING '{tnmProductAlias[tnmResolution]}' -  Multithreading Mode - # of Tiles: {numOfQualifiedTiles:,}")
                else:
                    AddMsgAndPrint(f"\nDOWNLOADING '{tnmProductAlias[tnmResolution]}' - Single Request Mode - # of Tiles: {numOfQualifiedTiles:,}")

                for huc,urls in urlDownloadDict.items():
                    i = 1
                    numOfHUCelevTiles = len(urls)
                    AddMsgAndPrint(f"\n\tDownloading {numOfHUCelevTiles} elevation tiles for HUC: {huc}")


                    if bDownloadMultithread:
                        with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:

                            # use a set comprehension to start all tasks.  This creates a future object
                            future_to_url = {executor.submit(DownloadElevationTile, url): url for url in urls}

                            # yield future objects as they are done.
                            for future in as_completed(future_to_url):
                                for printMessage in future.result():
                                    AddMsgAndPrint(printMessage)

                    else:
                        for url in urls:
                            #AddMsgAndPrint(f"\t\tDownloading elevation tile: {url.split('/')[-1]} -- {i} of {numOfHUCelevTiles}")
                            downloadMsgs = DownloadElevationTile(url)

                            for msg in downloadMsgs:
                                AddMsgAndPrint(msg)
                            i+=1
                dlStop = toc(dlStart)

                """ ----------------------------- UNZIP ELEVATION DATA ----------------------------- """
                if bUnzipFiles:

                    unzipStart = tic()
                    zipFileList = glob.glob(f"{downloadFolder}//*.zip")

                    if len(zipFileList) > 0:

                        AddMsgAndPrint(f"\nUnzipping {len(zipFileList)} Elevation Files")

                        with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
                            unZipResults = {executor.submit(unzip, zipFile, bDeleteZipFiles): zipFile for zipFile in zipFileList}

                            # yield future objects as they are done.
                            for msg in as_completed(unZipResults):
                                for printMessage in msg.result():
                                    AddMsgAndPrint(printMessage)

                    else:
                        AddMsgAndPrint(f"There are no files to uzip")
                    unzipStop = toc(unzipStart)

            else:
                f("\nThere are no elevation tiles to download")

        if unaccountedHUCs:
            AddMsgAndPrint(f"\nThere are {len(unaccountedHUCs):,} HUCs that are completely unaccounted for:")
            AddMsgAndPrint(f"{str(unaccountedHUCs)}")

        AddMsgAndPrint(f"\n{'-'*40}SUMMARY{'-'*40}")
        if numOfTotalTiles > numOfQualifiedTiles:
            AddMsgAndPrint(f"Total # of Watersheds: {totalWatersheds:,}")
            if len(emptyHUCs):
                AddMsgAndPrint(f"Total # of Watersheds w/ NO Data: {len(emptyHUCs):,}")

            AddMsgAndPrint(f"\nTotal # of Files: {numOfTotalTiles:,}")
            AddMsgAndPrint(f"Total Duplicate Files: {(numOfTotalTiles - numOfQualifiedTiles):,}")
            AddMsgAndPrint(f"Total Unique Files: {numOfQualifiedTiles:,}")
            AddMsgAndPrint(f"\nTotal Download Size: {convert_bytes(totalSizeBytes)}")

        else:
            AddMsgAndPrint(f"Total # of Files: {numOfQualifiedTiles:,}")
            AddMsgAndPrint(f"Total Download Size: {convert_bytes(totalSizeBytes)}")

        if len(badAPIurls) > 0:
            AddMsgAndPrint(f"\nTotal # of Bad API Requests: {len(badAPIurls):,}")
            AddMsgAndPrint("\tThese requests will have a -999 in num_of_tiles column")

        if bDownloadData:
            AddMsgAndPrint(f"\nTotal Download Time: {dlStop}")

            if len(failedDownloadList):
                AddMsgAndPrint(f"\n\tFailed to download {len(failedDownloadList)} elevation files)")

            if bUnzipFiles:
                if len(zipFileList) > 0:
                    AddMsgAndPrint(f"\n\tTotal Time to unzip data {unzipStop}")

                    try:
                        totalUnzipSize = sum([os.stat(x).st_size for x in glob.glob(f"{downloadFolder}\\*") if not x.endswith(".zip")])
                        AddMsgAndPrint(f"\tTotal Data Size: {convert_bytes(totalUnzipSize)}")
                    except:
                        pass

        AddMsgAndPrint(f"\nTotal Processing Time: {toc(startTime)}")

    except:
        try:
            f.close()
            g.close()
        except:
            pass
        errorMsg()




