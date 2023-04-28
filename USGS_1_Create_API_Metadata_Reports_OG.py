# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 10:12:13 2022
Updated on 2/8/2023

@author: Adolfo.Diaz
Geospatial Business Analyst
USDA - NRCS - SPSD - Soil Services and Information
email address: adolfo.diaz@usda.gov
cell: 608.215.7291

This is script #1 in the USGS Elevation workflow developed for the DS Hub.

The purpose of this script is to gather elevation metadata information from
the USGS API for an 8-digit watershed boundary.  The CSV metadata text file produced by this
script will be the input for the USGS_2_Download_by_MetadataFile.py script, the 2nd script
in the USGS elevation workflow.  

Parameters:
    1) hucBoundaries - path to HUC 8 Boundary Dataset feature class
    2) metadataPath - path to where all text files will be written to
    3) tnmResolution - Elevation resolution (1M, 3M, 10M, 30M, 5M_AK, 60M_AK)
    4) DOWNLOAD ELEVATION DATA PARAMETERS -- REMOVE THESE

- This script takes in a HUC feature class along with the USGS Elevation API
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
    8) Embed option to write to a FGDB or SQLite database
    9) instead of writing to g.write, write the outputs to a list and then write results to the file.
       This is safer in case the script crashes.

---------------- UPDATES
2/8/2023
Modified code to incorporate capability for HUC4 Watersheds to use for ALASKA 10M and 5M IFSAR
- Add validation for hucboundary path
- Add validation for hucBoundary fields (hucCodeFld,hucNameFld)
- Modify code to be huc digit independent.  Should work with any digit code (2,4,8)
- Add check to verify that digit length is only 2,4 or 8 since the API only handles those
- Started to remove code for downloading elevation data since this is handled elsewhere
- Figured out why there are unaccountable elevation files.  It had to do with maxProdsPerPage

2/15/2023
Uncovered a flaw in the methodology.  Increased the number of products per page to 1000 b/c
many DEMs per watershed were be left out if the products per page was left too low.  i.e.
if maxProdsPerPage was set to 75 and a watershed had 100 DEMs associated with that watershed (According
to the USGS API) then 25 were left out and a message like 'There are 25 tiles that are not
duplicate or unique' would be written out.  Set the maxProdsPerPage to 1000.

Updated the # of elevation tiles logged to metadata file#1.  The total number of DEMs returned from
the API was being logged.  This includes duplicates.  Replaced it with counter i.

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
def fieldExists(dataset: str, field_name: str) -> bool:
    """Return boolean indicating if field exists in the specified dataset."""

    return field_name in [field.name for field in arcpy.ListFields(dataset)]

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
        hucBoundaries = r'E:\GIS_Projects\DS_Hub\hydrologic_units\HUC12.gdb\WBDHU8'
        hucCodeFld = 'huc8'
        hucNameFld = 'name'
        metadataPath = r'E:\DSHub\Elevation'
        tnmResolution = '1M'
        bAlaska = True

        if not arcpy.Exists(hucBoundaries):
            AddMsgAndPrint(f"\n{hucBoundaries} does NOT exist! EXITING!")
            exit()

        if not fieldExists(hucBoundaries,hucCodeFld):
            AddMsgAndPrint(f"\n{hucCodeFld} does NOT exist! EXITING!")
            exit()

        if not fieldExists(hucBoundaries,hucNameFld):
            AddMsgAndPrint(f"\n{hucNameFld} does NOT exist! EXITING!")
            exit()

        # Constant variables
        totalWatersheds = int(arcpy.GetCount_management(hucBoundaries)[0])
        tnmAPIurl = r'https://tnmaccess.nationalmap.gov/api/v1/products?'

        tnmProductAlias = {'1M':'Digital Elevation Model (DEM) 1 meter',
                           '3M':'National Elevation Dataset (NED) 1/9 arc-second',
                           '10M':'National Elevation Dataset (NED) 1/3 arc-second',
                           '30M': 'National Elevation Dataset (NED) 1 arc-second',
                           '5M_AK':'Alaska IFSAR 5 meter DEM',
                           '60M_AK':'National Elevation Dataset (NED) Alaska 2 arc-second'}

        maxProdsPerPage = 1000

        """ ---------------------------- Establish Metdata and log FILES ---------------------------------------------------"""
        today = datetime.today().strftime('%m%d%Y')  #11192022

        # Metadata file#1; USGS_3DEP_1M_Metadata_API_02082023.txt
        # huc_digit, Number of tiles associated with huc, URL for TNM API
        if bAlaska and not tnmResolution == '5M_AK':
            metadataFile1 = f"USGS_3DEP_{tnmResolution}_AK_Metadata_API_{today}.txt"
        else:
            metadataFile1 = f"USGS_3DEP_{tnmResolution}_Metadata_API_{today}.txt"

        metadataFile1path = f"{metadataPath}\\{metadataFile1}"
        f = open(metadataFile1path,'a+')
        f.write(f"huc_digit,huc_name,num_of_tiles,API_URL") # log headers

        # Metadata file#2; USGS_3DEP_1M_Metadata_Elevation_02082023.txt
        # huc_digit,prod_title,pub_date,last_updated,size,format,sourceID,metadata_url,download_url
        if bAlaska and not tnmResolution == '5M_AK':
            metadataFile2 = f"USGS_3DEP_{tnmResolution}_AK_Metadata_Elevation_{today}.txt"
        else:
            metadataFile2 = f"USGS_3DEP_{tnmResolution}_Metadata_Elevation_{today}.txt"
        metadataFile2path =  f"{metadataPath}\\{metadataFile2}"
        g = open(metadataFile2path,'a+')
        g.write(f"huc_digit,prod_title,pub_date,last_updated,size,format,sourceID,metadata_url,download_url") # log headers

        # Log file that captures console messages
        logFile = f"USGS_3DEP_{tnmResolution}_Metadata_API_ConsoleMsgs_{today}.txt"
        logFilePath = f"{metadataPath}\\{logFile}"
        h = open(logFilePath,'a+')
        h.write(f"Executing: USGS_1_Create_API_Metadata Script {today}\n\n")
        h.write(f"User Selected Parameters:\n")
        h.write(f"\tTNM Product: {tnmProductAlias[tnmResolution]}\n")
        h.write(f"\tHUC Boundary Dataset: {hucBoundaries}\n")
        h.write(f"\tMetadata File Path: {metadataPath}\n")
        h.close()

        badAPIurls = dict()
        emptyHUCs = list()            # Watershed with NO DEMS associated to it
        unaccountedHUCs = list()      # Not a Bad url, not an empty watershed, unaccounted for result
        numOfTotalTiles = 0           # Total of all DEMS from watersheds; including overalp DEMs
        numOfUniqueDEMs = 0           # Number of unique DEMs
        totalSizeBytes = 0

        elevMetadataDict = dict()     # info that will be written to Metadata elevation file - sourceID:huc,title,date...etc
        uniqueSourceIDList = list()   # list of unique sourceIDs
        uniqueURLs = list()
        uniqueTitle = list()

        AddMsgAndPrint(f"\nCOMPILING USGS API URL REQUESTS FOR {totalWatersheds:,} Watershed(s) -- {tnmProductAlias[tnmResolution]}")

        # Iterate through every watershed to gather elevation info
        # create dictionary of Watershed digits (keys) and API request (values)
        orderByClause = f"ORDER BY {hucCodeFld} ASC"
        for row in arcpy.da.SearchCursor(hucBoundaries, [hucCodeFld,hucNameFld], sql_clause=(None, orderByClause)):

            hucDigit = row[0]
            hucName = row[1]

            # Need a valid hucDigit code; must be integer and not character
            if hucDigit in (None,' ','NULL','Null') or not hucDigit.isdigit():
                AddMsgAndPrint(f"\t{hucDigit} value is invalid.  Skipping Record")
                continue

            # USGS API only handles 2,4,8 huc-digits
            hucLength = len(hucDigit)
            if not hucLength in (2,4,8):
                AddMsgAndPrint(f"\tUSGS API only handles HUC digits 2, 4 and 8. Not {hucLength}-digit")
                continue

            AddMsgAndPrint(f"\t{hucLength}-digit HUC {hucDigit}: {hucName}")

            # Formulate API rquest
            params = urllibEncode({'f': 'json',
                                   'datasets': tnmProductAlias[tnmResolution],
                                   'polyType': f"huc{hucLength}",
                                   'polyCode': hucDigit,
                                   'max':maxProdsPerPage})

            # concatenate URL and API parameters
            tnmURLhuc = f"{tnmAPIurl}{params}"

            # Send REST API request
            try:
                with urllib.request.urlopen(tnmURLhuc) as conn:
                    resp = conn.read()
                results = json.loads(resp)
            except:
                badAPIurls[hucDigit] = tnmURLhuc
                AddMsgAndPrint(f"\t\tBad Request")
                f.write(f"\n{hucDigit},{hucName},-999,{tnmURLhuc}")
                continue

            i = 0  # Counter for unique DEMs within a unique HUC
            j = 0  # Counter for duplicate DEM that exists in a different HUC

            if 'errorMessage' in results:
                AddMsgAndPrint(f"\t\tError Message from server: {results['errorMessage']}")

            elif results['errors']:
                AddMsgAndPrint(f"\t\tError Message in results: {results['errors']}")

            # JSON results
            elif 'total' in results:
                if results['total'] > 0:

                    # the # of DEMs associated with query
                    totalNumOfTiles = results['total']
                    numOfTotalTiles += totalNumOfTiles

                    # collect info from unique elevation files
                    for itemInfo in results['items']:

                        sourceID = itemInfo['sourceId']
                        title = itemInfo['title']
                        pubDate = itemInfo['publicationDate']
                        lastModified = itemInfo['modificationInfo']
                        size = itemInfo['sizeInBytes']
                        fileFormat = itemInfo['format']
                        metadataURL = itemInfo['metaUrl']
                        downloadURL = itemInfo['downloadURL']

                        # --------------- Check for duplicate DEMs
                        # check for duplicate sourceIDs in DEM
                        if sourceID in elevMetadataDict:
                        #if sourceID in uniqueSourceIDlist:
                            AddMsgAndPrint(f"\t\tDEM sourceID {sourceID} already exists")
                            j+=1
                            continue
                        else:
                            uniqueSourceIDlist.append(sourceID)

                        # check for duplicate download URLs in DEM
                        if downloadURL in uniqueURLs:
                            AddMsgAndPrint(f"\t\tDEM download URL: {downloadURL} already exists")
                            j+=1
                            continue
                        else:
                            uniqueURLs.append(downloadURL)

                        # check for duplicate product titles;
                        if title in uniqueTitle:
                            AddMsgAndPrint(f"\t\tDEM Title: {title} already exists")
                            j+=1
                            continue
                        else:
                            uniqueTitle.append(title)

                        # Ran into a situation where size was incorrectly populated as none
                        # https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/NH_CT_RiverNorthL6_P2_2015/TIFF/USGS_one_meter_x31y491_NH_CT_RiverNorthL6_P2_2015.tif
                        if size: totalSizeBytes += size
                        else: size = 0

                        elevMetadataDict[sourceID] = [hucDigit,title,pubDate,lastModified,size,fileFormat,sourceID,metadataURL,downloadURL]
                        #g.write(f"\n{hucDigit},{title},{pubDate},{lastModified},{size},{fileFormat},{sourceID},{metadataURL},{downloadURL}")
                        numOfUniqueDEMs += 1
                        i+=1

                    AddMsgAndPrint(f"\t\tNumber of {tnmResolution} elevation tiles: {i}")
                    f.write(f"\n{hucDigit},{hucName},{i},{tnmURLhuc}")

                    # total number of tiles for this watershed are neither unique nor duplicate.
                    if totalNumOfTiles != (i+j):
                        AddMsgAndPrint(f"\t\t\tThere are {totalNumOfTiles - (i+j)} tiles that are not accounted for.")
                        AddMsgAndPrint(f"\t\t\tTry INCREASING max # of products per page; Currently set to {maxProdsPerPage}")

                else:
                    AddMsgAndPrint(f"\t\tThere are NO {tnmResolution} elevation tiles")
                    f.write(f"\n{hucDigit},{hucName},0,{tnmURLhuc}")
                    emptyHUCs.append(hucDigit)

            else:
                # I haven't seen an error like this
                AddMsgAndPrint(f"\t\tUnaccounted Scenario - HUC {hucDigit}: {hucName}")
                unaccountedHUCs.append(hucDigit)

        f.close()

        # Check for duplicates


        if unaccountedHUCs:
            AddMsgAndPrint(f"\nThere are {len(unaccountedHUCs):,} HUCs that are completely unaccounted for:")
            AddMsgAndPrint(f"{str(unaccountedHUCs)}")

        """ ------------------------------------ SUMMARY -------------------------------------- """
        AddMsgAndPrint(f"\n{'-'*40}SUMMARY{'-'*40}")
        if numOfTotalTiles > numOfUniqueDEMs:
            AddMsgAndPrint(f"Total # of Watersheds: {totalWatersheds:,}")
            if len(emptyHUCs):
                AddMsgAndPrint(f"Total # of Watersheds w/ NO Data: {len(emptyHUCs):,}")

            AddMsgAndPrint(f"\nTotal # of Elevation Files for all watersheds: {numOfTotalTiles:,}")
            AddMsgAndPrint(f"Total # of Overlap Elevation Files: {(numOfTotalTiles - numOfUniqueDEMs):,}")
            AddMsgAndPrint(f"Total # of Unique Elevation Files to download: {numOfUniqueDEMs:,}")
            AddMsgAndPrint(f"\nTotal Download Size (According to USGS Metadata): {convert_bytes(totalSizeBytes)}")

        else:
            AddMsgAndPrint(f"Total # of Elevation Files: {numOfUniqueDEMs:,}")
            AddMsgAndPrint(f"Total Download Size (According to USGS Metadata): {convert_bytes(totalSizeBytes)}")

        if len(badAPIurls) > 0:
            AddMsgAndPrint(f"\nTotal # of Bad API Requests: {len(badAPIurls):,}")
            AddMsgAndPrint("\tThese requests will have a -999 in num_of_tiles column")

        AddMsgAndPrint(f"\nTotal Processing Time: {toc(startTime)}")

    except:
        try:
            f.close()
            g.close()
        except:
            pass
        errorMsg()




