0# -*- coding: utf-8 -*-
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

This script can only be ran on python 3.7 until acrpy is removed 

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
    3. Number of elevation files associated with the watershed; This number might be reduced (14)
       in file #2 to account for duplicate DEMs from adjacent watersheds.
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
    10) Expand warning messages for duplicate product titles; run on 10M for 8-digit HUC 17010205; Warning will '
        print but no other indication of what is going on.
    11) Several 10M DEMs have the same date on the file name.  Add functionality to compare the times when dates
        the same.

---------------- UPDATES
2/8/2023
Modified code to incorporate capability for HUC4 Watersheds to use for ALASKA 10M and 5M IFSAR
- Add validation for hucboundary path
- Add validation for hucBoundary fields (hucCodeFld,hucCodeName)
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

2/23/2023
Modified entire code to account for a quality control inconsistency that was detected with DEM files
that have a different sourceIDs but whose product title and download url were identical.  The problem
with this situation is that the same file ultimately gets loaded twice into postgres b/c there will be
duplicate records in the raster2pgsql file.  This will have an adverse effect on any post-processing
that is done to these elevation files.

- Added new function to check for duplicate elements.  All API items from the metadata elevation file
  were parsed into individual lists so that duplicates could easily be found.  Currently, only sourceID,
  product title and download URLs are being assessed for duplicate records.  Other items will be assessed
  as they pose a problem.  If a duplicate item is found, it's index position is logged.  Once all items are
  checked, the index positions are removed from all lists.
- Updated Summary report to account for:
    - files that are overlapped
    - HUCs that do have elevation data but are accounted for by adjacent HUCs
    - Number of records that were removed due to duplicate items (sourceID, titles, URLs)
- Write Metadata elevation file after duplicate records have been removed.

4/14/2023
Updated the name of the 3 output files to be more intuitive.
- USGS_3DEP_1M_Metadata_API_02242023.txt --> USGS_3DEP_1M_Step1A_API_URLs_04132023.txt
- USGS_3DEP_1M_Metadata_Elevation_02242023.txt --> USGS_3DEP_1M_Step1B_ElevationDL_04132023.txt
- USGS_3DEP_1M_Metadata_API_ConsoleMsgs_02242023.txt --> USGS_3DEP_1M_Step1C_ConsoleMsgs_04132023.txt

4/24/2023
    - Updated the checkForDuplicateElements() function to remove duplicate files found in the USGS 10M data.
        Example:
            USGS 1-degree block file 'USGS_13_n38w103' has 4 duplicate files:
                Keep: USGS_13_n38w103_20210630
                Drop: USGS_13_n38w103_20210623
                Drop: USGS_13_n38w103_20210624
                Drop: USGS_13_n38w103_20170927

5/1/2023
    - Bug: Too many 10M DEMs were being removed.  Problem occurred in checkForDuplicates function.  Updated function
      to check for positions already accounted for by duplicate URLs.

6/28/2023
    - Updated functionality to send multiple requests for the same HUC

7/27/2023
    - Added multiple Alaska products to determine what product contains the IFSAR that matches the USGS
      inventory service.
    - Updated urlenocding to:
        urllib.parse.urlencode(params, quote_via=urllib.parse.quote, safe='()/')
      in order to preserve the paranthesis.  I noticed that the URL generated directly from the API was
      slightly different than the URL generated using urlencode and taking the defaults.  This puts both
      URLs identical.
    - Noticed an inconsistency with the TNM API in that it randomly generated 504 timeouts so I embedded
      multiple tries to send request.  That didn't do much so I incorporated a pause of 5-10 seconds between
      each attempt.
    - Switched protocols to HTTP vs HTTPS and realized more consistent results

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

## ================================================================================================================
def checkForDuplicateElements():
    """ This function only exists independently for organizing purposes.  The purpose
    is to find duplicate items in 3 lists: sourceIDList, titleList, downloadURLList
    and record their list index position.
    - If sourceIDs are found to be duplicated, then simply notify user.
    - If downloadURLs are duplicated then grab the most current record from the duplicated
      items and deleted the others.
    - If duplicate titles are found, return the name of the file that will be registered
      to postgres.  Duplicate titles should be handled differently but thus far they seem
      to be handled by the duplicate URLs.

    This function returns a list of index positions that need to be removed from all lists
    of elevation items.
    """

    try:
        idxPositionsToRemove = list()

        """ ----------------------- Duplicate 1M and 3M Files  ----------------------------"""
        #if tnmResolution in ('1M','3M'):

        # Create counter object to summarize unique element in a list and provide a count summary
        # {'USGS 1 Meter 13 x61y385 NM_NRCS_FEMA_Northeast_2017': 1}
        sourceIDsummary = dict(Counter(sourceIDList))
        titleSummary = dict(Counter(titleList))
        downloadURLSummary = dict(Counter(downloadURLList))

        # Takes counter object from above and isolates any counts greater than 1 and creates
        # a Dict from them k,v = {USGS one meter x21y385 TX Panhandle B5 2017': 2}
        duplicateSourcIDs = {key:value for key, value in sourceIDsummary.items() if value > 1}
        duplicateTitles = {key:value for key, value in titleSummary.items() if value > 1}
        duplicateDLurls = {key:value for key, value in downloadURLSummary.items() if value > 1}

        if len(duplicateSourcIDs) or len(duplicateDLurls) or len(duplicateTitles):

            numOfDEMdupURLs = sum([value for key, value in downloadURLSummary.items() if value > 1])
            numOfDEMdupTitles = sum([value for key, value in titleSummary.items() if value > 1])
            # list of index positions to be removed from master lists
            prodTitle = list()

            """ ----------------------- Find Duplicate SourceIDs --------------------"""
            # Unlikely that there will be unique sourceIDs --- Not handled
            if len(duplicateSourcIDs):
                AddMsgAndPrint(f"\tThere are {len(duplicateSourcIDs):,} duplicate sourceIDs.")

            """ ----------------------- Find Duplicate DownloadURLs --------------------"""
            # take the item that has the most recent modified date.
            if numOfDEMdupURLs:

                AddMsgAndPrint(f"\n\t{numOfDEMdupURLs:,} DEM files have duplicate Download URLs:")
                for url,count in duplicateDLurls.items():

                    # positions of duplicated DL URLs.
                    indexVal = [i for i, x in enumerate(downloadURLList) if x == url]
                    #AddMsgAndPrint(f"\n\t\tURL: {url} is duplicated {count}x at index positions {indexVal}")

                    # Get the last modified dates for the duplicate URLs and drop the oldest date
                    dates = list()
                    srcID = list()
                    for idx in indexVal:
                        dates.append(lastModifiedDate[idx])
                        srcID.append(sourceIDList[idx])
                        prodTitle.append(titleList[idx])

                    oldestDate = min(dates)
                    oldestDatePos = indexVal[dates.index(oldestDate)]
                    idxPositionsToRemove.append(oldestDatePos)

                    # shorten the URL for formatting purposes; assumes 'Elevation' is part of the URL
                    urlShortened = '/'.join(e for e in url.split('/')[url.split('/').index('Elevation'):])
                    AddMsgAndPrint(f"\n\t\tURL: {urlShortened} is duplicated {count}x for sourceIDs {srcID}")
                    AddMsgAndPrint(f"\t\t\tLast Updated Dates: {dates} -- Date Dropped: {oldestDate} -- sourceID: {sourceIDList[oldestDatePos]}")
                    #AddMsgAndPrint(f"\t\t\tIndex Position: {oldestDatePos}")

            """ ----------------------- Find Duplicate product titles --------------------"""
            # If duplicate titles are not handled by the duplicate url method
            # above then simply inform the user of how the DEM will be loaded.
            if numOfDEMdupTitles:

                AddMsgAndPrint(f"\n\tWARNING: {numOfDEMdupTitles:,} DEM files have duplicate Titles (Likely different URLs")
                accountedFor = 0
                for title,count in duplicateTitles.items():

                    if title in prodTitle:
                        #AddMsgAndPrint(f"\t\tTitle: {title} is duplicated {count}x but has been accounted for above")
                        accountedFor+=1
                        continue
                    else:
                        indexVal = [i for i, x in enumerate(titleList) if x == title]

                        url = list()
                        srcID = list()
                        msgs = list()
                        for idx in indexVal:
                            urlLink = downloadURLList[idx]
                            ID = sourceIDList[idx]
                            url.append(urlLink)
                            srcID.append(ID)
                            msgs.append(f"\t\t\t{titleList[idx]} ({ID}) will be loaded as {os.path.basename(urlLink)}")

                        AddMsgAndPrint(f"\t\tTitle: {title} is duplicated {count}x for sourceIDs {srcID}")
                        for msg in msgs:
                            AddMsgAndPrint(msg)

                if accountedFor == numOfDEMdupTitles:
                    AddMsgAndPrint(f"\t\tAll duplicated titles have been resolved")

        # USGS 10M duplicate DEMs can only be checked using the URL.  The naming convention for a 10M file is
        # USGS_13_n47w115_20230119.tif where a date is appended at the end.  However, the date doesn't correspond
        # the last_updated date or the pub_date.  Terrible QA
        """ ----------------------- Duplicate 10M Files  ----------------------------"""
        if tnmResolution == '10M':

            # take the downloadURL and isolate the root file name
            # 'https://prd-tnm.s3.amazonaws.com/S....101/USGS_13_n37w101_20210623.tif' --> USGS_13_n37w101

            dlURLfileName10MList = [((f.split('/')[-1]).split('.')[0]) for f in downloadURLList] # 'USGS_13_n37w101_20210623'
            dlURLParsedfileName10MList = ['_'.join(f.split('_')[0:-1]) for f in dlURLfileName10MList] # 'USGS_13_n37w101'
            #dlURLParsedfileName10MList = ['_'.join(((f.split('/')[-1]).split('.')[0]).split('_')[0:-1]) for f in downloadURLList]
            dlURLfileName10MSummary = dict(Counter(dlURLParsedfileName10MList))
            duplicate10Mfiles = {key:value for key, value in dlURLfileName10MSummary.items() if value > 1}

            if len(duplicate10Mfiles):

                numOf10MDEMdupURLs = len([value for key, value in dlURLfileName10MSummary.items() if value > 1])
                AddMsgAndPrint(f"\n\tThere are {numOf10MDEMdupURLs:,} USGS 1-degree block DEMs that have duplicate DEMs associated with them:")

                for file,count in duplicate10Mfiles.items():

                    AddMsgAndPrint(f"\n\t\tUSGS 1-degree block file '{file}' has {count} duplicate files:")

                    # list of index values from the files that are duplicated
                    indexVal = [i for i, x in enumerate(dlURLParsedfileName10MList) if x == file]

                    # Remove index positions that have already been accounted for above
                    alreadyAccounted = list()
                    for idx in indexVal:
                        if idx in idxPositionsToRemove:
                            alreadyAccounted.append(idx)
                            AddMsgAndPrint(f"\t\t\tDrop: {dlURLfileName10MList[idx]} -- sourceID: {sourceIDList[idx]} -- (Already Accounted for)")
                    for idx in alreadyAccounted:
                        indexVal.remove(idx)

                    # Duplicate dates should already be accounted for by finding duplicate URLs
                    # Find most current date
                    if indexVal:
                        # list of dates from filename ['20230119', '20160610']
                        datesList = list()
                        for idx in indexVal:
                            datesList.append(dlURLfileName10MList[idx].split('_')[-1])

                        demToKeep = max(datesList)

                        AddMsgAndPrint(f"\t\t\tKeep: {dlURLfileName10MList[indexVal[datesList.index(demToKeep)]]} -- sourceID: {sourceIDList[indexVal[datesList.index(demToKeep)]]}")
                        indexVal.pop(datesList.index(demToKeep))  # Remove most current DEM from indexVal list

                        #
                        for idx in indexVal:
                            idxPositionsToRemove.append(idx)
                            AddMsgAndPrint(f"\t\t\tDrop: {dlURLfileName10MList[idx]} -- sourceID: {sourceIDList[idx]}")

        """ ----------------------- Duplicate 3M Files  ----------------------------"""
        if tnmResolution == '3M':

            # take the downloadURL and isolate the root file name
            # 'https://prd-tnm.s3.amazonaws.com/S....101/USGS_13_n37w101_20210623.tif' --> USGS_13_n37w101

            dlURLfileName3MList = [((f.split('/')[-1]).split('.')[0]) for f in downloadURLList] # 'USGS_13_n37w101_20210623'
            dlURLParsedfileName3MList = ['_'.join(f.split('_')[0:3]) for f in dlURLfileName3MList] # 'ned19_n46x75_w068x50'
            #dlURLParsedfileName3MList = ['_'.join(((f.split('/')[-1]).split('.')[0]).split('_')[0:-1]) for f in downloadURLList]
            dlURLfileName3MSummary = dict(Counter(dlURLParsedfileName3MList))
            duplicate3Mfiles = {key:value for key, value in dlURLfileName3MSummary.items() if value > 1}

            if len(duplicate3Mfiles):

                numOf3MDEMdupURLs = len(duplicate3Mfiles)
                AddMsgAndPrint(f"\n\tThere are {numOf3MDEMdupURLs:,} USGS 1-degree block DEMs that have duplicate DEMs associated with them:")

                for file,count in duplicate3Mfiles.items():

                    AddMsgAndPrint(f"\n\t\tUSGS degree block file '{file}' has {count} duplicate files:")

                    # list of index values from the files that are duplicated
                    indexVal = [i for i, x in enumerate(dlURLParsedfileName3MList) if x == file]

                    # Remove index positions that have already been accounted for above
                    alreadyAccounted = list()
                    for idx in indexVal:
                        if idx in idxPositionsToRemove:
                            alreadyAccounted.append(idx)
                            AddMsgAndPrint(f"\t\t\tDrop: {dlURLfileName3MList[idx]} -- sourceID: {sourceIDList[idx]} -- (Already Accounted for)")
                    for idx in alreadyAccounted:
                        indexVal.remove(idx)

                    # Duplicate dates should already be accounted for by finding duplicate URLs
                    # Find most current date
                    if indexVal:
                        # list of dates from filename ['20230119', '20160610']
                        datesList = list()
                        for idx in indexVal:
                            datesList.append(dlURLfileName3MList[idx].split('_')[-1])

                        demToKeep = max(datesList)

                        AddMsgAndPrint(f"\t\t\tKeep: {dlURLfileName3MList[indexVal[datesList.index(demToKeep)]]} -- sourceID: {sourceIDList[indexVal[datesList.index(demToKeep)]]}")
                        indexVal.pop(datesList.index(demToKeep))  # Remove most current DEM from indexVal list

                        #
                        for idx in indexVal:
                            idxPositionsToRemove.append(idx)
                            AddMsgAndPrint(f"\t\t\tDrop: {dlURLfileName3MList[idx]} -- sourceID: {sourceIDList[idx]}")

        return idxPositionsToRemove

    except:
        errorMsg()
        return False

## ================================================================================================================
def qaDegreeBlockElevation(degreeLyr):

    try:
        if not arcpy.Exists(degreeLyr):
            AddMsgAndPrint(f"\tUSGS 1 Degree Block does not exist")
            return False

        dwnldDegreeBlocks = list()   # List of 1 degree-block DEMs returned from TNM API
        missingBlocks = list()

        # isolate degree block from download url
        for url in downloadURLList:
            degreeName = os.path.basename(os.path.dirname(url))
            dwnldDegreeBlocks .append(degreeName)

        # get a list of 1-degree blocks from USGS degree block index
        masterDegrees = [row[0] for row in arcpy.da.SearchCursor(degreeLyr, ['degreeName'])]

        for degreeBlock in masterDegrees:
            if not degreeBlock in dwnldDegreeBlocks:
                missingBlocks.append(degreeBlock)

        if len(missingBlocks):

            selQuery = ''
            for deg in missingBlocks:
                selQuery=f"{selQuery}'{deg}',"

            AddMsgAndPrint(f"\t\tYou have {len(missingBlocks)} missing 1x1 degree blocks from this API request")

            for block in missingBlocks:

                if tnmResolution == '10M':
                    dwnldURL = f"https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Elevation/13/TIFF/current/{block}/USGS_13_{block}.tif"
                    metaURL = f"https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Elevation/13/TIFF/current/{block}/USGS_13_{block}.xml"
                else:
                    dwnldURL = f"https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Elevation/1/TIFF/current/{block}/USGS_1_{block}.tif"
                    metaURL = f"https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Elevation/1/TIFF/current/{block}/USGS_1_{block}.xml"

                polyCodeList.append("99")
                titleList.append(f"USGS 1/3 Arc Second {block} 99999999")
                pubDateList.append("9999-99-99")
                lastModifiedDate.append("9999-99-99")
                sizeList.append(99999999)
                fileFormatList.append('GeoTIFF')
                sourceIDList.append('999999999999999999999')
                metadataURLList.append(metaURL)
                downloadURLList.append(dwnldURL)

            return selQuery

        else:
            AddMsgAndPrint("\All 1-Degree DEMs are accounted for")
            return True

    except:
        errorMsg()
        return False


## ====================================== Main Body ==================================
# Import modules
import sys, string, os, traceback, ast
import urllib, re, time, json, socket, requests
import arcgisscripting, arcpy
from datetime import datetime
from dateutil.parser import parse
from collections import Counter

from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

#urllibEncode = urllib.parse.urlencode

if __name__ == '__main__':

    try:
        # Start the clock
        startTime = tic()

        # 5 Tool Parameters
        # Boundaries used to iterate through the USGS API: state, huc4, huc8
        #apiBoundaries = r'E:\GIS_Projects\DS_Hub\hydrologic_units\WBD_National_GDB.gdb\WBDHU4'
        apiBoundaries = r'E:\GIS_Projects\DS_Hub\hydrologic_units\HUC12.gdb\states_a_US_Dissolve_WGS84'

        # Field containing the Well known poly code passed to API: huc = 'huc4'; state = 'STATE_FIPS'
        #wkPolyCode = 'huc4'
        wkPolyCode = 'STATE_FIPS'

        # Field containing alias name for the poly code above: huc = 'name'; state = 'STATE':
        wkPolyName = 'STATE'
        metadataPath = r'E:\GIS_Projects\DS_Hub\Elevation\DSHub_Elevation\USGS_Text_Files\10M\test'
        tnmResolution = '10M'

        if not arcpy.Exists(apiBoundaries):
            AddMsgAndPrint(f"\n{apiBoundaries} does NOT exist! EXITING!")
            exit()

        if not fieldExists(apiBoundaries,wkPolyCode):
            AddMsgAndPrint(f"\n{wkPolyCode} does NOT exist! EXITING!")
            exit()

        if not fieldExists(apiBoundaries,wkPolyName):
            AddMsgAndPrint(f"\n{wkPolyName} does NOT exist! EXITING!")
            exit()

        # Constant variables
        totalBoundaries = int(arcpy.GetCount_management(apiBoundaries)[0])
        tnmAPIurl = "https://tnmaccess.nationalmap.gov/api/v1/products?"
        if wkPolyName == 'STATE':
            boundaryName = 'State'
        else:
            boundaryName = 'Watershed'

        tnmProductAlias = {'1M':'Digital Elevation Model (DEM) 1 meter',
                           '3M':'National Elevation Dataset (NED) 1/9 arc-second',
                           '10M':'National Elevation Dataset (NED) 1/3 arc-second',
                           '30M': 'National Elevation Dataset (NED) 1 arc-second',
                           '5M_AK':'Alaska IFSAR 5 meter DEM',
                           '5M_AK_OPR':'Original Product Resolution (OPR) Digital Elevation Model (DEM)',
                           '5M_AK_ORI':'Ifsar Orthorectified Radar Image (ORI)',
                           '5M_AK_DSM':'Ifsar Digital Surface Model (DSM)',
                           '60M_AK':'National Elevation Dataset (NED) Alaska 2 arc-second'}


        """ ---------------------------- Establish Metdata and log FILES ---------------------------------------------------"""
        today = datetime.today().strftime('%m%d%Y')  #11192022

        # Metadata file#1; USGS_3DEP_1M_Step1_Metadata_API_02082023.txt
        # huc_digit, Number of tiles associated with huc, URL for TNM API
        metadataFile1 = f"USGS_3DEP_{tnmResolution}_Step1A_API_URLs_{today}.txt"

        metadataFile1path = f"{metadataPath}\\{metadataFile1}"
        f = open(metadataFile1path,'a+')
        f.write(f"polyCode,polyName,num_of_tiles,API_URL") # log headers

        # Metadata file#2; USGS_3DEP_1M_Metadata_Elevation_02082023.txt
        # huc_digit,prod_title,pub_date,last_updated,size,format,sourceID,metadata_url,download_url
        metadataFile2 = f"USGS_3DEP_{tnmResolution}_Step1B_ElevationDL_{today}.txt"
        metadataFile2path =  f"{metadataPath}\\{metadataFile2}"

        # Log file that captures console messages
        logFile = f"USGS_3DEP_{tnmResolution}_Step1C_ConsoleMsgs_{today}.txt"
        logFilePath = f"{metadataPath}\\{logFile}"
        h = open(logFilePath,'a+')
        h.write(f"Executing: USGS_1_Create_API_Metadata Script {today}\n\n")
        h.write(f"User Selected Parameters:\n")
        h.write(f"\tTNM Product: {tnmResolution} - {tnmProductAlias[tnmResolution]}\n")
        h.write(f"\tAPI Boundary Dataset: {apiBoundaries}\n")
        h.write(f"\tMetadata File Path: {metadataPath}\n")
        h.close()

        uniquePolyCodes = list()      # unique list of huc code fields - should match totalBoundaries
        duplicatePolyCodes = list()    # duplicate HUC - error in water huc code field
        invalidPolyCodes = list()      # invalid hucs; must be 2,4,8 huc-digits - must be integer
        emptyPolyCodes = list()       # Watershed with NO DEMS associated to it; according to API
        badAPIurls = dict()           # huc:API URL - These URLs returned an error code
        allAccountedFor = list()      # # of HUCs whose DEMs are accounted for by adjacent HUCs
        unaccountedHUCs = list()      # Not a Bad url, not an empty watershed, unaccounted for result
        numOfTotalTiles = 0           # Total of all DEMS from watersheds; including overalp DEMs
        numOfTotalOverlaps = 0        # Total # of DEMs that overlap with adjacent HUCs
        uniqueSourceIDlist = list()   # unique list of sourceIDs

        # master lists for each data element collected from USGS API
        polyCodeList = list()
        titleList = list()
        pubDateList = list()
        lastModifiedDate = list()
        sizeList = list()
        fileFormatList = list()
        sourceIDList = list()
        metadataURLList = list()
        downloadURLList = list()

        AddMsgAndPrint(f"\nCOMPILING USGS API URL REQUESTS FOR {totalBoundaries:,} {boundaryName}(s) -- {tnmResolution}: {tnmProductAlias[tnmResolution]}")
        wbd = 0

        # 500 API records is the max --- DO NOT CHANGE!
        maxProdsPerPage = 500

        # Iterate through every watershed to gather elevation info
        # create dictionary of Watershed digits (keys) and API request (values)
        orderByClause = f"ORDER BY {wkPolyCode} ASC"
        for row in arcpy.da.SearchCursor(apiBoundaries, [wkPolyCode,wkPolyName], sql_clause=(None, orderByClause)):

            wbd+=1
            apiPolyCode = row[0]
            apiPolyName = row[1]

            # State Boundary vs HUC
            if wkPolyName == 'Watershed':
                hucLength = len(apiPolyCode)
                AddMsgAndPrint(f"\n\t{hucLength}-digit HUC {apiPolyCode}: {apiPolyName} -- {wbd:,} of {totalBoundaries:,}")
                polyType = f"huc{hucLength}"

                # USGS API only handles 2,4,8 huc-digits
                if not hucLength in (2,4,8):
                    AddMsgAndPrint(f"\tUSGS API only handles HUC digits 2, 4 and 8. Not {hucLength}-digit")
                    invalidPolyCodes.append(apiPolyCode)
                    continue

            else:
                AddMsgAndPrint(f"\n\tState: {apiPolyCode}: {apiPolyName} -- {wbd:,} of {totalBoundaries:,}")
                polyType = f"state"

            # geometry multi-parts are irrelavant
            if apiPolyCode in uniquePolyCodes:
                AddMsgAndPrint(f"\tDUPLICATE {boundaryName}.  Skipping!")
                duplicatePolyCodes.append(apiPolyCode)
                continue
            else:
                uniquePolyCodes.append(apiPolyCode)

            # Need a valid apiPolyCode code; must be integer and not character
            if apiPolyCode in (None,' ','NULL','Null') or not apiPolyCode.isdigit():
                AddMsgAndPrint(f"\t{apiPolyCode} value is invalid.  Skipping Record")
                invalidPolyCodes.append(apiPolyCode)
                continue

            bRecordsAccounted = False  # boolean to
            i = 0  # Counter for unique DEMs
            j = 0  # Counter for duplicate DEM that exists in a different State/HUC
            k = 0  # Counter for records per request; resetted if multiple requets are needed
            recordStart = 0
            numOfHucDEMfiles = 0
            requestNumber = 0

            while not bRecordsAccounted:

                requestNumber+=1

                # Formulate API rquest
                params = {"datasets": tnmProductAlias[tnmResolution],
                          "polyCode": apiPolyCode,
                          "polyType": polyType,
                          "offset":recordStart,
                          "max":maxProdsPerPage}

                # encode using quote() function to use UTF-8 encoding scheme.  This will translate
                # spaces ' ' to %20
                paramsEncoded = urllib.parse.urlencode(params, quote_via=urllib.parse.quote, safe='()/')

                # concatenate URL and API parameters
                tnmURLhuc = f"{tnmAPIurl}{paramsEncoded}"

                # Send REST API request
                try:
                    # Attempt #1
                    time.sleep(5)
                    with urllib.request.urlopen(tnmURLhuc) as conn:
                        resp = conn.read()
                    results = json.loads(resp)
                    print("\t\t----------option A")

                # Have received URLError 504 but results are still populated.
                except URLError as e:
                    AddMsgAndPrint(f"\t\tURL Error: {tnmURLhuc}")
                    AddMsgAndPrint(f"\t\tReason: {e.reason}")
                    badAPIurls[apiPolyCode] = tnmURLhuc
                    f.write(f"\n{apiPolyCode},{apiPolyName},-999,{tnmURLhuc}")
                    break

                except HTTPError as e:
                    AddMsgAndPrint(f"\t\tHTTP Error: {tnmURLhuc}")
                    AddMsgAndPrint(f"\t\tReason: {e.reason}")
                    AddMsgAndPrint(f"\t\tCode: {e.reason}")
                    badAPIurls[apiPolyCode] = tnmURLhuc
                    f.write(f"\n{apiPolyCode},{apiPolyName},-999,{tnmURLhuc}")
                    break

                except json.JSONDecodeError as e:
                    try:
                        AddMsgAndPrint(f"\t\t2nd Attempt")
                        time.sleep(10)
                        try:
                            del resp, results,conn
                        except:
                            pass
                        request = urllib.request.Request(tnmURLhuc,headers={'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11'})

                        with urllib.request.urlopen(request) as conn:
                            resp = conn.read()
                        try:
                            results = json.loads(resp)
                            print("\t\t----------option C")
                        except json.JSONDecodeError as e:
                            dataform = str(resp).strip("'<>() ").replace('\'', '\"')
                            results = json.loads(dataform)
                            print("\t\t----------option D")

                    except URLError as e:
                        AddMsgAndPrint(f"\t\t\tURL Error: {tnmURLhuc}")
                        AddMsgAndPrint(f"\t\t\tReason: {e.reason}")
                        badAPIurls[apiPolyCode] = tnmURLhuc
                        f.write(f"\n{apiPolyCode},{apiPolyName},-999,{tnmURLhuc}")
                        break

                    except HTTPError as e:
                        AddMsgAndPrint(f"\t\tHTTP Error: {tnmURLhuc}")
                        AddMsgAndPrint(f"\t\tReason: {e.reason}")
                        AddMsgAndPrint(f"\t\tCode: {e.reason}")
                        badAPIurls[apiPolyCode] = tnmURLhuc
                        f.write(f"\n{apiPolyCode},{apiPolyName},-999,{tnmURLhuc}")
                        break

                    except:
                        badAPIurls[apiPolyCode] = tnmURLhuc
                        AddMsgAndPrint(f"\t\t\tUnhandled URL request error: {errorMsg(errorOption=2).strip()}")
                        AddMsgAndPrint(f"\t\t\tURL: {tnmURLhuc}")
                        f.write(f"\n{apiPolyCode},{apiPolyName},-999,{tnmURLhuc}")
                        break

                if 'errorMessage' in results:
                    AddMsgAndPrint(f"\t\tError Message from server: {results['errorMessage']}")
                    badAPIurls[apiPolyCode] = tnmURLhuc
                    bRecordsAccounted = True

                elif len(results['errors']):
                    AddMsgAndPrint(f"\t\tError Message in results: {results['errors']}")
                    badAPIurls[apiPolyCode] = tnmURLhuc
                    bRecordsAccounted = True

                # JSON results
                elif 'total' in results:

                    # the # of DEMs associated with the HUC
                    numOfHucDEMfiles = results['total']

                    if numOfHucDEMfiles > 0:

                        if requestNumber==1:
                            numOfTotalTiles += numOfHucDEMfiles
                            AddMsgAndPrint(f"\t\t# of DEMs from API: {numOfHucDEMfiles:,}")

                        # collect info from unique elevation files
                        for itemInfo in results['items']:

                            k+=1

                            sourceID = itemInfo['sourceId']
                            title = itemInfo['title']
                            pubDate = itemInfo['publicationDate']
                            lastModified = itemInfo['modificationInfo']
                            size = itemInfo['sizeInBytes']
                            fileFormat = itemInfo['format']
                            metadataURL = itemInfo['metaUrl']
                            downloadURL = itemInfo['downloadURL']

                            # use sourceID to filter out duplicate DEMs from adjacent watersheds
                            if sourceID in uniqueSourceIDlist:
                                #AddMsgAndPrint(f"\t\t\tTile already exists {apiPolyCode} -- {sourceID}")
                                j+=1
                                numOfTotalOverlaps+=1
                                continue
                            else:
                                uniqueSourceIDlist.append(sourceID)

                            # Ran into a situation where size was incorrectly populated as none
                            # https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/NH_CT_RiverNorthL6_P2_2015/TIFF/USGS_one_meter_x31y491_NH_CT_RiverNorthL6_P2_2015.tif
                            if not size:
                                size = 0

                            polyCodeList.append(apiPolyCode)
                            titleList.append(title)
                            pubDateList.append(pubDate)
                            lastModifiedDate.append(lastModified)
                            sizeList.append(size)
                            fileFormatList.append(fileFormat)
                            sourceIDList.append(sourceID)
                            metadataURLList.append(metadataURL)
                            downloadURLList.append(downloadURL)

                            i+=1

                        # DEMs NOT ACCOUNTED FOR - lOOP AGAIN
                        # num of HUC DEMs is greater than 1000 rec limit AND counters for unique and
                        # duplicate DEMs are less than total
                        if numOfHucDEMfiles > maxProdsPerPage and ((i+j) < numOfHucDEMfiles):
                            AddMsgAndPrint(f"\t\t\tRequest: #{requestNumber} -- DEMs obtained: {k:,}")
                            recordStart+=maxProdsPerPage
                            k=0

                        # NO NEED TO LOOP AGAIN
                        # API request is less than record limit BUT DEM files are unaccounted for --
                        # Wrong count from USGS API?
                        # Look into this for patterns
                        elif numOfHucDEMfiles < maxProdsPerPage and ((i+j) != numOfHucDEMfiles):
                            AddMsgAndPrint(f"\t\t\tThere are {numOfHucDEMfiles - (i+j):,} files that are not accounted for. ")
                            #AddMsgAndPrint(f"\t\t\tTry INCREASING max # of products per page; Currently set to {maxProdsPerPage:,}")
                            AddMsgAndPrint(f"\t\t# of DEMs from API: {numOfHucDEMfiles:,}")
                            AddMsgAndPrint(f"\t\t# of overlap DEMs: {j:,}")
                            AddMsgAndPrint(f"\t\t# of {tnmResolution} DEMs to download: {i:,}")
                            f.write(f"\n{apiPolyCode},{apiPolyName},{numOfHucDEMfiles},{tnmURLhuc}")
                            bRecordsAccounted = True

                        # DEMs ACCOUNTED FOR - NO NEED TO LOOP AGAIN
                        else:

                            # HUC's DEMs have been already accounted for by adjacent DEMs
                            if j==numOfHucDEMfiles:
                                allAccountedFor.append(apiPolyCode)

                            if requestNumber>1:
                                AddMsgAndPrint(f"\t\t\tRequest: #{requestNumber} -- DEMs obtained: {k:,}")

                            #AddMsgAndPrint(f"\t\t# of DEMs from API: {numOfHucDEMfiles:,}")
                            AddMsgAndPrint(f"\t\t# of overlap DEMs: {j:,}")
                            AddMsgAndPrint(f"\t\t# of {tnmResolution} DEMs to download: {i:,}")
                            f.write(f"\n{apiPolyCode},{apiPolyName},{numOfHucDEMfiles},{tnmURLhuc}")
                            bRecordsAccounted = True

                    else:
                        AddMsgAndPrint(f"\t\tThere are NO {tnmResolution} elevation files")
                        f.write(f"\n{apiPolyCode},{apiPolyName},0,{tnmURLhuc}")
                        emptyPolyCodes.append(apiPolyCode)
                        bRecordsAccounted = True

                else:
                    # I haven't seen an error like this
                    AddMsgAndPrint(f"\t\tUnaccounted Scenario - HUC {apiPolyCode}: {apiPolyName}")
                    AddMsgAndPrint(f"\t\t\tResults: {results}")
                    unaccountedHUCs.append(apiPolyCode)
                    bRecordsAccounted = True

        f.close()

        #-------------------------------------------------- Check for and fix duplicate elements
        AddMsgAndPrint(f"\nChecking DEM files for duplicate: 'sourceIDs', 'Product Titles' or 'Download URLs'")
        idxValuesToRemove = checkForDuplicateElements()

        if len(idxValuesToRemove):

            # index values must be in descending order so correct value is removed
            idxValuesToRemove.sort(reverse=True)
            AddMsgAndPrint(f"\n\tThere are {len(idxValuesToRemove):,} records that will be removed due to duplicate elements")

            masterLists = [polyCodeList,titleList,pubDateList,lastModifiedDate,sizeList,
                            fileFormatList,sourceIDList,metadataURLList,downloadURLList]
            for mList in masterLists:
                for idx in idxValuesToRemove:
                    mList.pop(idx)

        else:
            AddMsgAndPrint(f"\tNo duplicate DEM elements found were found")

        hucsWithData = len(set(polyCodeList))
        duplicateElements = len(idxValuesToRemove)

        #-------------------------------------------------- QA 10M and 30M elevation by Degree block
        if tnmResolution in ('10M','30M'):
            degreeLyr = r'E:\GIS_Projects\DS_Hub\Boundaries\Default.gdb\CellGrid_1X1Degree_forElevation'
            AddMsgAndPrint("\nRunning QA/QC against 1-Degree Blocks")
            qa = qaDegreeBlockElevation(degreeLyr)

        #-------------------------------------------------- Write Elevation download file
        g = open(metadataFile2path,'a+')
        g.write(f"polyCode,prod_title,pub_date,lastupdate,rds_size,format,sourceid,meta_url,downld_url") # log headers

        for i in range(0,len(polyCodeList)):
            g.write(f"\n{polyCodeList[i]},{titleList[i]},{pubDateList[i]},{lastModifiedDate[i]},{sizeList[i]},{fileFormatList[i]},{sourceIDList[i]},{metadataURLList[i]},{downloadURLList[i]}")
        g.close()

        if unaccountedHUCs:
            AddMsgAndPrint(f"\nThere are {len(unaccountedHUCs):,} HUCs that are completely unaccounted for:")
            AddMsgAndPrint(f"{str(unaccountedHUCs)}")

        """ ------------------------------------ SUMMARY -------------------------------------- """
        AddMsgAndPrint(f"\n{'-'*40} SUMMARY {'-'*40}")

        # Summary of HUCs
        AddMsgAndPrint(f"Total # of Input {boundaryName}(s): {totalBoundaries:,}")
        AddMsgAndPrint(f"\t# of {boundaryName}(s) with Elevation Data: {hucsWithData:,}")

        if len(emptyPolyCodes):
            AddMsgAndPrint(f"\t# of {boundaryName}(s) without Elevation Data: {len(emptyPolyCodes):,}")
        if len(allAccountedFor):
            AddMsgAndPrint(f"\t# of {boundaryName}s whose DEMs are accounted for by adjacent {boundaryName}s: {len(allAccountedFor):,}")
        if len(badAPIurls) > 0:
            AddMsgAndPrint(f"\t# of {boundaryName}(s) with Bad API Requests: {len(badAPIurls):,}")
            AddMsgAndPrint("\t\tThese requests will have a -999 in num_of_tiles column")

        # Summary of DEMs to download
        AddMsgAndPrint(f"\nTotal # of DEMs for all {boundaryName}(s) (per USGS API): {numOfTotalTiles:,}")
        AddMsgAndPrint(f"\t# of DEMs that overlap with adjacent {boundaryName}(s): {numOfTotalOverlaps:,}")
        if duplicateElements:
            AddMsgAndPrint(f"\t# of DEMs with duplicate elements: {duplicateElements:,}")

        AddMsgAndPrint(f"\nTotal # of unique DEMs to download: {len(polyCodeList):,}")

        # Size Summary
        AddMsgAndPrint(f"\nTotal Download Size (According to USGS Metadata): {convert_bytes(sum(sizeList))}")

        # Time Summary
        AddMsgAndPrint(f"\nTotal Processing Time: {toc(startTime)}")

    except:
        try:
            f.close()
            g.close()
        except:
            pass
        errorMsg()




