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

## ===================================================================================
def errorMsg(errorOption=1):
    try:

        exc_type, exc_value, exc_traceback = sys.exc_info()
        theMsg = "\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[1] + "\n\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[-1]

        print(theMsg)
##        if errorOption==1:
##            AddMsgAndPrint(theMsg)
##        else:
##            return theMsg

    except:
        AddMsgAndPrint("Unhandled error in unHandledException method")
        pass

## ===================================================================================
def print_progress_bar(index, total, label):

    "prints generic percent bar to indicate progress. Cheesy but works."

    n_bar = 50  # Progress bar width
    progress = index / total
    sys.stdout.write('\r')
    sys.stdout.write(f"\t[{'=' * int(n_bar * progress):{n_bar}s}] {int(100 * progress)}%  {label}")
    sys.stdout.flush()

## ===================================================================================
def createMasterDBfile(elevMetadataDict,dlStatusFileDict):

    """ elevMetadataDict
        [huc8digit,prod_title,pub_date,last_updated,size,fileFormat,sourceID,metadata_url,downloadURL]

    # dlStatusFileDict
    # [sourceID,prod_title,local_file,str(numOfFiles),str(size),now,'True']
        # 5eacf54a82cefae35a24e177,
        # USGS one meter x44y515 ME Eastern B1 2017
        # /data03/gisdata/elev/01/0101/010100/01010002/1m/USGS_one_meter_x44y515_ME_Eastern_B1_2017.tif
        # 1
        # 62690309
        # 11232022 16:19:44
        # True
    """

    try:

        dlMasterFileName = os.path.basename(downloadFile).split('.')[0] + "_MASTER_DB.txt"
        dlMasterFilePath = f"{os.path.dirname(downloadFile)}{os.sep}{dlMasterFileName}"

        g = open(dlMasterFilePath,'a+')
        header = ('huc8_digit,prod_title,pub_date,last_updated,size,format,sourceID,metadata_url,'
                  'download_url,DEMname,DEMpath,columns,rows,bandCount,cellSize,rdsFormat,bitDepth,noDataVal,srType,'
                  'EPSG,srsName,top,left,right,bottom,minStat,meanStat,maxStat,stDevStat,blockXsize,blockYsize')
        g.write(header)
        g.close()

        total = len(elevMetadataDict)
        index = 1
        label = "Gathering Raster Metadata Information"

        for srcID,demInfo in elevMetadataDict.items():

            g = open(dlMasterFilePath,'a')

            # huc8_digit,prod_title,pub_date,last_updated,size,format,sourceID,metadata_url,download_url
            firstPart = ','.join(str(e) for e in demInfo)
            fileFormat = demInfo[5].lower()

            if srcID in dlStatusFileDict:
                path = dlStatusFileDict[srcID][2]

                if path.endswith('.zip'):
                    if fileFormat == 'img':
                        demFilePath = path.replace('.zip','.img')
                    elif fileFormat == 'geotiff':
                        demFilePath = path.replace('.zip','.tif')
                    else:
                        print(f"SourceID: {srcID} is a zipe file but COULD NOT DETERMINE FILE FORMAT")
                        continue

                elif path.endswith('.tif') or path.endswith('.img'):
                    demFilePath = path

                else:
                    print(f"\tSourceID: {srcID} COULD NOT FIND A .TIF OR .IMG DEM FILe.  Trying alt method")
                    continue

                # Could not get DEM path using above method
                # Try looking for the DEM within the folder and using an inner name
                if not os.path.exists(demFilePath):

                    bAltMethodSuccess = False
                    demName = os.path.basename(path)
                    demDir = os.path.dirname(path)

                    # ['ned19', 'n33x00', 'w089x25', 'MS', 'NRCS-Lauderdale', '2013', '2014.zip']
                    # Remove the 1st and last item; focus on inner part of name
                    demNameSplit = demName.split('_')
                    demNameSplit.pop(0)
                    demNameSplit.pop(-1)

                    # Try searching directory using inner part of the demName
                    if len(demNameSplit) > 1:
                        innerName = '_'.join(demNameSplit)
                        possibleFiles = glob.glob(f"{demDir}{os.sep}*{innerName}*{fileFormat}")

                        if len(possibleFiles) == 1:
                            demFilePath = possibleFiles[0]
                            bAltMethodSuccess = True

##                            # Some USGS files have a prefix of img in the name; inconsistent
##                            # Try adding 'img' to beginning of name; make sure ext is .img; check if path exists
##                            # imgDEMpath = os.path.join(demDir,'img' + demName.split('.')[0] + '.' + fileFormat)
##
                    if not bAltMethodSuccess:
                        print("\t\tCould not find a DEM using alternate method")
                        #print(f"\tMultiple DEMs using this wildcard: *{innerName}*{fileFormat} Found")
                        g.write(f"\n{firstPart},{demName},{os.path.dirname(demFilePath)},{','.join('#'*20)}")
                        g.close()
                        print_progress_bar(index, total, label)
                        index+=1
                        continue

                demFileName = os.path.basename(demFilePath)
                secondPart = getRasterInformation(demFilePath)

                if secondPart:
                    g.write(f"\n{firstPart},{demFileName},{os.path.dirname(demFilePath)},{secondPart}")
                else:
                    # Raster information will have a #; need to revisit this error.
                    #print(f"\tError in getting raster information for sourceID: {srcID}")
                    g.write(f"\n{firstPart},{demFileName},{os.path.dirname(demFilePath)},{','.join('#'*20)}")

            else:
                print(f"SourceID: {srcID} FROM ELEVATION METADATA FILE HAS NO CORRESPONDING DEM IN DLSTATUS FILE")
                g.close()
                continue

            print_progress_bar(index, total, label)
            index+=1
            g.close()

        return dlMasterFilePath

    except:
        try:
            g.close()
        except:
            pass
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

        if not os.path.exists(raster):
            print("\t{raster} DOES NOT EXIST. Could not get Raster Information")
            return ','.join('#'*20)

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
                print(" Stats are set to 0 -- Calculating")
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
            print(" Stats not present in info -- Forcing Calc'ing of raster info")
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

##        if srs.IsProjected:
##            srsName = srs.GetAttrValue('projcs')
##        else:
##            srsName = srs.GetAttrValue('geogcs')

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
def getRasterInformationLINUX(raster):

    # The following raster information is not available through this function:
    # epsg ID: Not available for GCS but wil default to 4269; Available for projected CRS
    # bandCount: default to 1

    # r'GEOGCS["GCS_North_American_1983",DATUM["North_American_Datum_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]]PRIMEM["Greenwich",0.0],UNIT["Degree",0.017453292519943295]]'

    import pycrs

    command = r'gdalinfo /data03/gisdata/test/testAD.tif'
    command = r'gdalinfo -stats -listmdd -nofl -proj4 /data04/gisdata/elev/07/0704/070400/07040006/3m/ned19_n43x75_w091x00_wi_lacrosseco_2008.img'

##    # resuts are in bytes
##    import subprocess
##    subOut = subprocess.check_output(command, shell=True)

##    # results in bytes
##    proc = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
##    (out, err) = proc.communicate()

    # results are in a string
    from subprocess import PIPE, run
    result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True).stdout
    resList = result.split('\n')

    objects = ['Driver:','Files:','Coordinate System is:','PROJ.4 string is:','Metadata domains:','Corner Coordinates:']
    references = ['Size is',]

    columns = ""
    rows = ""
    bandCount = 1
    cellSize = ""

    bCoordSys = False
    ogc_wkt = ""

    for item in resList:

        # 'Driver:' describes file format; This is already captured in USGS metadata
        if item.startswith('Driver:'):
            continue

        # 'Files:' describes file name; This is already captured in USGS metadata
        if item.startswith('Files:'):
            continue

        # 'Size is:' describes rows and columns; r'Size is 8112, 8112'
        if item.startswith('Size is:'):
            colrows = item.replace('Size is ','').split(',') # ['8112', ' 8112']
            columns = int(colrows[0])
            rows = int(colrows[1])
            continue

        # 'Coordinate System is:' describes CRS; STARTS HERE
        if item.startswith('Coordinate System is:'):
            bCoordSys = True
            continue

        # WKT CRS description ends here
        if item.startswith(r'PROJ.4 string is:') and bCoordSys:
            bCoordSys = False
            continue

        # Append CRS items together
        if bCoordSys:
            ogc_wkt += " ".join(item.split()).strip('\n')

        # 'Pixel Size:' Pixel Size = (1.000000000000000,-1.000000000000000)
        if item.startswith('Driver:'):
            continue


    crs = pycrs.parse.from_ogc_wkt(ogc_wkt)
    srsName = crs.name

    if isinstance(crs, pycrs.ProjCS):
        srsType = 'PROJECTED'
    else:
        srsType = 'GEOGRAPHIC'

    rasterInfoList = [columns,rows,bandCount,cellSize,rdsFormat,bitDepth,noDataVal,srsType,epsg,srsName,
                      top,left,right,bottom,minStat,meanStat,maxStat,stDevStat,blockXsize,blockYsize]

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
        masterElevRecCount = len(open(masterElevFile).readlines())

        recCount = 0
        r2pgsqlFileName = os.path.basename(downloadFile).split('.')[0] + "_RASTER2PGSQL.txt"
        r2pgsqlFilePath = f"{os.path.dirname(downloadFile)}{os.sep}{r2pgsqlFileName}"
        g = open(r2pgsqlFilePath,'a+')

        total = sum(1 for line in open(masterElevFile)) -1
        label = "Generating Raster2PGSQL Statements"

        """ ------------------- Open Master Elevation File and write raster2pgsql statements ---------------------"""
        with open(masterElevFile, 'r') as fp:
            for line in fp:

                bMissingData = False

                # Check for missing data that may impact raster2pgsql parameters
                if line.find('#'):
                    bMissingData = True

                # Skip header line and empty lines
                if recCount == 0 or line == "\n":
                    recCount+=1
                    continue

                items = line.split(',')

##
##                if bMissingData:
##                    posIndexList = list()
##
##                    for item in items:
##                        if item == '#':
##                            posIndexList.append(i)
##                            i+=1
##                            print(f"{item} -- {i}")
##                            continue
##                        i+=1

                # Raster2pgsql parameters
                srid = items[19]
                tileSize = '507x507'
                demPath = f"{items[10]}{os.sep}{items[9]}"
                dbName = 'elevation'
                dbTable = 'elevation_3m'
                demName = items[9]
                password = 'itsnotflat'
                localHost = '10.11.11.10'
                port = '5432'

                r2pgsqlCommand = f"raster2pgsql -s {srid} -b 1 -t {tileSize} -F -a -R {demPath} {dbName}.{dbTable} | PGPASSWORD={password} psql -U {dbName} -d {dbName} -h {localHost} -p {port}"

                if recCount == masterElevRecCount:
                    g.write(r2pgsqlCommand)
                else:
                    g.write(r2pgsqlCommand + "\n")

                print_progress_bar(recCount, total, label)
                recCount+=1

        g.close()
        del masterElevRecCount

        return r2pgsqlFilePath

    except:
        errorMsg()


## ===================================================================================
if __name__ == '__main__':

    try:

        # DOWNLOAD FILE
        downloadFile = input("\nEnter full path to USGS_3DEP_XM_Metadata_Elevation_XXXXXXXX.txt File: ")
        while not os.path.exists(downloadFile):
            print(f"{downloadFile} does NOT exist. Try Again")
            downloadFile = input("Enter full path to USGS Metadata Download Text File: ")

        # DOWNLOAD Status FILE
        dlStatusFile = input("\nEnter full path to USGS_3DEP_XM_Metadata_Elevation_XXXXXXXX_Download_Status File: ")
        while not os.path.exists(dlStatusFile):
            print(f"{dlStatusFile} does NOT exist. Try Again")
            dlStatusFile = input("Enter full path to USGS Download Status File: ")

##        downloadFile = r'E:\python_scripts\DSHub\LinuxWorkflow\TEMP_Update_DL_StatusFile\TEMP_testingFiles\DevTesting\downloadFile_TEST.txt'
##        dlStatusFile = r'E:\python_scripts\DSHub\LinuxWorkflow\TEMP_Update_DL_StatusFile\TEMP_testingFiles\DevTesting\DLStatus_FileTEST.txt'
##
##        downloadFile = r'E:\python_scripts\DSHub\LinuxWorkflow\TEMP_Update_DL_StatusFile\TEMP_testingFiles\DevTesting\USGS_3DEP_1M_Metadata_Elevation_11202022_TEST.txt'
##        dlStatusFile = r'E:\python_scripts\DSHub\LinuxWorkflow\TEMP_Update_DL_StatusFile\TEMP_testingFiles\DevTesting\USGS_3DEP_1M_Metadata_Elevation_11202022_Download_Status_TEST.txt'


        dlFileHeaderItems = {
            "huc8_digit":0,
            "prod_title":1,
            "pub_date":2,
            "last_updated":3,
            "size":4,
            "format":5,
            "sourceID":6,
            "metadata_url":7,
            "download_url":8}

        dlStatusHeaderItems = {
            "sourceID":0,
            "prod_title":1,
            "path":2,
            "numofFiles":3,
            "unzipSize":4,
            "timestamp":5,
            "downloadstatus":6,}

        bHeader = True
        downloadDict = dict()  # contains download URLs and sourceIDs grouped by HUC; 07040006:[[ur1],[url2]]
        dlStatusDict = dict()  # contains all input info from input downloadFile.  sourceID:dlFile items
        dlFile_recCount = 0
        dlStatus_recCount = 0
        a_badLines = 0
        b_badLines = 0

        """ ---------------------------- Open Download File and Parse Information ----------------------------------"""
        with open(downloadFile, 'r') as fp:
            for line in fp:
                items = line.split(',')

                # Skip header line and empty lines
                if bHeader and dlFile_recCount == 0 or line == "\n":
                    dlFile_recCount+=1
                    continue

                # Skip if number of items are incorrect
                if len(items) != len(dlFileHeaderItems):
                    badLines+=1
                    continue

                huc8digit = items[dlFileHeaderItems["huc8_digit"]]
                prod_title = items[dlFileHeaderItems["prod_title"]]
                pub_date = items[dlFileHeaderItems["pub_date"]]
                last_updated = items[dlFileHeaderItems["last_updated"]]
                size = items[dlFileHeaderItems["size"]]
                fileFormat = items[dlFileHeaderItems["format"]]
                sourceID = items[dlFileHeaderItems["sourceID"]]
                metadata_url = items[dlFileHeaderItems["metadata_url"]]
                downloadURL = items[dlFileHeaderItems["download_url"]].strip()

                # Add info to elevMetadataDict
                downloadDict[sourceID] = [huc8digit,prod_title,pub_date,last_updated,
                                              size,fileFormat,sourceID,metadata_url,downloadURL]
                dlFile_recCount+=1
        del fp

        """ ---------------------------- Open dlStatus File and Parse Information ----------------------------------"""
        with open(dlStatusFile, 'r') as fp:
            for line in fp:
                items = line.split(',')

                # Skip header line and empty lines
                if bHeader and dlStatus_recCount == 0 or line == "\n":
                    dlStatus_recCount +=1
                    continue

                # Skip if number of items are incorrect
                if len(items) != len(dlStatusHeaderItems):
                    b_badLines+=1
                    continue

                sourceID = items[dlStatusHeaderItems["sourceID"]]
                prod_title = items[dlStatusHeaderItems["prod_title"]]
                filePath = items[dlStatusHeaderItems["path"]]
                numofFiles = items[dlStatusHeaderItems["numofFiles"]]
                unzipSize = items[dlStatusHeaderItems["unzipSize"]]
                timestamp = items[dlStatusHeaderItems["timestamp"]]
                downloadstatus = items[dlStatusHeaderItems["downloadstatus"]]

                # Add info to elevMetadataDict
                dlStatusDict[sourceID] = [sourceID,prod_title,filePath,numofFiles,unzipSize,timestamp,downloadstatus]
                dlStatus_recCount+=1


        # Create Master Elevation File
        if len(downloadDict):
            print(f"\nCreating Master Database Elevation File")

            dlMasterFile = createMasterDBfile(downloadDict,dlStatusDict)
            print(f"\tMaster Database Elevation File: {dlMasterFile}")

        else:
            print(f"\nNo information available to produce Master Database Elevation File")

        # Create Raster2pgsql Files
        if os.path.exists(dlMasterFile):
            print(f"\nCreating Raster2pgsql File")
            r2pgsqlFile = createRaster2pgSQLFile(dlMasterFile)
            print(f"\tRaster2pgsql File Path: {dlMasterFile}")
            print(f"\tIMPORTANT: Make sure dbTable variable (elevation_3m) is correct in Raster2pgsql file!!")
        else:
            print(f"\nNo information available to produce Raster2pgsql File")

    except:
        errorMsg()
