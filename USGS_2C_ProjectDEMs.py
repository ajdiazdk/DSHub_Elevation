# -*- coding: utf-8 -*-
"""
Created on Tue Oct 24 10:40:54 2023
@author: Adolfo.Diaz

---------------- UPDATES
11/29/2023
    - Added 3 subfunctions within the getRegionalSRS function
    
12/15/2023
    - Added flexibility of passing all new header values (poly_name) over to the new metadata file
    - Adjusted for Alaska IFSAR and OPR DEMs by setting them to EPSG 3338 without inquiring aobut the inputSRS
      b/c many are set to an ESRI SRS.
      
1/8/2024
    - Added "targetAlignedPixels=True" option to projectDEM function to ensure pixels were aligned from file to file      
    
UPDATES:
    - If there is a DEM that fails to be projected the EPSG summary becomes incorrect.  Need to remove
      bad unprojected DEMs from projectionInfoList using the projectionFailedDict.  Iterate through the 
      projectionFailedDict and remove the DEMs that failed from projectionInfoList.  This should fix things.
"""
import os, traceback, sys, time, fnmatch, glob, psutil
import multiprocessing
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from osgeo import gdal
from osgeo import osr


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
        print(errorMsg())
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
            return(theMsg)

    except:
        AddMsgAndPrint("Unhandled error in errorMsg()")
        #AddMsgAndPrint("Unhandled error in unHandledException method")
        #pass
        
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
def convertMasterDBfileToDict(elevMetdataFile):
    """ Opens the Master Elevation Database CSV file containing the metadata for every
        DEM file, including statistical and geographical information.  Parses the content
        into a dictionary with the sourceId being the key and the rest of the information
        seriving as the key in a list format.

        '63e7308bd34efa0476ae8401': ['11030005',
                              'USGS 1 Meter 14 x34y421 '
                              'KS_StatewideFordGray_2018_A18',
                              '2023-02-06',
                              '2023-02-11',
                              '3871630',
                              'GeoTIFF',
                              '63e7308bd34efa0476ae8401',
                              'https://www.sciencebase.gov/catalog/item/63e7308bd34efa0476ae8401',
                              'https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1m/Projects/KS_StatewideFordGray_2018_A18/TIFF/USGS_1M_14_x34y421_KS_StatewideFordGray_2018_A18.tif',
                              'USGS_1M_14_x34y421_KS_StatewideFordGray_2018_A18.tif',
                              'D:\\projects\\DSHub\\reampling\\1M',
                              '10012',
                              '10012',
                              '1',
                              '1.0',
                              'GeoTIFF',
                              'Float32',
                              '-999999.0',
                              'PROJECTED',
                              '26914',
                              'NAD83 / UTM zone 14N',
                              '4210006.0',
                              '339994.0',
                              '350006.0',
                              '4199994.0',
                              '875.856201171875',
                              '878.0380251453253',
                              '881.0860595703125',
                              '1.272683098168289',
                              '512',
                              '512\n']}
    """
    try:
        
        # List of header values from elevation metadata file
        headerValues = open(elevMetdataFile).readline().rstrip().split(',')
        numOfTotalRecs = len(open(elevMetdataFile).readlines())-1

        # Position of DEM Name
        demNameIdx = headerValues.index("dem_name")
        sourceIDidx = headerValues.index('sourceid')

        masterDBfileDict = dict()
        recCount = 0
        badLines = 0

        """ ---------------------------- Open Download File and Parse Information ----------------------------------"""
        with open(elevMetdataFile, 'r') as fp:
            for line in fp:
                items = line.split(',')
                items[-1] = items[-1].strip('\n')  # re"D:\projects\DSHub\3mdsh_testing\USGS_3DEP_3M_Step2_Elevation_Metadata.txt"move the hard return of the line
                
                # Skip header line and empty lines
                if recCount == 0 or line == "\n":
                    recCount+=1
                    continue
                
                #REMOVE
                items.pop(31);items.pop(31),items.pop(31)

                # Skip if number of items are incorrect
                if len(items) != len(headerValues):
                    AddMsgAndPrint(f"\tLine # {recCount} has {len(items)} out of {len(headerValues)} values")
                    badLines+=1
                    continue

                # Skip if a # was found
                if '#' in items:
                    errorPos = items.index('#')
                    AddMsgAndPrint(f"\tLine # {recCount} has an error value for '{headerValues[errorPos]}'")
                    badLines+=1
                    # continue

                demName = items[demNameIdx]
                sourceID = items[sourceIDidx]

                # Add info to elevMetadataDict
                masterDBfileDict[sourceID] = items
                recCount+=1
        del fp

        # Remove Header Value from count
        recCount-=1

        if numOfTotalRecs > 0:
            AddMsgAndPrint(f"\tElevation File contains {numOfTotalRecs:,} DEM files")

        if badLines > 0:
            AddMsgAndPrint(f"\tThere are(is) {badLines:,} records with anomalies found")

        if len(masterDBfileDict) == 0:
            AddMsgAndPrint("\t\tThere are no valid DEMs to project")
            return False

        return masterDBfileDict

    except:
        errorMsg()
        return False

## ================================================================================================================
def getRegionalSRS(itemCollection,headerValues):
    
    try:
        
        #====================================================================
        def getEPSG(raster):
            try:
                rds = gdal.Open(raster)

                prj = rds.GetProjection()  # GDAL returns projection in WKT
                srs = osr.SpatialReference(prj)

                # If no valid EPSG is found, an error will be thrown
                srs.AutoIdentifyEPSG()
                epsg = srs.GetAttrValue('AUTHORITY',1)
                
                if srs.IsProjected():
                    srsName = srs.GetAttrValue('projcs')
                else:
                    srsName = srs.GetAttrValue('geogcs')
                    
                rds = None;del rds
                return epsg,srsName.lower()
            except:
                AddMsgAndPrint(f"\tFailed to execute getEPSG() for: {os.path.basename(raster)} \n\t {errorMsg(errorOption=2)}")
                rds = None;del rds
                return None,None  
            
        #====================================================================
        def getOutputSRS(topExtent):
            # CONUS
            if top > 25 and top < 50.5:
                outEPSG = 5070
            # PRUSVI
            elif top > 17.5 and top < 19.5 and left > -68.5:
                outEPSG = 32161
            # Alaska
            elif top > 51.0:
                outEPSG = 3338
            # Hawaii - PacBasin
            else:
                outEPSG = 4326
                
            #print(f"\toutput EPSG = {outEPSG}")
            return outEPSG
        
        #==================================================================== 
        def getExtent(raster):
            # Some 1M files failed to get raster information produced
            try:
                rds = gdal.Open(raster)
                rdsInfo = gdal.Info(rds,format="json")
                
                right,top = rdsInfo['cornerCoordinates']['upperRight']   # Eastern-Northern most extent
                left,bottom = rdsInfo['cornerCoordinates']['lowerLeft']  # Western - Southern most extent
                rds = None;del rds
                return float(top),float(left)
            except:
                return None,None
            
        #==================================================================== 
        def getCellSize(raster):
            try:
                rds = gdal.Open(raster)
                cellSize = rds.GetGeoTransform()[1]
                rds = None;del rds
                return str(cellSize)
            except:
                return None
            
        # tuple collection
        #sourceID = itemCollection[0]
        demInfo = itemCollection[1]  # list of lists
        
        # Positions of individual field names
        sourceID = demInfo[headerValues.index("sourceid")]
        DEMname = demInfo[headerValues.index("dem_name")]
        DEMpath = demInfo[headerValues.index("dem_path")]
        rasterPath = os.path.join(DEMpath,DEMname)
        cellSize = demInfo[headerValues.index("cellsize")]
        noDataVal = demInfo[headerValues.index("nodataval")]
        srsName = demInfo[headerValues.index("srs_name")]
        EPSG = (demInfo[headerValues.index("epsg_code")])
        
        # Check Raster Path
        # if not os.path.exists(rasterPath):
        #     AddMsgAndPrint(f"\t{DEMname} does NOT exist; Will NOT be projected")
        #     return [False,DEMname]
        
        # EPSG Information
        try:
            if not bAlaska:
                int(EPSG)
                
                if srsName == None:
                    EPSG,srsName = getEPSG(rasterPath)
                    
                    if EPSG == None or srsName == None:
                        AddMsgAndPrint(f"\tFailed to acquire EPSG from {DEMname}; Will NOT be projected")
                        return [False,DEMname]
        except:
            EPSG,srsName = getEPSG(rasterPath)
            
            if EPSG == None:
                AddMsgAndPrint(f"\tFailed to acquire EPSG from {DEMname}; Will NOT be projected")
                return [False,DEMname]
            
        # Check Extent Information
        try:
            top = float(demInfo[headerValues.index("rds_top")])
            left = float(demInfo[headerValues.index("rds_left")])
        except:
            top,left = getExtent(rasterPath)
            
            if top == None:
                AddMsgAndPrint(f"\tFailed to acquire TOP coordinate from {DEMname}; Will NOT be projected")
                return [False,DEMname]
            
        # Check Cell Size Information
        try:
            float(cellSize)
        except:
            cellSize = getCellSize(rasterPath)
            if cellSize == None:
                AddMsgAndPrint(f"\tFailed to acquire Cell Size from {DEMname}; Will NOT be projected")
                return [False,DEMname]
            
        # ------------------------ AK OPR USGS File; Multiple Coordinate systems and cell sizes
        if bAlaska:
            # Alaska had issues deciphering the EPSG so GDAL will simply  project it using DEM metadata
            #if EPSG in ['1116','1133','3338','4019','4269','6318','6332','6333','6334','6335','6337','6394','7019','26903','26906','26908']:
            inputSRS = EPSG
            
            # cell size will be kept as the same
            xyRes = cellSize
            outputSRS = 3338
            
            # else:
            #     return [False,DEMname]
                
        # ------------------------ File is a 1M USGS File; use UTM zones
        elif cellSize in ["1.0","1"] or cellSize.find("1.0") >-1 or cellSize.find("0.99") >-1:
            #print(f"\tsource resolution = {cellSize}; DEM: {DEMname}")

            inputSRS = EPSG
            xyRes = 1
            
            if srsName.find('zone') > -1:
                #print(f"\t\t{srsName}")
                zoneNumber = int(srsName.split(' ')[-1][:-1])
                                
                # Alaska or Hawaii
                if zoneNumber == 4 or zoneNumber == 5:
                    # Hawaii
                    if top < 2500000:
                        outputSRS = 4326
                        
                        # used https://www.opendem.info/arc2meters.html to convert 1M to DD
                        # using a latitude of 19.7 (center of big island)
                        # A better method is needed as 1M in HI and PB grow
                        xyRes = 0.00000955874839323294
                    # Alaska
                    else:
                        outputSRS = 3338
                # Alaska
                elif zoneNumber <= 9:
                    outputSRS = 3338
                # CONUS
                elif zoneNumber >= 10 and zoneNumber <= 18:
                    outputSRS = 5070 # CONUS
                # CONUS or PRUSVI
                elif zoneNumber == 19 or zoneNumber == 20:
                    if top < 2050007:
                        outputSRS = 32161
                    else:
                        outputSRS = 5070
                else:                
                    AddMsgAndPrint(f"\tUTM Zone not accounted for {zoneNumber}; Will NOT be projected")
                    return [False,DEMname]
                    
            else:
                AddMsgAndPrint(f"\tUTM Zone not detected in SRS Name for {DEMname}; Will NOT be projected \n\tSRS Name: {srsName}")
                return [False,DEMname]
                    
        # ------------------------ File is a 3M USGS File
        elif cellSize.find("4.11370") > -1 or cellSize.find("3.086419") > -1 or cellSize.find("3086419") or cellSize.find("411370") > -1:  
            #print("\tsource resolution = 3")
            inputSRS = EPSG
            outputSRS = getOutputSRS(top)
            
            if outputSRS == 4326:
                # must remain in degrees
                xyRes = cellSize
            else:
                xyRes = 3
            
        # ------------------------ File is a 10M USGS File
        elif cellSize.find("9.259259") >-1 or cellSize.find("9259259") >-1:  # w and w/o exponents
            #print("\tsource resolution = 10")
            inputSRS = EPSG
            outputSRS = getOutputSRS(top)
            
            if outputSRS == 4326:
                # must remain in degrees
                xyRes = cellSize
            else:
                xyRes = 10
        
        # ------------------------ File is a 30M USGS File
        elif cellSize.find("0.000277777") >-1 or cellSize.find("277777") >-1:
            #print("\tsource resolution = 30")
            inputSRS = EPSG
            outputSRS = getOutputSRS(top)
            
            if outputSRS == 4326:
                # must remain in degrees
                xyRes = cellSize
            else:
                xyRes = 30
            
        else:
            print(f"\tCellSize is not accounted for: {cellSize}; Will not be projected.")
            return [False,DEMname]
            
        if os.name == 'nt':
            outProjectRaster = f"{ntOutputDir}{os.sep}{DEMname.split('.')[0]}_{outputSRS}.tif"
        else:    
            outProjectRaster = f"{DEMpath}{os.sep}{DEMname.split('.')[0]}_{outputSRS}.tif"

        return [sourceID,rasterPath,outProjectRaster,xyRes,inputSRS,outputSRS,noDataVal]
        
    except:
        AddMsgAndPrint(f"\tFailed to get projection info for {DEMname} \n\t {errorMsg(errorOption=2)}")
        return [False,DEMname]
        
## ================================================================================================================
def projectDEM(projectInfoList):
    
    try:
        gdal.SetConfigOption('GDAL_PAM_ENABLED', 'TRUE')
        gdal.UseExceptions()    # Enable exceptions
        
        messageList = list()
        
        sourceID = projectInfoList[0]
        input_raster = projectInfoList[1]
        out_raster = projectInfoList[2]
        xRes = projectInfoList[3]
        yRes = projectInfoList[3]
        inputSRS = projectInfoList[4]
        outputSRS = projectInfoList[5]
        noData = projectInfoList[6]
        
        # Delete out_raster if it exists and bReplace is True
        if os.path.exists(out_raster):
            if bReplace:
                i=0
                for file in glob.glob(f"{out_raster.split('.')[0]}*"):
                    try:
                        os.remove(file)
                        if i==0:messageList.append(f"Successfully Deleted {file}")
                        i+=1
                    except:
                        messageList.append(f"Failed to Delete {out_raster}; Will not project")
                        return [messageList, [False,input_raster,sourceID]]
            else:
                messageList.append(f"Projected DEM already exists: {os.path.basename(out_raster)} -- Skipping!")
                return [messageList, [True,out_raster,sourceID]]
            
        if not bAlaska:
            # Set source Coordinate system to EPSG from input record
            inSpatialRef = osr.SpatialReference()
            inSpatialRef.ImportFromEPSG(int(inputSRS))

        # Set output Coordinate system to 5070
        outSpatialRef = osr.SpatialReference()
        outSpatialRef.ImportFromEPSG(int(outputSRS))
        
        gdal.SetConfigOption("GDAL_NUM_THREADS","ALL_CPUS")
        gdal.SetConfigOption("GDAL_CACHEMAX","512")
            
        # Projection configuration for Alaska - provide no inputSRS
        if bAlaska:
            args = gdal.WarpOptions(format="GTiff",
                                    xRes=xRes,
                                    yRes=yRes,
                                    srcNodata=noData,
                                    dstNodata=-999999.0,
                                    dstSRS=outSpatialRef,
                                    targetAlignedPixels=True,
                                    resampleAlg=gdal.GRA_Bilinear,
                                    multithread=True,
                                    creationOptions=["COMPRESS=DEFLATE", 
                                                     "TILED=YES",
                                                     "PREDICTOR=2",
                                                     "ZLEVEL=9",
                                                     "TFW=YES",
                                                     "BLOCKXSIZE=256",
                                                     "BLOCKYSIZE=256"])
        else:
            print("----------------------------")
            print(xRes)
            print(noData)
            print(inSpatialRef)
            args = gdal.WarpOptions(format="GTiff",
                                    xRes=xRes,
                                    yRes=yRes,
                                    #srcNodata=noData,
                                    dstNodata=-999999.0,
                                    srcSRS=inSpatialRef,
                                    dstSRS=outSpatialRef,
                                    targetAlignedPixels=True,
                                    resampleAlg=gdal.GRA_Bilinear,
                                    multithread=True,
                                    creationOptions=["COMPRESS=DEFLATE", 
                                                     "TILED=YES",
                                                     "PREDICTOR=2",
                                                     "ZLEVEL=9",
                                                     "TFW=YES",
                                                     "BLOCKXSIZE=256",
                                                     "BLOCKYSIZE=256"])

        g = gdal.Warp(out_raster, input_raster, options=args)
        g = None # flush and close out
        
        messageList.append(f"Successfully projected: {os.path.basename(out_raster)} from EPSG:{inputSRS} to EPSG:{outputSRS}")
        return [messageList,[True,out_raster,sourceID]]
        
    except:
        messageList.append(f"\n\tFailed to Project DEM: {input_raster} \n\t {errorMsg(errorOption=2)}")

        
        return [messageList,[False,input_raster,sourceID]]
    
## ===================================================================================
def createMasterDBfile_MT(projectedDEMsDict,elevMetadataDict):
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

        totalFiles = len(projectedDEMsDict)
        counter = 0

        if os.name == 'nt':
            numOfCores = int(psutil.cpu_count(logical = False))         # 16 workers
        else:
            numOfCores = int(psutil.cpu_count(logical = True) / 2)      # 32 workers

        """ ----------------------------- Step 1: Gather Statistic Information for all rasters ----------------------------- """
        AddMsgAndPrint("\n\tGathering Individual DEM Statistical Information")
        with ThreadPoolExecutor(max_workers=numOfCores) as executor:

            # use a set comprehension to start all tasks.  This creates a future object
            rasterStatInfo = {executor.submit(getRasterInformation_MT, rastItem): rastItem for rastItem in projectedDEMsDict.items()}

            # yield future objects as they are done.
            for stats in as_completed(rasterStatInfo):
                resultDict = stats.result()
                for results in resultDict.items():
                    ID = results[0]
                    rastInfo = results[1]
                    counter +=1

                    if rastInfo.find('#')>-1 or rastInfo.find('None')>-1:
                        badStats+=1
                        print(f"\t\tFailed to retrieve DEM Statistical Information -- {counter:,} of {totalFiles:,}")
                    else:
                        goodStats+=1
                        print(f"\t\tSuccessfully retrieved DEM Statistical Information -- {counter:,} of {totalFiles:,}")

                    demStatDict[ID] = rastInfo

        if goodStats:AddMsgAndPrint(f"\n\t\tSuccessfully Gathered stats for {goodStats:,} DEMs")
        if badStats:AddMsgAndPrint(f"\n\t\tProblems with Gathering stats for {badStats:,} DEMs")

        """ ----------------------------- Step 2: Create Master Elevation File ----------------------------- """

        # Add headers to the beginning of the file if the file is new.
        if not os.path.exists(dlMasterFilePath):

            g = open(dlMasterFilePath,'a+')

            # rast_size, rast_columns, rast_rows, rast_top, rast_left, rast_right, rast_bottom
            # header = ('poly_code,poly_name,prod_title,pub_date,lastupdate,rds_size,format,sourceid,meta_url,'
            #           'downld_url,dem_name,dem_path,rds_column,rds_rows,bandcount,cellsize,rdsformat,bitdepth,nodataval,srs_type,'
            #           'epsg_code,srs_name,rds_top,rds_left,rds_right,rds_bottom,rds_min,rds_mean,rds_max,rds_stdev,blk_xsize,blk_ysize')
            g.write(header)

        # else master elevation file exists
        else:
            g = open(dlMasterFilePath,'a+')

        downldurlIdx = headerValues.index("downld_url")+1

        # Iterate through all of the sourceID files in the download file (elevMetadatDict) and combine with stat info
        for srcID,demInfo in elevMetadataDict.items():

            # Copy all headers up to and including downld_url
            # 10 items from demInfo: poly_code,poly_name,prod_title,pub_date,last_updated,size,format,sourceID,metadata_url,download_url
            firstPart = ','.join(str(e) for e in demInfo[0:downldurlIdx])

            # srcID must exist in dlImgFileDict (successully populated during download)
            if srcID in projectedDEMsDict:
                demFilePath = projectedDEMsDict[srcID]
                demFileName = os.path.basename(demFilePath)
                secondPart = demStatDict[srcID]

                g.write(f"\n{firstPart},{demFileName},{os.path.dirname(demFilePath)},{secondPart}")

            # srcID failed during the download process.  Pass since it will be accounted for in error file
            elif srcID in projectionFailedDict:
                continue

            else:
                AddMsgAndPrint(f"\n\t\tSourceID: {srcID} NO output projection file FOUND -- Inspect this process")

        g.close()
        return dlMasterFilePath

    except:
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
        
        for stat in (columns,rows,bandCount,cellSize,rdsFormat,bitDepth):
            rasterInfoList.append(stat)

        try:
            noDataVal = rdsInfo['bands'][0]['noDataValue']
        except:
            try:
                noDataVal = bandInfo.GetNoDataValue()
            except:
                noDataVal = 'None'

        rasterInfoList.append(noDataVal)

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
        #     print(f"\t\t{os.path.basename(raster)} - Stats are not greater than {noDataVal} -- Calculating")
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
        AddMsgAndPrint(f"\t\tFailed to acquire Stat Information for: {os.path.basename(raster)} \n\t {errorMsg(errorOption=2)}")

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

        recCount = 0
        
        r2pgsqlFilePath = f"{os.path.dirname(elevationMetadataFile)}{os.sep}USGS_3DEP_{resolution}_Step2C_RASTER2PGSQL_reproject.txt"

        # Recreates raster2pgsql file b/c metadata file is passed over; don't want to double up
        g = open(r2pgsqlFilePath,'w')

        total = sum(1 for line in open(masterElevFile)) -1
        #label = "Generating Raster2PGSQL Statements"

        invalidCommands = list()
        
        def getRegion(srid):
            if srid == 5070:
                return "conus"
            elif srid == 3338:
                return "ak"
            elif srid == 32161:
                return "prvi"
            elif srid == 4326:
                return "pb"
            else:
                return "unkown"
          
        # Header value Index Positions
        epsgIdx = headerValues.index("epsg_code")
        demNameIdx = headerValues.index("dem_name")
        demPathIdx = headerValues.index("dem_path")
        
        """ ------------------- Open Master Elevation File and write raster2pgsql statements ---------------------"""
        with open(masterElevFile, 'r') as fp:
            for line in fp:

                # Skip header line and empty lines
                if recCount == 0 or line == "\n":
                    recCount+=1
                    continue

                items = line.split(',')

                # Raster2pgsql parameters
                epsg = items[epsgIdx]
                demPath = f"{items[demPathIdx]}{os.sep}{items[demNameIdx]}"
                
                try:
                    region = getRegion(int(epsg))
                except:
                    invalidCommands.append(f"{demPath} --> raster2pgsql -s {epsg}")
                    continue
                    
                tileSize = '256x256'
                dbName = 'elevation'
                dbTable = f"{region}_elevation_{resolution.lower()}_{epsg}"  # conus_elevation_3m_5070
                demName = items[demNameIdx]
                password = 'itsnotflat'
                localHost = '10.11.11.214'
                port = '6432'

                r2pgsqlCommand = f"raster2pgsql -s {epsg} -b 1 -t {tileSize} -F -a -R {demPath} {dbName}.{dbTable} | PGPASSWORD={password} psql -U {dbName} -d {dbName} -h {localHost} -p {port}"

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

        else:
            AddMsgAndPrint("\tSuccessfully created raster2pgsql file")
        return r2pgsqlFilePath

    except:
        errorMsg()
    
#### ===================================================================================
if __name__ == '__main__':
    
    try:
        funStarts = tic()
        
        """ ---------------------------- Parameters ---------------------------------------------------"""
        # Parameter #1: Path to elevation metadata file
        elevationMetadataFile = input("\nEnter full path to USGS Elevation Metadata file that contains DEMs to project: ")
        while not os.path.exists(elevationMetadataFile):
            print(f"{elevationMetadataFile} does NOT exist. Try Again")
            elevationMetadataFile = input("Enter full path to USGS Elevation Metadata file that contains DEMs to project: ")
            
        # Parameter #2: Replace Projected DEMS: yes or no
        bReplace = input("\nDo you want to replace existing projected DEMs? (Yes/No): ")
        while not bReplace.lower() in ("yes","no","y","n"):
            print("Please Enter Yes or No")
            bReplace = input("Do you want to replace existing data? (Yes/No): ")
    
        if bReplace.lower() in ("yes","y"):
            bReplace = True
        else:
            bReplace = False
            
        # Parameter #2: Replace Projected DEMS: yes or no
        bDeleteOGdems = input("\nDo you want to DELETE the original DEMs? (Yes/No): ")
        while not bDeleteOGdems.lower() in ("yes","no","y","n"):
            print("Please Enter Yes or No")
            bDeleteOGdems = input("Do you want to DELETE the original DEMs? (Yes/No): ")
    
        if bDeleteOGdems.lower() in ("yes","y"):
            bDeleteOGdems = True
        else:
            bDeleteOGdems = False
            
        bDeleteOGdems = False
            
        # Alaska DEMs from OPR will be treated different
        bAlaska = False
        if os.path.basename(elevationMetadataFile).find('AK') >-1:
            bAlaska = True
        
        ntOutputDir = r'D:\projects\DSHub\3mdsh_testing\prjTest'
        
        """ ---------------------------- Establish Console LOG FILE ---------------------------------------------------"""
        tempList = elevationMetadataFile.split(os.sep)[-1].split('_')
        startPos = tempList.index(fnmatch.filter(tempList, '3DEP*')[0]) + 1
        endPos = tempList.index(fnmatch.filter(tempList, 'Step*')[0])
        resolution = '_'.join(tempList[startPos:endPos])
        today = datetime.today().strftime('%m%d%Y')
        
        msgLogFile = f"{os.path.dirname(elevationMetadataFile)}{os.sep}USGS_3DEP_{resolution}_Step2C_Reproject_DEMs_ConsoleMsgs.txt"
    
        h = open(msgLogFile,'w')
        h.write(f"Executing: USGS_2C_ProjectDEMS.py {today}\n\n")
        h.write("User Selected Parameters:\n")
        h.write(f"\tInput Elevation Metadata File: {elevationMetadataFile}\n")
        h.write(f"\tLog File Path: {msgLogFile}\n")
        h.write(f"\tReplace Existing DEMs: {bReplace}\n")
        h.write(f"\tDelete Original DEMs: {bDeleteOGdems}\n")
        h.write(f"\n{'='*125}")
        h.close()

        # List of header values from elevation metadata file
        header =  open(elevationMetadataFile).readline().rstrip()
        headerValues = header.split(',')
        recCount = len(open(elevationMetadataFile).readlines())-1
    
        AddMsgAndPrint(f"There are {recCount:,} {resolution} DEM files in the elevation metadata file")
        
        """ -------------------------- STEP 1: Convert Metadata Elevation to python dict  -------------------"""
        # Convert input metadata elevation file to a dictionary
        # sourceID = [List of all attributes]
        AddMsgAndPrint("\nStep1: Converting input USGS Elevation Metadata File into a dictionary")
        metadataDict = convertMasterDBfileToDict(elevationMetadataFile)
        
        """ -------------------------- STEP 2: Get Projection Info ------------------------------------------"""
        AddMsgAndPrint(f"\nStep2: Determining Projection Info for {len(metadataDict):,} DEMs")
        projectionInfoList = list()
        noProjectionInfoList = list()
    
        with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
    
             # use a set comprehension to start all tasks.  This creates a future object
             getProjectInfo = {executor.submit(getRegionalSRS, item,headerValues): item for item in metadataDict.items()}
    
             # yield future objects as they are done.
             for projectInfo in as_completed(getProjectInfo):
                 result = projectInfo.result()
                 if result[0]:
                     projectionInfoList.append(result)
                 else:
                     noProjectionInfoList.append(result[1])
                     #AddMsgAndPrint(f"\tFailed to get projection Info for: {result[1]}")
    
             del getProjectInfo
             
        if len(noProjectionInfoList):
            AddMsgAndPrint(f"\n\tThere were {len(noProjectionInfoList)} DEM files that will not be projected.")
                           
        """ -------------------------- STEP 3: Project DEMs ------------------------------------------"""
        AddMsgAndPrint(f"\nStep3: Starting Re-Projecting Process of {len(projectionInfoList):,} DEMs:\n")
        projectTimeStart = tic()
        projectionFailedDict = dict()
        projectedDEMsDict = dict()    # {sourceID:projectedDEM}
        numOfDEMstoProject = len(projectionInfoList)
        j = 1

        with open(msgLogFile, 'a+') as f:
            with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        
                 # use a set comprehension to start all tasks.  This creates a future object
                 projectDEMs = {executor.submit(projectDEM, infoList): infoList for infoList in projectionInfoList}
        
                 # result = [messageList,[True,out_raster,sourceID]]
                 for projectResults in as_completed(projectDEMs):
                     result = projectResults.result()
                     if not result[1][0]:
                         projectionFailedDict[result[1][2]] = result[1][1]
                     else:
                         projectedDEMsDict[result[1][2]] = result[1][1]
                        
                     i=0
                     for msg in result[0]:
                        if i == 0:
                            f.write(f"\n\t{msg} -- ({j:,} of {numOfDEMstoProject:,})")
                            print(f"\t{msg} -- ({j:,} of {numOfDEMstoProject:,})")
                        else:
                            f.write(f"\n\t{msg}")
                            print(f"\t{msg}")
                        i+=1
                     j+=1
                         
                 del projectDEMs
                     
        projectTimeEnd = toc(projectTimeStart)
        
        """ ----------------------------- Step 4: Create Elevation Metadata File ---------------------------------------- """
        bMasterFile = False
        if len(projectedDEMsDict):
            dlMasterTimeStart = tic()
            AddMsgAndPrint("\nStep 4: Creating Elevation Metadata File for Projected DEMs")
            dlMasterFilePath = f"{os.path.dirname(elevationMetadataFile)}{os.sep}USGS_3DEP_{resolution}_Step2C_Elevation_Metadata_reproject.txt"
            dlMasterFile = createMasterDBfile_MT(projectedDEMsDict,metadataDict)
            dlMasterTimeStop = toc(dlMasterTimeStart)
            bMasterFile = True
            
        """ ----------------------------- Step 5: Create Raster2pgsql File ---------------------------------------------- """
        r2pgsqlTimeStart = tic()
        if bMasterFile:    
            AddMsgAndPrint("\nStep 5: Creating Raster2pgsql File\n")
            r2pgsqlFile = createRaster2pgSQLFile(dlMasterFilePath)
            #AddMsgAndPrint("\tIMPORTANT: Make sure dbTable variable (elevation_3m) is correct in Raster2pgsql file!!")
            r2pgsqlTimeStop = toc(r2pgsqlTimeStart)
        else:
            AddMsgAndPrint("\nDSH3M Raster2pgsql File will NOT be created")
            r2pgsqlTimeStop = toc(r2pgsqlTimeStart)
            
        """ ----------------------------- Step 6: Delete original files ---------------------------------------------- """
        if bDeleteOGdems:
            if len(projectionFailedDict) == 0:
                AddMsgAndPrint("\nStep 6: Deleting Original DEMs:")
                for k,v in metadataDict.items():
                    demName = v[headerValues.index("dem_name")]
                    demPath = v[headerValues.index("dem_path")]
                    fileToDelete = f"{os.path.join(demPath,demName).split('.')[0]}.*"
                    deletedFile = 0
                    invalidFiles = 0
                    
                    i = 0
                    for file in glob.glob(fileToDelete):
                        if os.path.isfile(file):
                            try:
                                os.remove(file)
                                if i==0:print(f"\n\tSuccessfully Deleted: {file}")
                                deletedFile+=1
                                i+=1
                            except:
                                AddMsgAndPrint(f"\Failed to Delete: {file}")
                        else:
                            AddMsgAndPrint(f"\tInvalid file: {file}")
                            invalidFiles+=1
            else:
                AddMsgAndPrint("\nOriginal Files will not be deleted b\c there were {len(projectionFailedDict)} DEMs that failed to be re-projected")
                 
        """ ------------------------------------ SUMMARY -------------------------------------------- """
        AddMsgAndPrint(f"\n{'-'*40}SUMMARY{'-'*40}")
        
        AddMsgAndPrint(f"\nTotal # of DEMs projected: {len(projectedDEMsDict):,}")
        if len(projectionFailedDict):
            AddMsgAndPrint(f"\n\tThere were {len(projectionFailedDict)} DEMs that failed to be re-projected")
    
        AddMsgAndPrint(f"\nTotal Processing Time: {toc(funStarts)}")
        AddMsgAndPrint(f"\tProjecting DEMs Time: {projectTimeEnd}")
        AddMsgAndPrint(f"\tCreating Elevation Metadata file Time: {dlMasterTimeStop}")
        AddMsgAndPrint(f"\tCreating Raster2pgsql file Time: {r2pgsqlTimeStop}")
        
        # Summarize projection info
        # prjLookUp = {}
        if len(projectionInfoList):
            AddMsgAndPrint("\nEPSG Summary:")
            epsgs = [f[5] for f in projectionInfoList]
            epsgSummary = Counter(epsgs)
            for epsg,count in epsgSummary.items():
                AddMsgAndPrint(f"\tEPSG: {epsg} -- # of DEMs: {count}")
                
        if bMasterFile:
            AddMsgAndPrint(f"\nElevation Metadata File Path: {dlMasterFile}")
            AddMsgAndPrint(f"Raster2pgsql File Path: {r2pgsqlFile}")
            
    except:
        try:
            errorMsg(errorOption=1)
        except:
            errorMsg()
                     