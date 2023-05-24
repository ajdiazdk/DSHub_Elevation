#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Adolfo.Diaz
#
# Created:     06/04/2023
# Copyright:   (c) Adolfo.Diaz 2023
# Licence:     <your licence>
#-------------------------------------------------------------------------------

"""
    USGS_1M_16_x54y443_IN_Indiana_Statewide_LiDAR_2017_B17.tif

    QGIS Settings
    gdalwarp
    -s_srs EPSG:26916
    -t_srs EPSG:5070
    -dstnodata -999999.0 -
    tr 3.0 3.0
    -r bilinear
    -te 793560.0 1913550.0 819789.0 1933170.0
    -multi
    -ot Float32
    -of GTiff D:/projects/DSHub/Elevation/1M/USGS_1M_16_x54y443_IN_Indiana_Statewide_LiDAR_2017_B17.tif D:/projects/DSHub/reampling/1M_5070/QGIS_5070_snap_bilinear_3M.tif

    Input Extent (EPSG 26919): catalog vs describe (same as GDAL)
        Top (ymax): 4430006.0     (4430006.000286639)
        Bottom (ymin): 4419994.0  (4419994.000286639)
        Left (xmin): 539994.0     (539993.9996601711)
        Right (xmax): 550006.0    (550005.9996601711)
        rows,cols = 10012x10012

    CONUS Snap Grid (aoi3M3.tif) Extent Coords
        Top (ymax): 3177582.0
        Bottom (ymin): 271437.0
        Left (xmin): -2361114.0
        Right (xmax): 2268768.0

    Output (Pro_5070_snap_bilinear_3M.tif) Extent (EPSG 5070)
        Top (ymax): 1929951.0
        Bottom (ymin): 1918950.0
        Left (xmin): 799722.0
        Right (xmax): 810561.0

    Output #2 (Pro_5070_snap_cube_1M.tif) Extent (EPSG 5070)
        Top (ymax): 1929949.0
        Bottom (ymin): 1918950.0
        Left (xmin): 799722.0
        Right (xmax): 810559.0

---------------- UPDATES
4/20/2023
Added source and destination nodata arguments to the warp function to harmonize the nodata values from
the warped outputs.  1M and 10M nodata values are set to -999999.0.  3M nodata values are set to
3.4028234663852886e+38.  The destination nodata values for all warped outputs will be set to -999999.0.

THINGS TO ADD:
    - Read Shapefile headers in a list to look up field position index instead of hard coding position

"""

import os, traceback, sys, time
import threading, multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime
from osgeo import gdal
from osgeo import osr
from osgeo import ogr
import geopandas as gp
from operator import itemgetter

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
        AddMsgAndPrint("Converting input Metadata Elevation File into a dictionary")
        mDBFnumOfRecs = len(open(elevMetdataFile).readlines())

        # Find sourceID in headerValues; return False if not found
        sourceIDidx = headerValues.index('sourceID')

        masterDBfileDict = dict()
        recCount = 0
        badLines = 0

        """ ---------------------------- Open Download File and Parse Information ----------------------------------"""
        with open(elevMetdataFile, 'r') as fp:
            for line in fp:
                items = line.split(',')
                items[-1] = items[-1].strip('\n')  # remove the hard return of the line

                # Skip header line and empty lines
                if recCount == 0 or line == "\n":
                    recCount+=1
                    continue

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

                sourceID = items[sourceIDidx]

                # Add info to elevMetadataDict
                masterDBfileDict[sourceID] = items
                recCount+=1
        del fp

        if len(masterDBfileDict) == recCount:
            return masterDBfileDict

        if len(masterDBfileDict) == 0:
            AddMsgAndPrint(f"\tElevation Metadata File: {os.path.basename(masterDBfile)} was empty!")
            return False

        if badLines > 0:
            AddMsgAndPrint(f"\tThere are(is) {badLines} records with anomalies found")

        return masterDBfileDict

    except:
        errorMsg()
        return False

## ===================================================================================
def createMultiResolutionOverlay_gp(indexLayer, gridLayer):

    try:
        # Read the best available 3M index
        index = gp.read_file(indexLayer)

        # Read the AOI grid
        grid = gp.read_file(gridLayer)

    except:
        AddMsgAndPrint("\nFailed to open input layers")
        AddMsgAndPrint(errorMsg())

    demDict = dict()

    oneMcnt = 0
    threeMcnt = 0
    tenMcnt = 0
    gridCnt = 0
    recCnt = 0

    for g in grid.index:

        # isolate grid as an aoi to use as a mask
        mask = grid.iloc[[g]].copy()

        # RID value of grid i.e. 332
        rid = mask.rid[g]

        # clip index using current aoi
        idxClip = gp.clip(index, mask)
        idxClip.reset_index(inplace=True)

        # Lists of DEM records that are within current aoi
        listOfDEMlists = list()

        # iterate through each DEM record and capture all attributes
        for dems in idxClip.index:

            vals = idxClip.iloc[dems].copy()
            vals.drop(labels = 'geometry', inplace=True)
            vals = vals.tolist()
            del vals[0]
            source = vals[-1]

            if source == 1:
                oneMcnt+=1
            elif source == 3:
                threeMcnt+=1
            else:
                tenMcnt+=1

            recCnt+=1
            listOfDEMlists.append(vals)

        # Sort all lists by resolution and last_update date
        dateSorted = sorted(listOfDEMlists, key=itemgetter(31,3), reverse=True)
        demDict[rid] = dateSorted

        gridCnt+=1

    AddMsgAndPrint(f"\tThere are {gridCnt} grid tiles that overlayed with {recCnt} DEMs:")
    AddMsgAndPrint(f"\t\t1M DEMs:  {oneMcnt:,}")
    AddMsgAndPrint(f"\t\t3M DEMs:  {threeMcnt:,}")
    AddMsgAndPrint(f"\t\t10M DEMs: {tenMcnt:,}")

    return demDict

## ===================================================================================
def createMultiResolutionOverlay(idx,grid):
    """
    This function returns 2 dictionaries:
     updates:
        1) don't limit to shapefile - use ogr to determine appropriate driver
        2) check grid cell to make sure it is single part - never know

    """

    try:
        driver = ogr.GetDriverByName('ESRI Shapefile')

        idx_ds = driver.Open(idx, 0) # 0 means read-only. 1 means writeable.
        grid_ds = driver.Open(grid, 0)

        # Validate SoilE3M Elevation Index
        if idx_ds is None:
            AddMsgAndPrint(f"\tERROR: Could not open DSH3M Index Layer: {dsh3mIdxShp} -- EXITING!")
            return False,False
        else:
            idx_Lyr = idx_ds.GetLayer()
            idxFeatCount = idx_Lyr.GetFeatureCount()
            layerDefinition = idx_Lyr.GetLayerDefn()
            numOfFields = layerDefinition.GetFieldCount()
            AddMsgAndPrint(f"\tNumber of DSH3M Index features in {os.path.basename(idx)}: {idxFeatCount}")

        # Validate grid layer
        if grid_ds is None:
            AddMsgAndPrint(f"\tERROR: Could not open Grid Layer: {grid} -- EXITING!")
            return False,False
        else:
            gridLyr = grid_ds.GetLayer()
            gridFeatCount = gridLyr.GetFeatureCount()
            AddMsgAndPrint(f"\tNumber of Grid Cells in {os.path.basename(gridShp)}: {gridFeatCount}")

        # {332: [-491520.0, 1720320.0, -368640.0, 1843200.0]}
        gridExtentDict = dict()
        gridIndexOverlayDict = dict()

        # iterate through grid, get extent and use it as a spatial filter for the idx
        for gridCell in gridLyr:
            rid = gridCell.GetField("rid")

            # Return the feature geometry
            cellGeom = gridCell.GetGeometryRef()
            envelope = cellGeom.GetEnvelope()
            xmin = envelope[0]
            xmax = envelope[1]
            ymin = envelope[2]
            ymax = envelope[3]

            AddMsgAndPrint(f"\t\tGrid RID: {rid} -- Extent: {envelope}")
            gridExtentDict[rid] = [xmin,ymin,xmax,ymax]

            # apply grid cell spatial filter to index layer
            idx_Lyr.SetSpatialFilter(cellGeom)

            # Lists of DEM records that are within current aoi
            listOfDEMlists = list()
            numOfSelectedFeats = 0

            # iterate through every feature and get DEM information
            for idxFeat in idx_Lyr:

                # List of attributes for 1 DEM
                #
                idxFeatValList = list()

                # iterate through feature's attributes
                for i in range(numOfFields):
                    val = idxFeat.GetField(layerDefinition.GetFieldDefn(i).GetName())
                    idxFeatValList.append(val)

                listOfDEMlists.append(idxFeatValList)
                numOfSelectedFeats+=1

            # No DEMs intersected with this Grid
            if numOfSelectedFeats == 0:
                AddMsgAndPrint(f"\t\t\tWARNING: There are no DEMs that intersect this grid")
                continue

            # Sort all lists by resolution and last_update date
            dateSorted = sorted(listOfDEMlists, key=itemgetter(31,3), reverse=True)
            gridIndexOverlayDict[rid] = dateSorted

            # Summarize the selected DEMs by resolution - unnecessary but useful
            oneMcnt = 0
            threeMcnt = 0
            tenMcnt = 0
            other = 0

            for demList in listOfDEMlists:
                if demList[-1] == 1:
                    oneMcnt+=1
                elif demList[-1] == 3:
                    threeMcnt+=1
                elif demList[-1] == 10:
                    tenMcnt+=1
                else:
                    other+=1

            AddMsgAndPrint(f"\t\t\tThere are {numOfSelectedFeats:,} DEMs that intersect this grid:")
            AddMsgAndPrint(f"\t\t\t\t1M DEMs:  {oneMcnt:,}")
            AddMsgAndPrint(f"\t\t\t\t3M DEMs:  {threeMcnt:,}")
            AddMsgAndPrint(f"\t\t\t\t10M DEMs: {tenMcnt:,}")

        # Close data sources
        del driver
        idx_ds = None
        grid_ds = None

        return gridIndexOverlayDict,gridExtentDict

    except:
        errorMsg()
        return False,False


## ===================================================================================
def getEBSfolder(gridID):
    # /data03/gisdata/elev/09/0904/090400/09040004/

    try:

        # last digit
        ld = str(gridID)[-1]

        if ld in ["1","9","0"]:
            folder = "/data02/gisdata/elev/dsh3m"
        elif ld in ["2","7","8"]:
            folder = "/data03/gisdata/elev/dsh3m"
        elif ld in ["3","6"]:
            folder = "/data04/gisdata/elev/dsh3m"
        elif ld in ["4","5"]:
            folder = "/data05/gisdata/elev/dsh3m"

        if not os.path.exists(folder):
            os.makedirs(folder)

        return folder

    except:
        errorMsg()
        return False

## ===================================================================================
def createSoil3MDEM(item):
    """
    item is passed over as a tuple with the key = 0 and the values = 1
    returns tuple """

    try:
        messageList = list()

        # Positions of individual field names
        last_update = item[headerValues.index("last_updat")]
        fileFormat = item[headerValues.index("format")]
        sourceID = item[headerValues.index("sourceID")]
        DEMname = item[headerValues.index("DEMname")]
        DEMpath = item[headerValues.index("DEMpath")]
        EPSG = item[headerValues.index("EPSG")]
        noData = float(item[headerValues.index("noDataVal")]) # returned as string
        srsName = item[headerValues.index("srsName")]
        top = item[headerValues.index("top")]
        left = item[headerValues.index("left")]
        right = item[headerValues.index("right")]
        bottom = item[headerValues.index("bottom")]
        source = item[headerValues.index("source")]

        if bDetails:
            messageList.append(f"\n\t\t\tProcessing DEM: {DEMname} -- {source}M")
            theTab = "\t\t\t\t"
        else:
            theTab = "\t\t\t"

        # D:\projects\DSHub\reampling\1M\USGS_1M_Madison.tif
        input_raster = os.path.join(DEMpath,DEMname)

        if not os.path.exists(input_raster):
            messageList.append(f"{theTab}{DEMname} does NOT exist! Skipping!")
            failedDEMs.append(sourceID)

        # D:\projects\DSHub\reampling\1M\USGS_1M_Madison_EPSG5070.tif
        #out_raster = f"{input_raster.split('.')[0]}_dsh3m.{input_raster.split('.')[1]}"
        out_raster = f"{input_raster.split('.')[0]}_dsh3m.tif"
        messageList.append(f"\n{theTab}Output DEM: {out_raster}")

        dsh3mList = [sourceID,last_update,out_raster,source]

        # DSH3M DEM exists b/c it was currently generated
        if sourceID in dsh3mStatDict:
            messageList.append(f"{theTab}{'DSH3M DEM Exists from grid overlap':<25} {os.path.basename(out_raster):<60}")
            return (messageList,dsh3mList)

        # DSH3M DEM exists b/c it was previously generated
        if os.path.exists(out_raster):
            try:
                if bReplace:
                    os.remove(out_raster)
                    messageList.append(f"{theTab}Successfully Deleted {os.path.basename(out_raster)}")
                else:
                    messageList.append(f"{theTab}{'DSH3M DEM Exists':<25} {os.path.basename(out_raster):<60}")
                    return (messageList,dsh3mList)
            except:
                messageList.append(f"{theTab}{'Failed to Delete':<35} {out_raster:<60}")
                failedDEMs.append(sourceID)
                return (messageList,[sourceID,last_update,])

        rds = gdal.Open(input_raster)
        rdsInfo = gdal.Info(rds,format="json")

        # Set source Coordinate system to EPSG from input record
        inSpatialRef = osr.SpatialReference()
        inSpatialRef.ImportFromEPSG(int(EPSG))
        inputSRS = f"EPSG:{inSpatialRef.GetAuthorityCode(None)}"

        # Degrees vs Meters
        inputUnits = inSpatialRef.GetAttrValue('UNIT')

        # Set output Coordinate system to 5070
        outSpatialRef = osr.SpatialReference()
        outSpatialRef.ImportFromEPSG(5070)
        outputSRS = f"EPSG:{outSpatialRef.GetAuthorityCode(None)}"

        # create Transformation
        coordTrans = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)
        #messageList.append(f"\n\t Projecting from {inputSRS} to {outputSRS}")

        # ----------------------- Project extent coords to 5070 --------------------------------
        # Create a geometry object of X,Y coordinates for LL, UL, UR, and LR in the source SRS
        # This represents the coords from the raster extent and will be projected to 5070
        # Degree coordinates are passed in lat/long (Y,X) format; all others are passed in as X,Y

        if inputUnits == 'degree':
            pointLL = ogr.CreateGeometryFromWkt("POINT ("+str(bottom)+" " +str(left)+")")
            pointUL = ogr.CreateGeometryFromWkt("POINT ("+str(top)+" " +str(left)+")")
            pointUR = ogr.CreateGeometryFromWkt("POINT ("+str(top)+" " +str(right)+")")
            pointLR = ogr.CreateGeometryFromWkt("POINT ("+str(bottom)+" " +str(right)+")")
        else:
            pointLL = ogr.CreateGeometryFromWkt("POINT ("+str(left)+" " +str(bottom)+")")
            pointUL = ogr.CreateGeometryFromWkt("POINT ("+str(left)+" " +str(top)+")")
            pointUR = ogr.CreateGeometryFromWkt("POINT ("+str(right)+" " +str(top)+")")
            pointLR = ogr.CreateGeometryFromWkt("POINT ("+str(right)+" " +str(bottom)+")")

        # Coordinates in native SRS
        if bDetails:
            messageList.append(f"\n{theTab}------------ {inputSRS} Exents ------------")
            messageList.append(f"{theTab}LL Coords - {pointLL} {'(lat,long)' if inputUnits == 'degree' else '(Xmin,Ymin)'}")
            messageList.append(f"{theTab}UL Coords - {pointUL} {'(lat,long)' if inputUnits == 'degree' else '(Xmin,Ymax)'}")
            messageList.append(f"{theTab}UR Coords - {pointUR} {'(lat,long)' if inputUnits == 'degree' else '(Xmax,Ymax)'}")
            messageList.append(f"{theTab}LR Coords - {pointLR} {'(lat,long)' if inputUnits == 'degree' else '(Xmax,Ymin)'}")

        # Project individual coordinates to 5070
        # 'POINT (800676.587222594 1918952.70626254)'
        pointLL.Transform(coordTrans)
        pointUL.Transform(coordTrans)
        pointUR.Transform(coordTrans)
        pointLR.Transform(coordTrans)

        # Coordinates in 5070
        if bDetails:
            messageList.append(f"\n{theTab}------------ {outputSRS} Exents ------------")
            messageList.append(f"{theTab}LL Coords - {pointLL} (Xmin,Ymin)")
            messageList.append(f"{theTab}UL Coords - {pointUL} (Xmin,Ymax)")
            messageList.append(f"{theTab}UR Coords - {pointUR} (Xmax,Ymax)")
            messageList.append(f"{theTab}LR Coords - {pointLR} (Xmax,Ymin)")

        # Convert the Transform object into a List of projected coordinates and extract
        # [(1308220.3216564057, -526949.4675336559)]
        prjLL = pointLL.GetPoints()[0]
        prjUL = pointUL.GetPoints()[0]
        prjUR = pointUR.GetPoints()[0]
        prjLR = pointLR.GetPoints()[0]

        # ----------------------- Unsnapped 5070 Extent --------------------------------
        # Truncate coordinates to remove precion otherwise mod%3 would never equal 0
        # Get the highest Y-coord to determine the most northern (top) extent - Ymax
        newTop = int(max(prjUL[1],prjUR[1]))

        # Get the lowest Y-coord to determine the most southern (bottom) extent - Ymin
        newBottom = int(min(prjLL[1],prjLR[1]))

        # Get the lowest X-coord to determine the most western (left) extent - Xmin
        newLeft = int(min(prjUL[0],prjLL[0]))

        # Get the highest X-coord to determine the most eastern (right) extent - Xmax
        newRight = int(max(prjUR[0],prjLR[0]))

        # ----------------------- Snapped 5070 Extent --------------------------------
        # update extent values so that they are snapped to a aoi3M cell.
        # new extent value will be divisible by 3.

        # Extent position 0-newTop, 1-newRight
        extPos = 0

        for extent in [newTop,newRight,newBottom,newLeft]:

            bDivisibleBy3 = False
            while not bDivisibleBy3:

                # Extent is evenly divisible by 3; update new coordinate value
                if extent % 3 == 0:

                    # Update extent variables to one more 3M cell
                    if extPos == 0:
                        newTop = extent + 3
                    elif extPos == 1:
                        newRight = extent + 3
                    elif extPos == 2:
                        newBottom = extent - 3
                    elif extPos == 3:
                        newLeft = extent - 3

                    extPos+=1
                    bDivisibleBy3 = True

                # Extent is not evenly divisible by 3; grow by 1M
                else:
                    # Top and Right coordinates will add 1 meter;
                    # Left and Bottom coordinates will subtract 1 meter
                    if extPos <= 1:
                        extent = extent + 1
                    else:
                        extent = extent - 1

        messageList.append(f"\n{theTab}------------ {outputSRS} Snapped Exents ------------")
        messageList.append(f"{theTab}LL Coords - POINT ({newLeft} {newBottom}) (Left,Bottom)")
        messageList.append(f"{theTab}UL Coords - POINT ({newLeft} {newTop}) (Left,Top)")
        messageList.append(f"{theTab}UR Coords - POINT ({newRight} {newTop}) (Right,Top)")
        messageList.append(f"{theTab}LR Coords - POINT ({newRight} {newBottom}) (Right,Bottom)")

        if fileFormat == 'GeoTIFF':
            outputFormat = 'GTiff'
        else:
            outputFormat = 'HFA' # Erdas Imagine .img

        # Add srcNodata and dstNodata
        # removed srcNodata and made output format TIFF; regardless of inputs
        args = gdal.WarpOptions(format="GTiff",
                                xRes=3,
                                yRes=3,
                                #srcNodata=noData,
                                dstNodata=-999999.0,
                                srcSRS=inputSRS,
                                dstSRS=outputSRS,
                                outputBounds=[newLeft, newBottom, newRight, newTop],
                                outputBoundsSRS=outputSRS,
                                resampleAlg=gdal.GRA_Bilinear,
                                multithread=True)
                                #creationOptions=["COMPRESS=DEFLATE", "TILED=YES","PREDICTOR=2","ZLEVEL=9","PROFILE=GeoTIFF"])

        g = gdal.Warp(out_raster, input_raster, options=args)
        g = None # flush and close out

        if bDetails:
            messageList.append(f"\n{theTab}Successfully Created Soil3M DEM: {os.path.basename(out_raster)}")
        else:
            messageList.append(f"{theTab}{'Successfully Created Soil3M DEM:':<35} {os.path.basename(out_raster):>60}")

        # [sourceID,last_update,out_raster,source]
        return (messageList,dsh3mList)

    except:
        failedDEMs.append(sourceID)
        messageList.append(f"\n{theTab}{errorMsg(errorOption=2)}")
        return (messageList,[])


## ===================================================================================
def mergeGridDEMs(listsOfDEMs):

    try:

        AddMsgAndPrint(f"\n\tGrid ID: {gridID} (Grid {gridCounter} of {totalNumOfGrids}) has {len(dsh3mRasters):,} DSH3M DEMs that will be merged")

        mergeStart = tic()
        dateSorted = sorted(dsh3mRasters, key=itemgetter(3,1), reverse=True)

        # isolate the dsh3m raster into a list
        mosaicList = list()
        for raster in dateSorted:
            mosaicList.append(raster[2])

        if os.name == 'nt':
            gridFolder = r'D:\projects\DSHub\reampling\TEST'
        else:
            gridFolder = getEBSfolder(gridID)

        gridRaster = os.path.join(gridFolder,f"dsh3m_grid{gridID}.tif")

        if os.path.exists(gridRaster):
            try:
                if bReplaceMerge:
                    os.remove(gridRaster)
                    AddMsgAndPrint(f"\t\tSuccessfully Deleted {os.path.basename(gridRaster)}")
                else:
                    AddMsgAndPrint(f"\t\t{'Merged DEM Exists':<25} {os.path.basename(gridRaster):<60}")
                    return True
            except:
                AddMsgAndPrint(f"\t\t{'Failed to Delete':<35} {gridRaster:<60}")
                return False

        clipExtent = gridExtentDict[gridID]

        args = gdal.WarpOptions(format="GTiff",
                                xRes=3,
                                yRes=3,
                                srcNodata=-999999.0,
                                dstNodata=-999999.0,
                                outputBounds=clipExtent,
                                outputBoundsSRS=srs,
                                srcSRS=srs,
                                dstSRS=srs,
                                creationOptions=["COMPRESS=DEFLATE", "TILED=YES","PREDICTOR=2","ZLEVEL=9",
                                                 "BIGTIFF=YES","PROFILE=GeoTIFF"],
                                multithread=True)
                                #options=["COMPRESS=LZW", "TILED=YES"])
                                #options="__RETURN_OPTION_LIST__")

        g = gdal.Warp(gridRaster,mosaicList,options=args)
        g = None

        mergeStop = toc(mergeStart)
        AddMsgAndPrint(f"\tSuccessfully Merged. Merge Time: {mergeStop}")
        return True

    except:
        errorMsg()
        return False

## ===================================================================================
def createElevMetadataFile_MT(dsh3mRasters,idxShp):

    # updates metadata file for soil3m DEMS
    # soil3Mdict: sourceID = rasterPath

    try:

        # key: sourceID -- value: [col,rows,bandcnt,cell,....] 20 stats
        demStatDict = dict()
        goodStats = 0
        badStats = 0
        recCount = len(dsh3mRasters)
        i = 1 # progress tracker

        """ --------------------- Step1: Gather Statistic Information ------------------------------------------"""
        AddMsgAndPrint(f"\n\tGathering Individual DEM Statistical Information")
        with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:

            # use a set comprehension to start all tasks.  This creates a future object
            rasterStatInfo = {executor.submit(getRasterInformation_MT, rastItem): rastItem for rastItem in dsh3mRasters.items()}

            # yield future objects as they are done.
            for stats in as_completed(rasterStatInfo):
                resultDict = stats.result()
                for results in resultDict.items():
                    ID = results[0]
                    rastInfo = results[1]

                    if rastInfo.find('#')>-1:
                        badStats+=1
                        AddMsgAndPrint(f"\t\tError in retrieving raster info -- ({i:,} of {recCount:,})")
                    else:
                        goodStats+=1

                        # No need to log this
                        print(f"\t\tSuccessfully retrieved raster info -- ({i:,} of {recCount:,})")
                    i+=1
                    demStatDict[ID] = rastInfo

        AddMsgAndPrint(f"\n\tSuccessfully Gathered stats for {goodStats:,} DEMs")

        if badStats > 0:
            AddMsgAndPrint(f"\tProblems with Gathering stats for {badStats:,} DEMs")

        """ --------------------- Step2: Make a copy of Index shapefile ---------------------------------"""
        idx_ds = ogr.GetDriverByName('ESRI Shapefile').Open(idxShp)
        idx_Lyr = idx_ds.GetLayerByIndex(0)
        idx_defn = idx_Lyr.GetLayerDefn()

        idxCopyShpPath = f"{os.path.dirname(idxShp)}{os.sep}{os.path.basename(idxShp).split('.')[0]}_DSH3M.shp"

        # Remove output shapefile if it already exists
        if os.path.exists(idxCopyShpPath):
            outDriver = ogr.GetDriverByName("ESRI Shapefile")
            outDriver.DeleteDataSource(idxCopyShpPath)
            AddMsgAndPrint(f"\n\tSuccessfully Deleted Existing {idxCopyShpPath} Spatial Footprint Index")
            del outDriver

        output_ds = ogr.GetDriverByName('ESRI Shapefile').CreateDataSource(idxCopyShpPath)
        output_Lyr = output_ds.CreateLayer('output_layer', idx_Lyr.GetSpatialRef(), idx_Lyr.GetGeomType())

        # Copying the old layer schema into the new layer
        for i in range(idx_defn.GetFieldCount()):
            output_Lyr.CreateField(idx_defn.GetFieldDefn(i))

        # Copying the features
        for feat in idx_Lyr:
            output_Lyr.CreateFeature(feat)

        # Save and close DataSources
        output_ds = None
        idx_ds = None
        AddMsgAndPrint(f"\tSuccessfully made a copy of {os.path.basename(idxShp)} --> {os.path.basename(idxCopyShpPath)}")

        """ --------------------- Step3: Open Index shapefile and update stats ---------------------------------"""
        missingRecords=0
        idxCopy_ds = ogr.GetDriverByName('ESRI Shapefile').Open(idxCopyShpPath)
        #idxCopy_Lyr = idxCopy_ds.GetLayerByIndex(0)
        idxCopy_Lyr = idxCopy_ds.GetLayer(0)
        layerDefinition = idxCopy_Lyr.GetLayerDefn()

        # Int - shapefile field count
        fieldCount = layerDefinition.GetFieldCount()

        # List of shapefile field names
        fieldList = [layerDefinition.GetFieldDefn(i).GetName() for i in range(fieldCount)]

        # this is the range of field attributes that will be updated.
        startPos = fieldList.index('columns')
        stopPos = fieldList.index('blockYsize')
        #stopPos = fieldList.index('blk_ysize')

        # iterate through DEM tiles and update statistics
        for feat in idxCopy_Lyr:
            # sourceID of the record
            sourceID = feat.GetField("sourceID")
            AddMsgAndPrint(f"\tUpdating sourceID: {sourceID}")

            if not sourceID in demStatDict:
                #AddMsgAndPrint("Could not update shapefile record for sourceID: {sourceID}")
                missingRecords+=1
                continue

            rastStatList = demStatDict[sourceID].split(',')

            # iterate through fields and update attributes
            j=0
            for i in range(startPos,stopPos+1):
                oldVal = feat.GetField(fieldList[i])
                feat.SetField(fieldList[i], rastStatList[j])
                AddMsgAndPrint(f"\t\tUpdating field {fieldList[i]} FROM: {oldVal} TO: {rastStatList[j]}")
                j+=1
            idxCopy_Lyr.SetFeature(feat)

        idxCopy_ds.Destroy()
        del idxCopy_ds

        if missingRecords:
            AddMsgAndPrint(f"\n\tThere were {missingRecords:,} DSH3M footprints whose attributes were not updated")

        return idxCopyShpPath

        # Create Master Elevation File
        soil3MmetadataFile = f"{os.path.dirname(elevationMetadataFile)}{os.sep}USGS_3DEP_{resolution}_Step4_DSH3M_Elevation_Metadata.txt"

        g = open(soil3MmetadataFile,'a+')
        header = ','.join(str(e) for e in headerValues)
        g.write(header)

        total = len(elevMetadataDict)
        index = 1

        # Iterate through all of the sourceID files in the download file (elevMetadatDict)
        for srcID,demInfo in elevMetadataDict.items():

            # 9 item INFO: huc_digit,prod_title,pub_date,last_updated,size,format,sourceID,metadata_url,download_url
            firstPart = ','.join(str(e) for e in demInfo[0:9])

            # srcID must exist in soil3Mdict (successully populated during download)
            if srcID in soil3Mdict:
                demFilePath = soil3Mdict[srcID]
                demFileName = os.path.basename(demFilePath)
                secondPart = demStatDict[srcID]

                g.write(f"\n{firstPart},{demFileName},{os.path.dirname(demFilePath)},{secondPart}")

            # srcID failed during the download process.  Pass since it will be accounted for in error file
            elif srcID in failedDEMs:
                AddMsgAndPrint(f"\n\t\tWARNING: SourceID: {srcID} does not exist in dsh3mDict; accounted for in failedDEMs")
                continue

            else:
                AddMsgAndPrint(f"\n\t\tWARNING: SourceID: {srcID} does not exist in dsh3mDict; NOT accounted for in failedDEMs -- Inspect")
                continue

        g.close()
        return soil3MmetadataFile

    except:
        errorMsg()
        try:
            g.close()
        except:
            pass
        return False


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
                #print(f"\t\t{os.path.basename(raster)} - Stats are set to 0 -- Calculating")
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
            #print(f"\t\t{os.path.basename(raster)} Stats not present in info -- Forcing Calc'ing of raster info")
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

        rasterInfoList = [columns,rows,bandCount,cellSize,rdsFormat,bitDepth,noDataVal,srsType,epsg,srsName,
                          top,left,right,bottom,minStat,meanStat,maxStat,stDevStat,blockXsize,blockYsize]

        rasterStatDict[srcID] = ','.join(str(e) for e in rasterInfoList)
        return rasterStatDict

    except:
        errorMsg()
        rasterStatDict[srcID] = ','.join('#'*20)
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
        masterElevRecCount = len(open(masterElevFile).readlines()) - 1  # subtract header

        recCount = 0
        r2pgsqlFilePath = f"{os.path.dirname(elevationMetadataFile)}{os.sep}USGS_3DEP_{resolution}_Step4_DSH3M_RASTER2PGSQL.txt"

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
                dbTable = f"elevation_DSH3M"  # elevation_3m
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

                # No need to log status to file
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
        else:
            AddMsgAndPrint(f"\tSuccessfully created RASTER2PGSQL commands for {total:,} DEM files")

        return r2pgsqlFilePath

    except:
        errorMsg()


## ===================================================================================
if __name__ == '__main__':

    try:

        """---------------------------------  Setup ---------------------------------- """
        # Script Parameters
        # File from Step#2
        #dsh3mIdxShp = r'D:\projects\DSHub\reampling\snapGrid\USGS_DSH3M_Sample.shp'
        dsh3mIdxShp = r'D:\projects\DSHub\reampling\dsh3m\USGS_DSH3M_Pro.shp'
        gridShp = r'D:\projects\DSHub\reampling\snapGrid\grid_122880m_367_.shp'
        bMultiThreadMode = True
        bReplace = False
        bReplaceMerge = False
        bDetails = True

        # claim gdal parameters
        #gdal.UseExceptions()    # Enable exceptions
        ogr.UseExceptions()
        gdal.SetConfigOption('GDAL_PAM_ENABLED', 'TRUE')

##        # Path to the DSH3M Elevation Index from the original elevation data
##        dsh3mIdxShp = input("\nEnter full path to the DSH3M Elevation Index Layer: ")
##        while not os.path.exists(dsh3mIdxShp):
##            print(f"{dsh3mIdxShp} does NOT exist. Try Again")
##            dsh3mIndex = input("\nEnter full path to the DSH3M Elevation Index Layer: ")
##
##        # Path to CONUS GRID - serves as the driver
##        gridShp = input("\nEnter full path to the CONUS GRID: ")
##        while not os.path.exists(gridShp):
##            print(f"{gridShp} does NOT exist. Try Again")
##            dsh3mIndex = input("\nEnter full path to the CONUS GRID: ")
##
##        # Replace Data
##        bReplace = input("\nDo you want to replace existing DSH3M data? (Yes/No): ")
##        while not bReplace.lower() in ("yes","no","y","n"):
##            print(f"Please Enter Yes or No")
##            bReplace = input("Do you want to replace existing DSH3M data? (Yes/No): ")
##
##        if bReplace.lower() in ("yes","y"):
##            bReplace = True
##        else:
##            bReplace = False
##
##        # Log Details
##        bDetails = input("\nDo you want to log specific DSH3M details? (Yes/No): ")
##        while not bDetails.lower() in ("yes","no","y","n"):
##            print(f"Please Enter Yes or No")
##            bDetails = input("Do you want to log specific DSH3M details? (Yes/No): ")
##
##        if bDetails.lower() in ("yes","y"):
##            bDetails = True
##        else:
##            bDetails = False
##
##        bMultiThreadMode = True

        # Start the clock
        startTime = tic()

        """ -------------------------- Establish Console LOG FILE ------------------------------------------"""
        # Setup Log file that captures console messages
        # USGS_3DEP_DSH3M_Step5_ConsoleMsgs.txt
        msgLogFile = f"{os.path.dirname(dsh3mIdxShp)}{os.sep}USGS_3DEP_DSH3M_Step5_ConsoleMsgs.txt"
        today = datetime.today().strftime('%m%d%Y')
        h = open(msgLogFile,'a+')
        h.write(f"Executing: USGS_5_Create_DSH3M_DEMs {today}\n\n")
        h.write(f"User Selected Parameters:\n")
        h.write(f"\tDSH3M Elevation Index Layer: {dsh3mIdxShp}\n")
        h.write(f"\tCONUS Grid Layer: {gridShp}\n")
        h.write(f"\tOverwrite Data: {bReplace}\n")
        h.write(f"\tVerbose Mode: {bDetails}\n")
        h.close()

        # Get Headers from dsh3m index layer
        # ['huc_digit','prod_title','pub_date','last_updated','size','format'] ...etc
        try:
            index = gp.read_file(dsh3mIdxShp)
            headerValues = index.columns.tolist()
            del index
        except:
            AddMsgAndPrint("\nFailed to open {dsh3mIdxShp}")
            AddMsgAndPrint(errorMsg())

        # key:grid ID -- value: list of lists of the DEMs that intersect the grid ID
        AddMsgAndPrint("\nSTEP 1: Creating DSH3M Multi-resolution Overlay")
        gridIndexOverlayDict,gridExtentDict = createMultiResolutionOverlay(dsh3mIdxShp,gridShp)

        """ -------------------------- STEP 2: Create DSH3M DEMs ------------------------------------------"""
        # key: gridID -- value: [sourceID,last_update,dsh3m_raster,source]
        # populated from the results of 'createSoil3MDEM' function -- feeds Step3
        dsh3mDict = dict()

        # key: srcID -- value: dsh3m_raster
        # populated from the results of 'createSoil3MDEM' function
        # dual purpose: ensure that overlapped DEMs are not deleted and recreated in the
        # 'createSoil3MDEM' function if bReplace is set to true; feeds Step4
        dsh3mStatDict = dict()

        # failed DEMs from 'createSoil3MDEM' function
        failedDEMs = list()

        totalNumOfGrids = len(gridIndexOverlayDict)
        gridCounter = 1

        # key: gridID -- value: [[x,y,z....],[x,y,z....],[x,y,z....]]
        dsh3mStartTime = tic()
        AddMsgAndPrint("\nSTEP 2: Creating DSH3M DEMs")
        for gridID,items in gridIndexOverlayDict.items():

            # counter grid DEMs to convert to DSH3M
            i = 0

            # Summarize to get DEM breakdown by resolution
            resList = [item[-1] for item in items]
            resCounts = {i:resList.count(i) for i in resList}

            numOfSoil3MtoProcess = len(items)
            AddMsgAndPrint(f"\n\tGrid ID: {gridID} (Grid {gridCounter} of {totalNumOfGrids}) has {numOfSoil3MtoProcess:,} DEMs that need a DSH3M DEM created")
            for k,v in resCounts.items():
                AddMsgAndPrint(f"\t\t{k}M DEMs: {v}")

            gridStartTime = tic()

            if bMultiThreadMode:
                """------------------  Execute in Multi-Thread Mode --------------- """
                with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:

                    # use a set comprehension to start all tasks.  This creates a future object
                    future3mDEM = {executor.submit(createSoil3MDEM, item): item for item in items}

                    # yield future objects as they are done.
                    for new3mDEM in as_completed(future3mDEM):

                        msgs = new3mDEM.result()[0]

                        # [sourceID,last_update,out_raster,source]
                        soil3MList = new3mDEM.result()[1]
                        sourceID = soil3MList[0]
                        dsh3mRaster = soil3MList[2]

                        # Print Messages from results
                        i+=1
                        j=1
                        for printMessage in msgs:
                            if j==1:
                                AddMsgAndPrint(f"{printMessage} -- ({i:,} of {numOfSoil3MtoProcess:,}) -- (Grid {gridCounter} of {totalNumOfGrids})")
                            else:
                                AddMsgAndPrint(printMessage)
                            j+=1

                        # Handle results
                        if len(soil3MList) > 0:

                            if not gridID in dsh3mDict:
                                dsh3mDict[gridID] = [soil3MList]
                            else:
                                dsh3mDict[gridID].append(soil3MList)

                            if not sourceID in dsh3mStatDict:
                                dsh3mStatDict[sourceID] = dsh3mRaster
                            else:
                                AddMsgAndPrint(f"\t\t\tDouble sourceID: {sourceID}: -- {dsh3mRaster}")


            else:
                """------------------  Execute in Single Mode --------------- """
                for item in items:

                    new3mDEMresult = createSoil3MDEM(item)
                    msgs = new3mDEMresult[0]
                    soil3MList = new3mDEMresult[1]
                    sourceID = soil3MList[0]
                    dsh3mRaster = soil3MList[2]


                    if len(soil3MList) > 0:
                        if not gridID in dsh3mDict:
                            dsh3mDict[gridID] = [soil3MList]
                        else:
                            dsh3mDict[gridID].append(soil3MList)

                        if not sourceID in dsh3mStatDict:
                            dsh3mStatDict[sourceID] = dsh3mRaster
                        else:
                            AddMsgAndPrint(f"\t\t\tDouble sourceID: {sourceID}: -- {dsh3mRaster}")

                    i+=1
                    j=1
                    for printMessage in msgs:
                        if j==1:
                            AddMsgAndPrint(f"{printMessage} -- ({i:,} of {numOfSoil3MtoProcess:,}) -- (Grid {gridCounter} of {totalNumOfGrids})")
                        else:
                            AddMsgAndPrint(printMessage)
                        j+=1

            AddMsgAndPrint(f"\n\t\tGrid ID: {gridID} Processing Time: {toc(gridStartTime)}")
            gridCounter+=1

        dsh3mStopTime = toc(dsh3mStartTime)
        AddMsgAndPrint(f"\n\tTotal Processing Time to create DSH3M DEMs: {dsh3mStopTime}")

        """ -------------------------- Step3: Merge DSH3M DEMs ------------------------------------------"""
        AddMsgAndPrint("\nSTEP 3: Merging DSH3M DEMs")
        gridCounter = 1

        # Set output Coordinate system to 5070
        spatialRef = osr.SpatialReference()
        spatialRef.ImportFromEPSG(5070)
        srs = f"EPSG:{spatialRef.GetAuthorityCode(None)}"

        # dsh3mRasters - dict of list containing lists of completed dsh3m rasters; each list contains 4 values
        # key: gridID -- value: [[sourceID,last_update,out_raster,source],[x,y,z],[x,y,z]]
        for gridID,dsh3mRasters in dsh3mDict.items():

            mergeResult = mergeGridDEMs(dsh3mRasters)
            gridCounter+=1

        del gridID, dsh3mRasters

        """ ----------------------------- Step 4: Create dsh3m Elevation Metadata ShapeFile ----------------------------- """
        if len(dsh3mStatDict):
            AddMsgAndPrint(f"\nSTEP 4: Creating Elevation Metadata Shapefile for DSH3M DEMs")
            metadataFileStart = tic()
            metadataFile = createElevMetadataFile_MT(dsh3mStatDict,dsh3mIdxShp)
            AddMsgAndPrint(f"\n\tDSH3M Elevation Metadata File Path: {metadataFile}")
            metadataFileStop = toc(metadataFileStart)
            exit()

            """ ----------------------------- Create Raster2pgsql File ---------------------------------- """
            if os.path.exists(metadataFile):
                AddMsgAndPrint(f"\nCreating Raster2pgsql File\n")
                r2pgsqlStart = tic()
                r2pgsqlFile = createRaster2pgSQLFile(metadataFile)
                AddMsgAndPrint(f"\t\nDSH3M Raster2pgsql File Path: {r2pgsqlFile}")
                AddMsgAndPrint(f"\tIMPORTANT: Make sure dbTable variable (elevation_3m) is correct in Raster2pgsql file!!")
                r2pgsqlStop = toc(r2pgsqlStart)
            else:
                AddMsgAndPrint(f"\nDSH3M Raster2pgsql File will NOT be created")
        else:
            AddMsgAndPrint(f"\nNo information available to produce DSH3M Metadata Database Elevation File")
            AddMsgAndPrint(f"\nNo information available to produce DSH3M Raster2pgsql File")

        """ ------------------------------------ SUMMARY -------------------------------------------- """
        AddMsgAndPrint(f"\n{'-'*40}SUMMARY{'-'*40}")

        AddMsgAndPrint(f"\nTotal Processing Time: {toc(startTime)}")
        AddMsgAndPrint(f"\tCreate DSH3M DEMs Time: {soil3MstopTime}")
        AddMsgAndPrint(f"\tCreate DSH3M Metadata Elevation File Time: {metadataFileStop}")
        AddMsgAndPrint(f"\tCreate DSH3M Raster2pgsql File Time: {r2pgsqlStop}")

        # Report number of original DEMs processed for Soil3M
        if len(soil3MDict) == recCount:
            AddMsgAndPrint(f"\nSuccessfully Processed ALL {len(soil3MDict):,} original DEM files")
        elif len(soil3MDict) == 0:
            AddMsgAndPrint(f"\nNo original DEMs files were processed")
        else:
            AddMsgAndPrint(f"\nProcessed {len(soil3MDict):,} out of {recCount:,} original DEM files")

        if len(failedDEMs):
            AddMsgAndPrint(f"\nFailed to create Soil3M files for {len(failedDEMs):,} original DEM files:")
            AddMsgAndPrint(f"{failedDEMs}")


    except:
        AddMsgAndPrint(errorMsg())