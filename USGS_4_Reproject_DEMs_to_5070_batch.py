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

"""

import os, traceback, sys, time
import threading, multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime
from osgeo import gdal
from osgeo import osr
from osgeo import ogr

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
        AddMsgAndPrint("Converting Master Elevation File into a dictionary")
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
                    continue

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
def createSoil3MDEM(item):
    """ item is passed over as a tuple with the key = 0 and the values = 1"""

    try:
        rasterRecord = item[1]

        # Positions of
        fileFormat = rasterRecord[headerValues.index("format")]
        DEMname = rasterRecord[headerValues.index("DEMname")]
        DEMpath = rasterRecord[headerValues.index("DEMpath")]
        EPSG = rasterRecord[headerValues.index("EPSG")]
        srsName = rasterRecord[headerValues.index("srsName")]
        top = rasterRecord[headerValues.index("top")]
        left = rasterRecord[headerValues.index("left")]
        right = rasterRecord[headerValues.index("right")]
        bottom = rasterRecord[headerValues.index("bottom")]

        messageList = list()

        if bDetails:
            messageList.append(f"\n\tProcessing DEM: {DEMname}")
            theTab = "\t\t"
        else:
            theTab = "\t"

        # D:\projects\DSHub\reampling\1M\USGS_1M_Madison.tif
        input_raster = os.path.join(DEMpath,DEMname)

        # D:\projects\DSHub\reampling\1M\USGS_1M_Madison_EPSG5070.tif
        out_raster = f"{input_raster.split('.')[0]}_EPSG5070.{input_raster.split('.')[1]}"

        if os.path.exists(out_raster):
            try:
                if bReplace:
                    os.remove(out_raster)
                    messageList.append(f"{theTab}Successfully Deleted {os.path.basename(out_raster)}")
                else:
                    messageList.append(f"{theTab}{'Projected DEM Exists':<35} {os.path.basename(out_raster):<60}")
                    return messageList
            except:
                messageList.append(f"{theTab}{'Failed to Delete':<35} {fileName:<60}")
                failedDEMs.append(rasterRecord)
                return messageList

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

##        messageList.append(f"\n{theTab}------------ {outputSRS} Snapped Exent ------------")
##        messageList.append(f"{theTab}Top Extent: {newTop}")
##        messageList.append(f"{theTab}Bottom Extent: {newBottom}")
##        messageList.append(f"{theTab}Left Extent: {newLeft}")
##        messageList.append(f"{theTab}Right Extent: {newRight}")

        messageList.append(f"\n{theTab}------------ {outputSRS} Snapped Exents ------------")
        messageList.append(f"{theTab}LL Coords - POINT ({newLeft} {newBottom}) (Left,Bottom)")
        messageList.append(f"{theTab}UL Coords - POINT ({newLeft} {newTop}) (Left,Top)")
        messageList.append(f"{theTab}UR Coords - POINT ({newRight} {newTop}) (Right,Top)")
        messageList.append(f"{theTab}LR Coords - POINT ({newRight} {newBottom}) (Right,Bottom)")

        if fileFormat == 'GeoTIFF':
            outputFormat = 'GTiff'
        else:
            outputFormat = 'HFA' # Erdas Imagine .img

        args = gdal.WarpOptions(format=outputFormat,
                                xRes=3,
                                yRes=3,
                                srcSRS=inputSRS,
                                dstSRS=outputSRS,
                                outputBounds=[newLeft, newBottom, newRight, newTop],
                                outputBoundsSRS=outputSRS,
                                resampleAlg=gdal.GRA_Bilinear,
                                multithread=True)

        gdal.Warp(out_raster, input_raster, options=args)

        if bDetails:
            messageList.append(f"\n{theTab}Successfully Created Soil3M DEM: {os.path.basename(out_raster)}")
        else:
            messageList.append(f"{theTab}{'Successfully Created Soil3M DEM:':<35} {os.path.basename(out_raster):>60}")

        return messageList

    except:
        failedDEMs.append(rasterRecord)
        messageList.append(f"\n{theTab}{errorMsg(errorOption=2)}")
        return messageList

## ===================================================================================
if __name__ == '__main__':

    try:

        # Script Parameters
        elevationMetadataFile = r'D:\projects\DSHub\reampling\USGS_3DEP_1M_Metadata_Elevation_03142023_MASTER_DB.txt'
        bMultiThreadMode = False
        bReplace = True
        bDetails = True

        # Start the clock
        start = tic()

        # Setup Log file that captures console messages
        # Pull elevation resolution from file name
        # USGS_3DEP_1M_Step4_Soil3M_ConsoleMsgs.txt
        today = datetime.today().strftime('%m%d%Y')
        resolution = elevationMetadataFile.split(os.sep)[-1].split('_')[2]
        msgLogFile = f"{os.path.dirname(elevationMetadataFile)}{os.sep}USGS_3DEP_{resolution}_Step4_Soil3M_ConsoleMsgs.txt"
        h = open(msgLogFile,'a+')
        h.write(f"Executing: USGS_4_Reproject_DEMs_to_5070 {today}\n\n")
        h.write(f"User Selected Parameters:\n")
        h.write(f"\tElevation Metadta File: {elevationMetadataFile}\n")
        h.write(f"\tMulti-Threading Mode: {bMultiThreadMode}\n")
        h.write(f"\tOverwrite Data: {bReplace}\n")
        h.write(f"\tVerbose Mode: {bDetails}\n")
        h.close()

        # List of header values
        headerValues = open(elevationMetadataFile).readline().rstrip().split(',')

        # Convert masterElevDbfile to a dictionary
        # sourceID = [List of all attributes]
        elevMetadataDict = convertMasterDBfileToDict(elevationMetadataFile)
        recCount = len(elevMetadataDict)

        # progress tracker
        i = 0
        failedDEMs = list()

        if bMultiThreadMode:
            """------------------  Execute in Multi-Thread Mode --------------- """
            AddMsgAndPrint(f"\nProcessing {recCount:,} DEM files to create Best Available 3M")
            with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:

                # use a set comprehension to start all tasks.  This creates a future object
                future3mDEM = {executor.submit(createSoil3MDEM, DEMrecord): DEMrecord for DEMrecord in elevMetadataDict.items()}

                # yield future objects as they are done.
                for new3mDEM in as_completed(future3mDEM):
                    i+=1
                    j=1
                    for printMessage in new3mDEM.result():
                        if j==1:
                            AddMsgAndPrint(f"{printMessage} -- ({i:,} of {recCount:,})")
                        else:
                            AddMsgAndPrint(printMessage)
                        j+=1

        else:
            """------------------  Execute in Single Mode --------------- """
            for sourceID,items in elevMetadataDict.items():
                result = createSoil3MDEM((sourceID,items))
                i+=1
                j=1
                for printMessage in result:
                    if j==1:
                        AddMsgAndPrint(f"{printMessage} -- ({i:,} of {recCount:,})")
                    else:
                        AddMsgAndPrint(printMessage)
                    j+=1

        print(toc(start))


        # ADD SUMMARY

    except:
        AddMsgAndPrint(errorMsg())