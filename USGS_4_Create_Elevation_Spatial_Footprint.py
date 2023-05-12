# -*- coding: utf-8 -*-
"""
Created on Wed Apr 12 12:48:16 2023

@author: 28200310160021036376
"""

# this function reads a raster ->
# makes it a vrt (set no data an actual value) ->
# translates to tif ->
# gdal calc to identify data/nodata ->
# polygonizes the result
# appends the polygons and srs to list

from contextlib import closing
import os, datetime, subprocess, sys, traceback, re, glob, math, time
from osgeo import gdal
from osgeo import ogr, osr
import threading, multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

## ===================================================================================
def AddMsgAndPrint(msg):

    # Print message to python message console
    print(msg)

    # Add message to log file

##    try:
##        h = open(msgLogFile,'a+')
##        h.write("\n" + msg)
##        h.close
##        del h
##    except:
##        pass

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


#### ===================================================================================
def createFootPrint(file):

    try:
        messageList = list()
        filestart = tic()
        rasterRecord = file[1]

        # Positions of
        demName = rasterRecord[headerValues.index("DEMname")]
        demDir = rasterRecord[headerValues.index("DEMpath")]
        EPSG = int(rasterRecord[headerValues.index("EPSG")])
        noData = float(rasterRecord[headerValues.index("noDataVal")])
        minStat = float(rasterRecord[headerValues.index("minStat")])

        messageList.append(f"\n\tProcessing {demName}")

        # use the minimum stat to reclassify the input raster
        calcValue = ""
        if minStat > 0:
            # Round down to the nearest 100th 875.567 --> 800
            calcValue = float(math.floor(minStat / 100.00) * 100)
            messageList.append(f"\t\tThreshold Value: {calcValue}")
        else:
            calcValue = noData
            messageList.append(f"\t\tThreshold Value: {noData}")

        fieldNames = {'huc_digit':ogr.OFTString,
                      'prod_title':ogr.OFTString,
                      'pub_date':ogr.OFTDate,
                      'last_updat':ogr.OFTDate,
                      'size':ogr.OFTInteger,
                      'format':ogr.OFTString,
                      'sourceID':ogr.OFTString,
                      'meta_url':ogr.OFTString,
                      'downld_url':ogr.OFTString,
                      'DEMname':ogr.OFTString,
                      'DEMpath':ogr.OFTString,
                      'columns':ogr.OFTInteger,
                      'rows':ogr.OFTInteger,
                      'bandCount':ogr.OFTInteger,
                      'cellSize':ogr.OFTReal,
                      'rdsFormat':ogr.OFTString,
                      'bitDepth':ogr.OFTString,
                      'noDataVal':ogr.OFTReal,
                      'srType':ogr.OFTString,
                      'EPSG':ogr.OFTInteger,
                      'srsName':ogr.OFTString,
                      'top':ogr.OFTReal,
                      'left':ogr.OFTReal,
                      'right':ogr.OFTReal,
                      'bottom':ogr.OFTReal,
                      'minStat':ogr.OFTReal,
                      'meanStat':ogr.OFTReal,
                      'maxStat':ogr.OFTReal,
                      'stDevStat':ogr.OFTReal,
                      'blockXSize':ogr.OFTInteger,
                      'blockYsize':ogr.OFTInteger,
                      "rastValue":ogr.OFTInteger}

        file = f"{demDir}{os.sep}{demName}"
        fileExt = file.split('.')[1] # tif, img
        outTif = f"{file.split('.')[0]}_TEMP.{fileExt}"

        if os.path.exists(outTif):
            os.remove(outTif)
            messageList.append(f"\t\tSuccessfully deleted {outTif}")

        """ ------------------------- Create Data Mask ----------------------------------- """
        #gdal_calc = f"gdal_calc -A {file} --outfile={outTif} --type=Byte --calc=\"A>{calcValue}\" --NoDataValue={noData}"
        #gdal_calc = f"gdal_calc -A {file} --outfile={outTif} --type=Byte --calc=\"A>{calcValue}\" --NoDataValue=0"
        gdal_calc = f"gdal_calc -A {file} --A_band=1 --outfile={outTif} --type=Byte --calc=\"A>{calcValue}\" --NoDataValue=0"

        execCmd = subprocess.Popen(gdal_calc, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
        msgs, errors = execCmd.communicate()
        errors = ','.join(errors.strip().split('\n'))  # some errors return mutliple carriage returns

        # Collect messages from subprocess
        errorList = ['error','failed','fail','uncommit','aborted','notice',
                     'warning','unable','not recognized','inoperable']
        words_re = re.compile("|".join(errorList))

        if words_re.search(errors.lower()) or not execCmd.returncode == 0:
            #msgDict['Error'] = f"{gdal_calc}\n\t{errors}"
            messageList.append(f"\t\tErrors creating 'data' mask: {errors}")
        else:
            #msgDict['Success'] = f"Successfully created 'data' mask"
            messageList.append(f"\t\tSuccessfully created 'data' mask")

        """ ------------------------- Establish New Shapefile ----------------------------------- """
        # Set up the shapefile driver
        outDriver = ogr.GetDriverByName("ESRI Shapefile")

        layername = 'fp99x_' + demName[:-4]                # New shapefile name - no ext
        shpPath = f"{demDir}{os.sep}{layername}.shp"    # New shapefile path

        # Remove output shapefile if it already exists
        if os.path.exists(shpPath):
            outDriver.DeleteDataSource(shpPath)
            messageList.append(f"\t\tSuccessfully deleted: {shpPath}")

        # set the spatial reference for the shapefile; same as input raster
        sp_ref = osr.SpatialReference()
        sp_ref.ImportFromEPSG(EPSG)
        #sp_ref.SetFromUserInput(f"EPSG:{EPSG}")

        # set the destination of the data source
        outDataSource = outDriver.CreateDataSource(demDir)
        outLayer = outDataSource.CreateLayer(layername, srs=sp_ref, geom_type=ogr.wkbPolygon)

        # Add new Fields to the output shapefile
        for fld in fieldNames.items():
            fldname = fld[0]
            fldType = fld[1]
            gdalFld = ogr.FieldDefn(fldname, fldType)

            # Getting warning messages with nodata val
            if fldname == 'noDataVal':
                gdalFld.SetWidth(50)
                gdalFld.SetPrecision(7)

            outLayer.CreateField(gdalFld)

        # Get the output Layer's Feature Definition
        outLayerDefn = outLayer.GetLayerDefn()

        """ ------------------------- Vectorize Mask ----------------------------------- """
        # Value from raster after
        rast_field = outLayer.GetLayerDefn().GetFieldIndex("rastValue")

        # Open raster to pass a band object to polygonize function
        ds = gdal.Open(outTif)
        band = ds.GetRasterBand(1)

        #result = gdal.Polygonize(band, None if , outLayer, rast_field, [], callback=None)
        result = gdal.Polygonize(band, band, outLayer, rast_field, [], callback=None)
        assert result == 0, AddMsgAndPrint(f"\tPolygonize failed")
        messageList.append(f"\t\tSuccessfully vectorized mask into {layername}.shp")

        # populate the shapefile fields after polygons exist
        for i in range(0, outLayer.GetFeatureCount()):
            feature = outLayer.GetFeature(i)
            for j in range(0,outLayerDefn.GetFieldCount()-1):
                feature.SetField(outLayerDefn.GetFieldDefn(j).GetName(),rasterRecord[j])
                #AddMsgAndPrint(f"{outLayerDefn.GetFieldDefn(j).GetName()} -- {rasterRecord[j]}")
                test = outLayer.SetFeature(feature)

        """ ------------------------- Cleanup Time ----------------------------------- """
        # close the shapefile and workspace
        outDataSource.FlushCache()
        outDataSource.Destroy()

        del outDriver, outDataSource, outLayer, ds, band

        # Delete TEMP raster layer
        for tmpFile in glob.glob(f"{outTif.split('.')[0]}*"):
            #os.remove(tmpFile)
            AddMsgAndPrint(f"\t\tSuccessfully Deleted: {tmpFile}")

        # gather a collection of shapefile, full path name and coord sys
        shapefiles.append((shpPath,EPSG))
        messageList.append(f"\t\tProcess Time: {toc(filestart)}")

        return messageList

    except:
        messageList.append(errorMsg(errorOption=2))
        return messageList


#### ===================================================================================

if __name__ == '__main__':

    elevationMetadataFile = r'D:\projects\DSHub\reampling\USGS_3DEP_10M_Step2_Elevation_Metadata.txt'

    # List of header values
    headerValues = open(elevationMetadataFile).readline().rstrip().split(',')
    recCount = len(open(elevationMetadataFile).readlines()) - 1

    # Convert input metadata elevation file to a dictionary
    # sourceID = [List of all attributes]
    files = convertMasterDBfileToDict(elevationMetadataFile)

    # accumulate shapefiles in list
    shapefiles = list()
    wdir = os.path.dirname(sys.argv[0])

    ndTracker = 0

##    start = tic()
##    for raster in files.items():
##        test = createFootPrint(raster)
##        print(test)
##        ndTracker+=1
##        break
##        if ndTracker == 10:
##            break
##
##    print(toc(start))
##    exit()

    """ ---------------------------- run createFootPrint in mult-thread mode ---------------------------------------------------"""
    ndRasStart = tic()
    with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        ndProcessing = {executor.submit(createFootPrint, raster): raster for raster in files.items()}

        # yield future objects as they are done.
        for future in as_completed(ndProcessing):
            ndTracker +=1
            j=1
            for printMessage in future.result():
                if j==1:
                    AddMsgAndPrint(f"{printMessage} -- ({ndTracker:,} of {recCount:,})")
                else:
                    AddMsgAndPrint(printMessage)
                j+=1

    ndRasStop = toc(ndRasStart)
    finish = datetime.datetime.now()



