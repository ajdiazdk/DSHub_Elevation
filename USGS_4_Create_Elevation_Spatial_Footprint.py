# -*- coding: utf-8 -*-
"""
Script Name: USGS_4_Create_Elevation_Spatial_Footprint.py
Created on Fri Aug 11 10:12:13 2022
updated 4/5/2023

@author: Adolfo.Diaz
GIS Business Analyst
USDA - NRCS - SPSD - Soil Services and Information
email address: adolfo.diaz@usda.gov
cell: 608.215.7291

This is script #4 in the USGS Elevation acquisition workflow developed for the DS Hub.

The purpose of this script is to create a delineation around valid pixels for the input
DEMs.  The delineations are NOT bounding boxes.  Instead they act more like a convex hull
around pixels that are greater or equal the minimum value.  It excludes NODATA pixels.


Parameters:
    1) elevationMetadataFile - path to text file that contains elevation metadata
    4) outFpDir - boolean indicator to replace download file
    5) bDeleteZipFiles - boolean to delete or leave downloaded zipped files (only relevant if dl files are zip files)
                         set to False by default but can be overwritten in main.
"""

import os, subprocess, sys, traceback, re, glob, math, time, fnmatch, psutil
from datetime import datetime
from osgeo import gdal
from osgeo import ogr, osr
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

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
def errorMsg(errorOption=1,batch=False):
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
        AddMsgAndPrint("\nConverting input Metadata Elevation File into a dictionary")
        mDBFnumOfRecs = len(open(elevMetdataFile).readlines())

        # Find sourceID in headers; return False if not found
        sourceIDidx = headerValues.index('sourceid')

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
            AddMsgAndPrint(f"\tElevation Metadata File: {os.path.basename(elevMetdataFile)} was empty!")
            return False

        if badLines > 0:
            AddMsgAndPrint(f"\tThere are(is) {badLines} records with anomalies found")

        return masterDBfileDict

    except:
        errorMsg()
        return False


#### ===================================================================================
def createFootPrint(items):
    """        '63e7308bd34efa0476ae8401': ['11030005',
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
        messageList = list()
        filestart = tic()
        rasterRecord = items[1]

        # Positions of item elements
        demName = rasterRecord[headerValues.index("dem_name")]
        demDir = rasterRecord[headerValues.index("dem_path")]
        EPSG = int(rasterRecord[headerValues.index("epsg_code")])
        noData = float(rasterRecord[headerValues.index("nodataval")])
        minStat = float(rasterRecord[headerValues.index("rds_min")])

        messageList.append(f"\n\tProcessing {demName}")

        # Just in case footprint already exists - don't redo it.
        prjShape = f"{outFpDir}{os.sep}{demName[:-4]}_FP_WGS84.shp"
        if os.path.exists(prjShape):

            driver = ogr.GetDriverByName('ESRI Shapefile')
            dataSource = driver.Open(prjShape, 0)

            # Check health of shapefile; delete and redo if it is corrupt; skip if healthy
            if dataSource is None:
                messageList.append(f"\t\t-WGS84 Footprint exists but is corrupt: {os.path.basename(prjShape)}")

                # Delete corrupted Shapefile
                for tmpFile in glob.glob(f"{prjShape.split('.')[0]}*"):
                    os.remove(tmpFile)
                    if tmpFile.endswith('.shp'):
                        messageList.append(f"\t\t-Successfully Deleted Shapefile: {os.path.basename(tmpFile)}")

            else:
                fpShapes.append(prjShape)
                messageList.append(f"\t\t-WGS84 Footprint exists: {os.path.basename(prjShape)}")
                messageList.append(f"\t\t-Process Time: {toc(filestart)}")
                return messageList

            del driver,dataSource
        del prjShape

        # use the minimum stat to reclassify the input raster
        calcValue = ""
        if minStat > 0:
            # Round down to the nearest 100th 875.567 --> 800
            calcValue = float(math.floor(minStat / 100.00) * 100)
        else:
            calcValue = noData
        #messageList.append(f"\t\tThreshold Value: {calcValue}")

        # Temp Tiff that will be created and used to vectorize
        demPath = f"{demDir}{os.sep}{demName}"
        fileExt = demPath.split('.')[1] # tif, img
        tempTif = f"{demPath.split('.')[0]}_TEMP.{fileExt}"

        # Delete TEMP raster layer
        for tmpFile in glob.glob(f"{tempTif.split('.')[0]}*"):
            os.remove(tmpFile)
            #messageList.append(f"\t\tSuccessfully Deleted temp tiff: {os.path.basename(tmpFile)}")

        """ ------------------------- Create Raster Data Mask ----------------------------------- """
        if os.name == 'nt':
            gdal_calc = f"gdal_calc -A {demPath} --outfile={tempTif} --type=Byte --calc=\"A>{calcValue}\" --NoDataValue=0"
        else:
            #gdal_calc = f"gdal_calc -A {demPath} --A_band=1 --outfile={tempTif} --type=Byte --calc=\"A>{calcValue}\" --NoDataValue=0"
            gdal_calc = f"python3 /bin/gdal_calc.py -A {demPath} --outfile={tempTif} --type=Byte --calc=\"A>{calcValue}\" --NoDataValue=0"

        # Collect messages from subprocess
        errorList = ['error','failed','fail','uncommit','aborted','notice','memory',
                     'unable','not recognized','inoperable','syntax']
        words_re = re.compile("|".join(errorList))

        # Send gdal command to os
        execCmd = subprocess.Popen(gdal_calc, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)

        # returns a tuple (stdout_data, stderr_data)
        msgs, errors = execCmd.communicate()
        errors = ','.join(errors.strip().split('\n'))  # some errors return mutliple carriage returns

        if words_re.search(errors.lower()) or not execCmd.returncode == 0:
            #msgDict['Error'] = f"{gdal_calc}\n\t{errors}"
            messageList.append(f"\t\t-Errors creating 'data' mask: {errors}")
        else:
            #msgDict['Success'] = f"Successfully created 'data' mask"
            messageList.append(f"\t\t-Successfully created 'data' mask using values >= {calcValue}")

        """ ------------------------- Create Footprint Shapefile ----------------------------------- """
        # List of field names to add to shapefile
        fieldNames = {'poly_code':ogr.OFTString,
                      'prod_title':ogr.OFTString,
                      'pub_date':ogr.OFTDate,
                      'lastupdate':ogr.OFTDate,
                      'rds_size':ogr.OFTInteger,
                      'format':ogr.OFTString,
                      'sourceid':ogr.OFTString,
                      'meta_url':ogr.OFTString,
                      'downld_url':ogr.OFTString,
                      'dem_name':ogr.OFTString,
                      'dem_path':ogr.OFTString,
                      'rds_column':ogr.OFTInteger,
                      'rds_rows':ogr.OFTInteger,
                      'bandCount':ogr.OFTInteger,
                      'cellSize':ogr.OFTReal,
                      'rdsformat':ogr.OFTString,
                      'bitdepth':ogr.OFTString,
                      'nodataval':ogr.OFTReal,
                      'srs_type':ogr.OFTString,
                      'epsg_code':ogr.OFTInteger,
                      'srs_name':ogr.OFTString,
                      'rds_top':ogr.OFTReal,
                      'rds_left':ogr.OFTReal,
                      'rds_right':ogr.OFTReal,
                      'rds_bottom':ogr.OFTReal,
                      'rds_min':ogr.OFTReal,
                      'rds_mean':ogr.OFTReal,
                      'rds_max':ogr.OFTReal,
                      'rds_stdev':ogr.OFTReal,
                      'blk_xsize':ogr.OFTInteger,
                      'blk_ysize':ogr.OFTInteger,
                      "rastValue":ogr.OFTInteger}

        # Set up the shapefile driver
        outDriver = ogr.GetDriverByName("ESRI Shapefile")

        fpLyrName = f"{demName[:-4]}_FP"                    # New shapefile name - no ext
        fpShpPath = f"{outFpDir}{os.sep}{fpLyrName}.shp"    # New shapefile footprint path

        # Remove output shapefile if it already exists
        if os.path.exists(fpShpPath):
            outDriver.DeleteDataSource(fpShpPath)
            messageList.append(f"\t\t-Successfully deleted existing Shp: {fpLyrName}.shp")

        # set the spatial reference for the shapefile; same as input raster
        sp_ref = osr.SpatialReference()
        sp_ref.ImportFromEPSG(EPSG)
        #sp_ref.SetFromUserInput(f"EPSG:{EPSG}")

        # set the destination of the data source
        outDataSource = outDriver.CreateDataSource(outFpDir)
        outLayer = outDataSource.CreateLayer(fpLyrName, srs=sp_ref, geom_type=ogr.wkbPolygon)

        # Add new Fields to the output shapefile
        for fld in fieldNames.items():
            fldname = fld[0]
            fldType = fld[1]
            gdalFld = ogr.FieldDefn(fldname, fldType)

            # Getting warning messages with nodata val
            if fldname == 'noDataVal':
                gdalFld.SetWidth(90)
                gdalFld.SetPrecision(60)

            outLayer.CreateField(gdalFld)

        # Get the output Layer's Feature Definition
        outLayerDefn = outLayer.GetLayerDefn()

        """ ------------------------- Vectorize Mask ----------------------------------- """
        # Value from raster after
        rast_field = outLayer.GetLayerDefn().GetFieldIndex("rastValue")

        # Open raster to pass a band object to polygonize function
        ds = gdal.Open(tempTif)
        band = ds.GetRasterBand(1)

        # vectorize mask
        #result = gdal.Polygonize(band, None if , outLayer, rast_field, [], callback=None)
        result = gdal.Polygonize(band, band, outLayer, rast_field, [], callback=None)
        assert result == 0, AddMsgAndPrint(f"\tPolygonize failed")
        messageList.append(f"\t\t-Successfully vectorized mask into: {fpLyrName}.shp")

        # populate the shapefile fields after polygons exist
        for i in range(0, outLayer.GetFeatureCount()):
            feature = outLayer.GetFeature(i)
            for j in range(0,outLayerDefn.GetFieldCount()-1):
                feature.SetField(outLayerDefn.GetFieldDefn(j).GetName(),rasterRecord[j])
                #AddMsgAndPrint(f"{outLayerDefn.GetFieldDefn(j).GetName()} -- {rasterRecord[j]}")
                test = outLayer.SetFeature(feature)

        # close the shapefile and workspace
        outDataSource.FlushCache()
        outDataSource.Destroy()

        del outDataSource, outLayer, ds, band

        """ ------------------------- Project Mask to WGS84 ----------------------------------- """
        #inDriver = ogr.GetDriverByName("ESRI Shapefile")
        #shape = inDriver.Open(fpShpPath,0)
        #layer = shape.GetLayer()

        # projected output Shapefile
        prjShape = f"{fpShpPath.split('.')[0]}_WGS84.shp"

        # Delete if projected shp exists
        if os.path.exists(prjShape):
            outDriver.DeleteDataSource(prjShape)

        prjCmd = f"ogr2ogr -f \"ESRI Shapefile\" {prjShape} {fpShpPath} -t_srs EPSG:4326 -s_srs EPSG:{EPSG} "# -lco ENCODING=UTF-8"

        # Create 4326 prj file if not created; not sure why the prj is not automatically created.
        prjFile = f"{prjShape[0:-4]}.prj"
        if not os.path.exists(prjFile):
            spatialRef = osr.SpatialReference()
            spatialRef.ImportFromEPSG(4326)
            spatialRef.MorphToESRI()
            file = open(prjFile, 'w')
            file.write(spatialRef.ExportToWkt())
            file.close()
            messageList.append(f"\t\t-Successfully Created WGS84 PRJ File: {prjFile}")

        execCmd = subprocess.Popen(prjCmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
        msgs, errors = execCmd.communicate()
        errors = ','.join(errors.strip().split('\n'))  # some errors return mutliple carriage returns

        fpShapes.append(prjShape)

        # Close datasource
        #shape = None
        #del inDriver,shape,layer,prjShape
        messageList.append(f"\t\t-Successfully Projected: {os.path.basename(fpShpPath)} to WGS84")

        """ ------------------------- Delete Temp Raster Files ----------------------------------- """
        # Delete TEMP raster layer
        for tmpFile in glob.glob(f"{tempTif.split('.')[0]}*"):
            os.remove(tmpFile)
            messageList.append(f"\t\t-Successfully Deleted temp: {os.path.basename(tmpFile)}")

        # Delete TEMP raster layer
        outDriver.DeleteDataSource(fpShpPath)
        messageList.append(f"\t\t-Successfully Deleted temp: {os.path.basename(fpShpPath)}")

        messageList.append(f"\t\t-Process Time: {toc(filestart)}")
        return messageList

    except:
        messageList.append(errorMsg(errorOption=2))
        messageList.append(f"\t\t-Process Time: {toc(filestart)}")
        failedFootprints.append(items)
        return messageList

#### ===================================================================================
def mergeFootPrints(outDir,shapefiles):

    try:
        AddMsgAndPrint(f"\nMerging Shapefile Spatial Footprints")

        """ ------------------  Create new shapefile and add fields -------------------------------"""
        # Create a new shapefile to store the merged data
        mergeShape = f"USGS_3DEP_{resolution}_FootPrints_WGS84"
        mergeShapePath = f"{outDir}{os.sep}{mergeShape}.shp"
        AddMsgAndPrint(f"\n\tCreating Merged Shapefile: {mergeShapePath}")

        outDriver = ogr.GetDriverByName("ESRI Shapefile")

        if os.path.exists(mergeShapePath):
            outDriver.DeleteDataSource(mergeShapePath)
            AddMsgAndPrint(f"\t\t{os.path.basename(mergeShapePath)} Exists.  Successfully Deleted")

        # set the spatial reference for the shapefile; same as input raster
        sp_ref = osr.SpatialReference()
        sp_ref.SetFromUserInput(f"EPSG:4326")

        # set the destination of the data source
        outDataSource = outDriver.CreateDataSource(outDir)
        outLayer = outDataSource.CreateLayer(mergeShape, srs=sp_ref, geom_type=ogr.wkbPolygon)

        # Grab first shape in list just to get the field names
        daShapefile = shapefiles[0]

        dataSource = ogr.Open(daShapefile)
        daLayer = dataSource.GetLayer(0)
        inLayerDefn = daLayer.GetLayerDefn()

        # Add field names to newly created target shapefile
        for i in range(inLayerDefn.GetFieldCount()):
            fieldDefn = inLayerDefn.GetFieldDefn(i)
            outLayer.CreateField(fieldDefn)

        del daShapefile,dataSource,daLayer,inLayerDefn,outDriver

        """ ------------------  Append WGS84 FP shapefiles to new shapefile -------------------------------"""
        AddMsgAndPrint(f"\n\tAppending {len(shapefiles)} WGS84 Footprint Shapefiles to: {mergeShapePath}")
        appendCnt = 0

        # Add each shapefile to the merged layer
        for shapefile in shapefiles:
            print(f"\t\tAppending: {os.path.basename(shapefile)}")

            # Layer of Source Shp
            inDriver = ogr.GetDriverByName("ESRI Shapefile")
            ds_source = inDriver.Open(shapefile,0)

            if ds_source is None:
                AddMsgAndPrint(f"{shapefile} is empty")
                continue

            sourceLyr = ds_source.GetLayer()
            sourceLyrDef = sourceLyr.GetLayerDefn()

            # Append Geometry
            for feat in sourceLyr:
                out_feat = ogr.Feature(outLayer.GetLayerDefn())

                # Get all attributes related to this geometry record
                for i in range(0, sourceLyrDef.GetFieldCount()):
                    out_feat.SetField(sourceLyrDef.GetFieldDefn(i).GetNameRef(),feat.GetField(i))

                out_feat.SetGeometry(feat.GetGeometryRef().Clone())
                outLayer.CreateFeature(out_feat)
                out_feat = None
                outLayer.SyncToDisk()

            ds_source = None
            del inDriver,ds_source,sourceLyr,sourceLyrDef
            appendCnt+=1

        del shapefile
        AddMsgAndPrint(f"\n\t\tSuccessfully Appended {appendCnt:,} footprint Shapefiles to: {os.path.basename(mergeShapePath)}")

        """ ------------------  Delete Temp Projected Files -------------------------------"""
        AddMsgAndPrint(f"\n\tDeleting {len(shapefiles)} Projected Shapefiles")
        deleteCnt = 0

        # Add each shapefile to the merged layer
        driver = ogr.GetDriverByName("ESRI Shapefile")
        for shapefile in shapefiles:
            if os.path.exists(shapefile):
                driver.DeleteDataSource(shapefile)
                print(f"\t\tSuccessfully Deleted {os.path.basename(shapefile)}")
                deleteCnt+=1

        AddMsgAndPrint(f"\t\tSuccessfully Deleted {deleteCnt:,} Projected Shapefiles")

    except:
        errorMsg()

#### ===================================================================================

if __name__ == '__main__':

    funStarts = tic()

    # Parameters
    elevationMetadataFile = r'E:\GIS_Projects\DS_Hub\Elevation\DSHub_Elevation\USGS_Text_Files\30M\20230801_windows\USGS_3DEP_30M_Step2_Elevation_Metadata_test.txt'
    outFpDir = r'D:\projects\DSHub\USGS_Spatial_Footprints\30M_test'
    outputSRS = '4326'

    """ ---------------------------- Establish Console LOG FILE ---------------------------------------------------"""
    tempList = elevationMetadataFile.split(os.sep)[-1].split('_')
    startPos = tempList.index(fnmatch.filter(tempList, '3DEP*')[0]) + 1
    endPos = tempList.index(fnmatch.filter(tempList, 'Step*')[0])
    resolution = '_'.join(tempList[startPos:endPos])
    today = datetime.today().strftime('%m%d%Y')

    # Log file that captures console messages
    #logFile = os.path.basename(downloadFile).split('.')[0] + "_Download_ConsoleMsgs.txt"

    msgLogFile = f"{os.path.dirname(elevationMetadataFile)}{os.sep}USGS_3DEP_{resolution}_Step4_Create_SpatialFootprints_ConsoleMsgs.txt"

    h = open(msgLogFile,'w')
    h.write(f"Executing: USGS_4_Create_Elevation_Spatial_Footprints {today}\n\n")
    h.write(f"User Selected Parameters:\n")
    h.write(f"\tElevation Metadata File: {elevationMetadataFile}\n")
    h.write(f"\tOutput Footprint Directory: {outFpDir}\n")
    h.write(f"\tOutput Spatial Reference EPSG: {outputSRS}\n")
    h.write(f"\tLog File Path: {msgLogFile}\n")
    h.close()

    AddMsgAndPrint(f"\n{'='*125}")

    # List of header values from elevation metadata file
    headerValues = open(elevationMetadataFile).readline().rstrip().split(',')
    recCount = len(open(elevationMetadataFile).readlines()) - 1

    AddMsgAndPrint(f"There are {recCount:,} DEM files to create footprints for")

    # Convert input metadata elevation file to a dictionary
    # sourceID = [List of all attributes]
    files = convertMasterDBfileToDict(elevationMetadataFile)

    # list of tuples for footprint shapefile paths and corresponding EPSG [(path,EPSG)]
    fpShapes = list()
    wdir = os.path.dirname(sys.argv[0])

    fpTracker = 0
    failedFootprints = list()

    """ ---------------------------- run createFootPrint in mult-thread mode ---------------------------------------------------"""
    rastFpStart = tic()
    AddMsgAndPrint(f"\nCreating Spatial Footprints")
    numOfCores = psutil.cpu_count(logical = False)

    # Execute in Multi-threading mode
    with ThreadPoolExecutor(max_workers=numOfCores) as executor:
        ndProcessing = {executor.submit(createFootPrint, rastItems): rastItems for rastItems in files.items()}

        # yield future objects as they are done.
        for future in as_completed(ndProcessing):
            fpTracker +=1
            j=1
            for printMessage in future.result():
                if j==1:
                    AddMsgAndPrint(f"{printMessage} -- ({fpTracker:,} of {recCount:,})")
                else:
                    AddMsgAndPrint(printMessage)
                j+=1

    rastFpStop = toc(rastFpStart)

    # Merge the WGS84 Footprint shapefiles created by the 'createFootprint' function
    if len(fpShapes):
        mergeStart = tic()
        mergeFootPrints(outFpDir,fpShapes)
        mergeStop = toc(mergeStart)

    funEnds = toc(funStarts)

    """ ------------------------------------ SUMMARY -------------------------------------------- """
    AddMsgAndPrint(f"\n{'-'*40}SUMMARY{'-'*40}")

    AddMsgAndPrint(f"\nTotal Processing Time: {funEnds}")
    AddMsgAndPrint(f"\tCreate 'Data' Footprint Time: {rastFpStop}")
    AddMsgAndPrint(f"\tMerge Footprint Time: {mergeStop}")

    # Report number of DEMs processed
    if len(failedFootprints):
        AddMsgAndPrint(f"\nProcessed {(recCount-len(failedFootprints)):,} out of {recCount:,} DEM files")
    elif fpTracker == recCount:
        AddMsgAndPrint(f"\nSuccessfully Processed ALL {recCount:,} DEM files")
    elif fpTracker == 0:
        AddMsgAndPrint(f"\nFailed to Process ALL DEM files")


