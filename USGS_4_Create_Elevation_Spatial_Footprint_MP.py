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

The purpose of this script is to create a vector footprint around valid pixels for the input
DEMs.  The delineations are NOT bounding boxes.  Instead they act more like a convex hull
around pixels that are greater or equal the minimum value.  It excludes NODATA pixels.


Parameters:
    1) elevationMetadataFile - path to text file that contains elevation metadata
    4) outFpDir - boolean indicator to replace download file
    5) bDeleteZipFiles - boolean to delete or leave downloaded zipped files (only relevant if dl files are zip files)
                         set to False by default but can be overwritten in main.
"""

import os, subprocess, sys, traceback, re, glob, math, time, fnmatch, psutil, numpy
from datetime import datetime
from osgeo import gdal
from osgeo import ogr, osr
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

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
def toc(_start_time,seconds=False):
    """ Returns the total time by subtracting the start time - finish time"""

    try:

        t_sec = round(time.time() - _start_time)

        if seconds:
            return int(t_sec)

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
def convertMasterDBfileToDict(elevMetdataFile,outDir):
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
        AddMsgAndPrint("\nConverting input USGS Elevation Metadata File into a dictionary")

        # List of header values from elevation metadata file
        headerValues = open(elevMetdataFile).readline().rstrip().split(',')
        numOfTotalRecs = len(open(elevMetdataFile).readlines())-1

        # Position of DEM Name
        demNameIdx = headerValues.index("dem_name")
        sourceIDidx = headerValues.index('sourceid')

        masterDBfileDict = dict()
        recCount = 0
        badLines = 0
        fpShapesList = list()

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

                demName = items[demNameIdx]
                prjShape = f"{outDir}{os.sep}{demName[:-4]}_FP_WGS84.shp"

                # Check health of shapefile; delete and redo if it is corrupt; skip if healthy
                if os.path.exists(prjShape):

                    driver = ogr.GetDriverByName('ESRI Shapefile')

                    # Open Shapefile and assess health
                    try:
                        # Shapefile is Corrupt
                        dataSource = driver.Open(prjShape, 0)

                        if dataSource is None:
                            AddMsgAndPrint(f"\t-WGS84 Shapefile footprint exists but is corrupt: {os.path.basename(prjShape)}")

                            # Delete corrupted Shapefile
                            for tmpFile in glob.glob(f"{prjShape.split('.')[0]}*"):
                                os.remove(tmpFile)
                                if tmpFile.endswith('.shp'):
                                    AddMsgAndPrint(f"\t-Successfully Deleted Shapefile: {os.path.basename(tmpFile)}")

                        # Shapefile is healthy
                        else:
                            fpShapesList.append(prjShape)
                            del driver,dataSource
                            continue

                    # Error opening Shapefile - Corrupt
                    except:
                        # Delete corrupted Shapefile
                        for tmpFile in glob.glob(f"{prjShape.split('.')[0]}*"):
                            os.remove(tmpFile)
                            if tmpFile.endswith('.shp'):
                                AddMsgAndPrint(f"\t-Successfully Deleted Shapefile: {os.path.basename(tmpFile)}")

                sourceID = items[sourceIDidx]

                # Add info to elevMetadataDict
                masterDBfileDict[sourceID] = items
                recCount+=1
        del fp

        # Remove Header Value from count
        recCount-=1

        if numOfTotalRecs > 0:
            AddMsgAndPrint(f"\n\tElevation File contains {numOfTotalRecs:,} DEM files")

        if recCount > 0:
            AddMsgAndPrint(f"\t\tThere are {recCount:,} DEM files to create footprints for")

        if len(fpShapesList) > 0:
            AddMsgAndPrint(f"\t\tThere are {len(fpShapesList):,} Shapefile Footprints that already exist")

        if badLines > 0:
            AddMsgAndPrint(f"\tThere are(is) {badLines:,} records with anomalies found")

        if len(masterDBfileDict) == 0:
            AddMsgAndPrint(f"\t\tThere are no valid DEMs to create footprints for")
            return False,False,False

        return masterDBfileDict,headerValues,recCount,fpShapesList

    except:
        errorMsg()
        return False,False,False


#### ===================================================================================
def createFootPrint(items, headerValues,outFpDir,bGDAL):
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

        # Establish dict of contents to return
        returnDict = dict()
        returnDict['msgs'] = []
        returnDict['shp'] = []
        returnDict['fail'] = []
        returnDict['pTime'] = []

        filestart = tic()
        rasterRecord = items[1]

        # Positions of item elements
        demName = rasterRecord[headerValues.index("dem_name")]
        demDir = rasterRecord[headerValues.index("dem_path")]
        EPSG = int(rasterRecord[headerValues.index("epsg_code")])
        noData = float(rasterRecord[headerValues.index("nodataval")])
        minStat = float(rasterRecord[headerValues.index("rds_min")])

        demPath = f"{demDir}{os.sep}{demName}"
        returnDict['msgs'].append(f"\n\tProcessing {demName} - Size: {round(os.path.getsize(demPath) /1024,1):,} KB")

        # use the minimum stat to reclassify the input raster
        calcValue = ""
        if minStat > 0:
            # Round down to the nearest 100th 875.567 --> 800
            calcValue = float(math.floor(minStat / 100.00) * 100)
        else:
            calcValue = noData
        #returnDict['msgs'].append(f"\t\tThreshold Value: {calcValue}")

        # Temp Tiff that will be created and used to vectorize

        fileExt = demPath.split('.')[1] # tif, img
        tempTif = f"{demPath.split('.')[0]}_TEMP.{fileExt}"

        # Delete TEMP raster layer
        for tmpFile in glob.glob(f"{tempTif.split('.')[0]}*"):
            os.remove(tmpFile)
            #returnDict['msgs'].append(f"\t\tSuccessfully Deleted temp tiff: {os.path.basename(tmpFile)}")

        """ ------------------------- Create Raster Data Mask ----------------------------------- """
        calcStart = tic()

        if bGDAL:
            if os.name == 'nt':
                gdal_calc = f"gdal_calc -A {demPath} --outfile={tempTif} --type=Byte --calc=\"A>{calcValue}\" --NoDataValue=0"
            else:
                #gdal_calc = f"gdal_calc -A {demPath} --A_band=1 --outfile={tempTif} --type=Byte --calc=\"A>{calcValue}\" --NoDataValue=0"
                gdal_calc = f"python3 /bin/gdal_calc.py -A {demPath} --outfile={tempTif} --type=Byte --calc=\"A>{calcValue}\" --NoDataValue=0"

        else:
            # create binary raster NODATA=0, DATA = 1
            numpyCalc = '"numpy.where(A > ' + str(calcValue) + ', 1, A)"'
            gdal_calc = f"gdal_calc -A {demPath} --outfile={tempTif} --type=Byte --calc={numpyCalc} --NoDataValue=0"

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
            returnDict['msgs'].append(f"\t\t-Errors creating 'data' mask: {errors}")
        else:
            #msgDict['Success'] = f"Successfully created 'data' mask"
            pass
            ##returnDict['msgs'].append(f"\t\t-Successfully created 'data' mask using values >= {calcValue}")

        returnDict['msgs'].append(f"\t\t-GDAL Calc Process Time: {toc(calcStart)}")

        """ ------------------------- Create Empty Footprint Shapefile ----------------------------------- """
        emptyShpStart = tic()

        fpLyrName = f"{demName[:-4]}_FP"                    # New shapefile name - no ext
        fpShpPath = f"{outFpDir}{os.sep}{fpLyrName}.shp"    # New shapefile footprint path - temporary

        # Remove temp shapefile if it already exists
        if os.path.exists(fpShpPath):
            for tmpFile in glob.glob(f"{fpShpPath.split('.')[0]}*"):
                os.remove(tmpFile)
            ##returnDict['msgs'].append(f"\t\t-Successfully deleted existing Shp: {fpLyrName}.shp")

        # set the spatial reference for the shapefile; same as input raster
        sp_ref = osr.SpatialReference()
        sp_ref.ImportFromEPSG(EPSG)

        # set the shapefile driver and destination of the data source
        outDriver = ogr.GetDriverByName("ESRI Shapefile")
        outDataSource = outDriver.CreateDataSource(outFpDir)
        outLayer = outDataSource.CreateLayer(fpLyrName, srs=sp_ref, geom_type=ogr.wkbPolygon)

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

        # Raster field that will be used to vectorize
        rast_field = outLayer.GetLayerDefn().GetFieldIndex("rastValue")

        returnDict['msgs'].append(f"\t\t-Create empty shapefile Process Time: {toc(emptyShpStart)}")

        """ ------------------------- Vectorize Raster Data Mask ----------------------------------- """
        vectorStart = tic()

        # Open raster to pass a band object to polygonize function
        ds = gdal.Open(tempTif, gdal.GA_ReadOnly)
        band = ds.GetRasterBand(1)

        # vectorize mask
        #result = gdal.Polygonize(band, None if , outLayer, rast_field, [], callback=None)
        result = gdal.Polygonize(band, band, outLayer, rast_field, [], callback=None)
        assert result == 0, AddMsgAndPrint(f"\tPolygonize failed")
        ##returnDict['msgs'].append(f"\t\t-Successfully vectorized mask into: {fpLyrName}.shp")

        # Close the raster
        ds = None
        returnDict['msgs'].append(f"\t\t-Vectorize Mask Process Time: {toc(vectorStart)}")

        updateAttributes = tic()
        # populate the shapefile fields after polygons exist
        for i in range(0, outLayer.GetFeatureCount()):
            feature = outLayer.GetFeature(i)
            for j in range(0,outLayerDefn.GetFieldCount()-1):
                feature.SetField(outLayerDefn.GetFieldDefn(j).GetName(),rasterRecord[j])
                #AddMsgAndPrint(f"{outLayerDefn.GetFieldDefn(j).GetName()} -- {rasterRecord[j]}")
                test = outLayer.SetFeature(feature)

        # close the shapefile and workspace
        outDataSource.FlushCache()
        outDataSource = None

        del outLayer, ds, band
        returnDict['msgs'].append(f"\t\t-Update Attribute Time: {toc(updateAttributes)}")

        """ ------------------------- Project Mask Shapefile to WGS84 ----------------------------------- """
        projectShpStart = tic()

        # projected output Shapefile
        prjShape = f"{outFpDir}{os.sep}{demName[:-4]}_FP_WGS84.shp"

        # Remove prj shapefile if it already exists
        if os.path.exists(prjShape):
            print("EXISTS")
            for tmpFile in glob.glob(f"{prjShape.split('.')[0]}*"):
                os.remove(tmpFile)

        prjCmd = f"ogr2ogr -f \"ESRI Shapefile\" {prjShape} {fpShpPath} -t_srs EPSG:4326 -s_srs EPSG:{EPSG} "# -lco ENCODING=UTF-8"

        execCmd = subprocess.Popen(prjCmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
        msgs, errors = execCmd.communicate()
        errors = ','.join(errors.strip().split('\n'))  # some errors return mutliple carriage returns

        # Create 4326 prj file if not created; not sure why the prj is not automatically created.
        prjFile = f"{prjShape[0:-4]}.prj"
        if not os.path.exists(prjFile):
            spatialRef = osr.SpatialReference()
            spatialRef.ImportFromEPSG(4326)
            spatialRef.MorphToESRI()
            file = open(prjFile, 'w')
            file.write(spatialRef.ExportToWkt())
            file.close()
            ##returnDict['msgs'].append(f"\t\t-Successfully Created WGS84 PRJ File: {prjFile}")

        returnDict['shp'].append(prjShape)
        ##returnDict['msgs'].append(f"\t\t-Successfully Projected: {os.path.basename(fpShpPath)} to WGS84")

        returnDict['msgs'].append(f"\t\t-Project Shapefile Process Time: {toc(projectShpStart)}")

        """ ------------------------- Delete Temp Files ----------------------------------- """
        # Delete TEMP raster layer
        for tmpFile in glob.glob(f"{tempTif.split('.')[0]}*"):
            os.remove(tmpFile)
            ##returnDict['msgs'].append(f"\t\t-Successfully Deleted temp: {os.path.basename(tmpFile)}")

        # Delete TEMP shapefile layer
        if os.path.exists(fpShpPath):

            for tmpFile in glob.glob(f"{fpShpPath.split('.')[0]}.*"):
                os.remove(tmpFile)
        ##returnDict['msgs'].append(f"\t\t-Successfully Deleted temp: {os.path.basename(fpShpPath)}")

        returnDict['msgs'].append(f"\t\t-Process Time: {toc(filestart)}")
        returnDict['pTime'].append(toc(filestart,seconds=True))

        del outDataSource
        return returnDict

    except:
        returnDict['msgs'].append(errorMsg(errorOption=2))
        returnDict['msgs'].append(f"\t\t-Process Time: {toc(filestart)}")
        returnDict['fail'].append(items)
        return returnDict

#### ===================================================================================
def mergeFootPrints(outDir,shapefiles):

    try:
        AddMsgAndPrint(f"\nMerging Shapefile Spatial Footprints")

        """ ------------------  Create empty output merged shapefile -------------------------------"""
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

        return mergeShapePath

    except:
        errorMsg()
        return False


#### ===================================================================================

if __name__ == '__main__':

    funStarts = tic()

##    # Parameter 1 - Metadata Elevation File
##    elevationMetadataFile = input("\nEnter full path to USGS Metadata Master Elevation File: ")
##    while not os.path.exists(elevationMetadataFile):
##        print(f"{elevationMetadataFile} does NOT exist. Try Again")
##        elevationMetadataFile = input("Enter full path to USGS Metadata Master Elevation File: ")
##
##    # Parameter 2 - Directory where spatial footprints will be saved
##    outFpDir = input("\nEnter path where elevation footprints files will be saved to: ")
##    while not os.path.exists(outFpDir):
##        print(f"{outFpDir} does NOT exist. Try Again")
##        outFpDir = input("Enter path where elevation footprints files will be saved to: ")

    # Parameters
    elevationMetadataFile = r'E:\GIS_Projects\DS_Hub\Elevation\DSHub_Elevation\USGS_Text_Files\1M\20230728_windows\USGS_3DEP_1M_Step2_Elevation_Metadata_10.txt'
    outFpDir = r'D:\projects\DSHub\USGS_Spatial_Footprints\1M'
    bMultiProcess = True
    bGDAL = True
    outputSRS = 4326

    """ ---------------------------- Establish Console LOG FILE ---------------------------------------------------"""
    tempList = elevationMetadataFile.split(os.sep)[-1].split('_')
    startPos = tempList.index(fnmatch.filter(tempList, '3DEP*')[0]) + 1
    endPos = tempList.index(fnmatch.filter(tempList, 'Step*')[0])
    resolution = '_'.join(tempList[startPos:endPos])
    today = datetime.today().strftime('%m%d%Y')

    # Log file that captures console messages
    #logFile = os.path.basename(downloadFile).split('.')[0] + "_Download_ConsoleMsgs.txt"

    msgLogFile = f"{os.path.dirname(elevationMetadataFile)}{os.sep}USGS_3DEP_{resolution}_Step4_Create_SpatialFootprints_{'MP' if bMultiProcess else 'SP'}_ConsoleMsgs.txt"

    h = open(msgLogFile,'w')
    h.write(f"Executing: USGS_4_Create_Elevation_Spatial_Footprints {today}\n\n")
    h.write(f"User Selected Parameters:\n")
    h.write(f"\tElevation Metadata File: {elevationMetadataFile}\n")
    h.write(f"\tOutput Footprint Directory: {outFpDir}\n")
    h.write(f"\tOutput Spatial Reference EPSG: {outputSRS}\n")
    h.write(f"\tProcessing Mode: {'Multi-Processing' if bMultiProcess else 'Single-Processing'}\n")
    h.write(f"\tLog File Path: {msgLogFile}\n")
    h.close()

    AddMsgAndPrint(f"\n{'='*125}")

    """ ---------------------------- Retrieve Information from Elevation file ---------------------------------------------------"""

    # retrieve info from elevation metadata file
    # files = dict() containing DEM info key=sourceID values=[list of DEM record]
    # headerValues = list of the headers
    # recCount = Number of records to create footprint for
    # fpShapes = list of existing shapefile footprints
    files,headerValues,recCount,fpShapes = convertMasterDBfileToDict(elevationMetadataFile,outFpDir)

    if not files:
        AddMsgAndPrint(f"\nThere are no valid DEMs to create footprints for.  Exiting")
        exit()

    fpTracker = 0
    failedFootprints = list()
    processTimes = list()

    """ ---------------------------- run createFootPrint in Mult-Process mode ---------------------------------------------------"""
    rastFpStart = tic()
    if bMultiProcess:

        if os.name == 'nt':
            numOfCores = int(psutil.cpu_count(logical = False))         # 16 workers
        else:
            numOfCores = int(psutil.cpu_count(logical = True) / 2)      # 32 workers
        AddMsgAndPrint(f"\nCreating Spatial Footprints in Multi-Process Mode using {numOfCores} workers for {recCount:,} DEM files")

        # Execute in Multi-processing mode
        with ThreadPoolExecutor(max_workers=numOfCores) as executor:
        #with ProcessPoolExecutor(max_workers=numOfCores) as executor:
            ndProcessing = [executor.submit(createFootPrint, rastItems, headerValues, outFpDir, bGDAL) for rastItems in files.items()]

            # yield future objects as they are done.
            for future in as_completed(ndProcessing):
                fpTracker +=1
                j=1

                returnDict = future.result()
                batchMsgs = list()
                for printMessage in returnDict['msgs']:

                    if j==1:
                        batchMsgs.append(f"{printMessage} -- ({fpTracker:,} of {recCount:,})")
                    else:
                        batchMsgs.append(printMessage)
                    j+=1

                AddMsgAndPrint(None,msgList=batchMsgs)

                if returnDict['shp']:
                    fpShapes.append(returnDict['shp'][0])

                if returnDict['fail']:
                    failedFootprints.append(returnDict['fail'][0])

                if returnDict['pTime']:
                    processTimes.append(returnDict['pTime'][0])

        rastFpStop = toc(rastFpStart)

    else:
        AddMsgAndPrint(f"\nCreating Spatial Footprints in Single-Process Mode for {recCount:,} DEM files")

        for rastItems in files.items():
            returnDict = createFootPrint(rastItems,headerValues,outFpDir,bGDAL)

            batchMsgs = list()
            fpTracker +=1
            j=1

            for printMessage in returnDict['msgs']:
                if j==1:
                    batchMsgs.append(f"{printMessage} -- ({fpTracker:,} of {recCount:,})")
                else:
                    batchMsgs.append(printMessage)
                j+=1

            AddMsgAndPrint(None,msgList=batchMsgs)

            if returnDict['shp']:
                fpShapes.append(returnDict['shp'][0])

            if returnDict['fail']:
                failedFootprints.append(returnDict['fail'][0])

            if returnDict['pTime']:
                processTimes.append(returnDict['pTime'][0])

        rastFpStop = toc(rastFpStart)


    """ ---------------------------- Merge WGS84 Footprint Shapefiles ----------------------------"""
    # Merge the WGS84 Footprint shapefiles created by the 'createFootprint' function
    if len(fpShapes):
        mergeStart = tic()
        mergedShapefile = mergeFootPrints(outFpDir,fpShapes)
        mergeStop = toc(mergeStart)

    funEnds = toc(funStarts)

    """ ---------------------------- Delete WGS84 Footprint Shapefiles ----------------------------"""
    if mergedShapefile:
        AddMsgAndPrint(f"\nDeleting {len(fpShapes):,} WGS84 Footprint Shapefiles")
        deleteCnt = 0

        # Add each shapefile to the merged layer
        driver = ogr.GetDriverByName("ESRI Shapefile")
        for shapefile in fpShapes:
            if os.path.exists(shapefile):
                driver.DeleteDataSource(shapefile)
                print(f"\t\tSuccessfully Deleted {os.path.basename(shapefile)}")
                deleteCnt+=1

    ##        for tmpFile in glob.glob(f"{shapefile.split('.')[0]}*"):
    ##            os.remove(tmpFile)
    ##            if tmpFile.endswith('.shp'):
    ##                print(f"\t\tSuccessfully Deleted {os.path.basename(shapefile)}")
    ##                deleteCnt+=1

        AddMsgAndPrint(f"\n\t\tSuccessfully Deleted {deleteCnt:,} WGS84 Footprint Shapefiles")
    else:
        AddMsgAndPrint(f"\nMerge Failed so the {len(fpShapes):,} WGS84 Footprint Shapefiles will not be deleted")

    """ ------------------------------------ SUMMARY -------------------------------------------- """
    AddMsgAndPrint(f"\n{'-'*40}SUMMARY{'-'*40}")

    AddMsgAndPrint(f"\nFinal Merged Footprint Path: {mergedShapefile}")

    AddMsgAndPrint(f"\nTotal Processing Time: {funEnds}")
    AddMsgAndPrint(f"\tCreate 'Data' Footprint Time: {rastFpStop}")
    AddMsgAndPrint(f"\t\tAverage Processing Time per DEM file: {int(sum(processTimes) / len(processTimes))} seconds")
    AddMsgAndPrint(f"\tMerge Footprint Time: {mergeStop}")

    # Report number of DEMs processed
    if len(failedFootprints):
        AddMsgAndPrint(f"\nProcessed {(recCount-len(failedFootprints)):,} out of {recCount:,} DEM files")
    elif fpTracker == recCount:
        AddMsgAndPrint(f"\nSuccessfully Processed ALL {recCount:,} DEM files")
    elif fpTracker == 0:
        AddMsgAndPrint(f"\nFailed to Process ALL DEM files")


