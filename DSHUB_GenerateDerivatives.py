# -*- coding: utf-8 -*-
"""
Created on Tue Aug 15 14:00:57 2023

Script Name: DSHUB_BigDataProcessing_Example.py
Created on Fri Sep  2 10:12:13 2022
updated 7/27/2023

@author: Adolfo.Diaz
GIS Business Analyst
USDA - NRCS - SPSD - Soil Services and Information
email address: adolfo.diaz@usda.gov
cell: 608.215.7291

The purpose of this script is to create a slope dataset to prototype

"""

## ========================================== Import modules ===============================================================
import sys, os, traceback, time, psutil
from datetime import datetime

from concurrent.futures import ThreadPoolExecutor, as_completed

from osgeo import gdal
from osgeo import ogr,osr
import whitebox

wbt = whitebox.WhiteboxTools()

## ===================================================================================
def AddMsgAndPrint(msg,msgList=list()):
    
    # Add message to log file
    try:
        if msgList:
            #h = open(msgLogFile,'a+')
            for msg in msgList:
                print(msg)
                #h.write("\n" + msg)
            #h.close
            #del h

        else:
            print(msg)
            #h = open(msgLogFile,'a+')
            #h.write("\n" + msg)
            #h.close
            #del h
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
def convertMasterDBfileToDict(elevMetdataFile):
    """ Opens the Master Elevation Database CSV file containing the metadata for every
        DEM file, including statistical and geographical information.  Parses the content
        into a dictionary with the sourceId being the key and the rest of the information
        seriving as the key in a list format.
        
        Assumes the ID is the first field; returns headervalues under the key 'headers'

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
        #mDBFnumOfRecs = len(open(elevMetdataFile).readlines())
        headerValues = open(elevMetdataFile).readline().rstrip().split(',')

        masterDBfileDict = dict()
        recCount = 0
        badLines = 0

        """ ---------------------------- Open Download File and Parse Information ----------------------------------"""
        with open(elevMetdataFile, 'r') as fp:
            for line in fp:
                items = line.split(',')
                items[-1] = items[-1].strip('\n')  # remove the hard return of the line
                recCount+=1

                # Add header line to dict
                if recCount == 1:
                    masterDBfileDict['headers'] = items
                
                # Skip Empty line
                elif line == "\n":
                    continue

                # Skip if number of items are incorrect
                elif len(items) != len(headerValues):
                    AddMsgAndPrint(f"\tLine # {recCount} has {len(items)} out of {len(headerValues)} values")
                    badLines+=1
                    continue

                # Skip if a # was found
                else: 
                    
                    if '#' in items:
                        emptyFields = items.count('#')
                        #errorPos = items.index('#')
                        #AddMsgAndPrint(f"\tLine # {recCount} has an error value for field: '{headerValues[errorPos]}'")
                        AddMsgAndPrint(f"\tWARNING: Line # {recCount} has {emptyFields} empty fields")
                        badLines+=1
                
                    # assume ID is the first field
                    sourceID = str(items[0])
    
                    # Add info to elevMetadataDict
                    if not sourceID in masterDBfileDict:
                        masterDBfileDict[sourceID] = items
                    else:
                        AddMsgAndPrint(f"\tWARNING: ID {sourceID} is duplicated -- Look into this")

        del fp

        if len(masterDBfileDict) == 0:
            AddMsgAndPrint(f"\tElevation Metadata File: {os.path.basename(elevMetdataFile)} was empty!")
            return False

        if badLines > 0:
            AddMsgAndPrint(f"\tThere are(is) {badLines} records with anomalies found")

        return masterDBfileDict

    except:
        errorMsg()
        return False


## ================================================================================================================
def createBuffer(bndryDriver, bndryDriverAttr, bufferDist):
    """This function buffers features of a layer and saves them to a new Layer
    """
    try:
        print(f"\nBuffering {os.path.basename(bndryDriver)} by {bufferDist} Meters")
        
        #  tuple (minX, maxX, minY, maxY)
        bufferExtents = dict()
        
        # Input Boundary Driver layer - HUC8 or Grid layer to perform data chunking
        inputds = ogr.Open(bndryDriver)
        inputlyr = inputds.GetLayer()
        sRef = inputlyr.GetSpatialRef()
        inputEPSG = int(sRef.GetAuthorityCode(None))        # Get EPSG from inShape
        inLayerDefn = inputlyr.GetLayerDefn()
    
        # Create Output Boundary Driver Buffered Layer
        shpdriver = ogr.GetDriverByName('ESRI Shapefile')
        outBufferShape = f"{bndryDriver.split('.')[0]}_{bufferDist}.shp"

        if os.path.exists(outBufferShape):
            shpdriver.DeleteDataSource(outBufferShape)
         
        outputBufferds = shpdriver.CreateDataSource(outBufferShape)
        bufferlyr = outputBufferds.CreateLayer(outBufferShape, geom_type=ogr.wkbPolygon)
        featureDefn = bufferlyr.GetLayerDefn()
        
        # Add field names to newly created output Boundary buffered shapefile
        for i in range(inLayerDefn.GetFieldCount()):
            fieldDefn = inLayerDefn.GetFieldDefn(i)
            bufferlyr.CreateField(fieldDefn)
    
        for feature in inputlyr:
            ingeom = feature.GetGeometryRef()
            code = feature.GetField(bndryDriverAttr)
            
            geomBuffer = ingeom.Buffer(bufferDist)
            outFeature = ogr.Feature(featureDefn)
            
            # Get Envelope returns a tuple (minX, maxX, minY, maxY)
            extent = geomBuffer.GetEnvelope()
            bufferExtents[code] = extent
            
            # Get all attributes related to this geometry record
            for i in range(0, inLayerDefn.GetFieldCount()):
                outFeature.SetField(inLayerDefn.GetFieldDefn(i).GetNameRef(),feature.GetField(i))
            
            outFeature.SetGeometry(geomBuffer)
            bufferlyr.CreateFeature(outFeature)
            
            outFeature = None
            
        # Create 4326 prj file if not created; not sure why the prj is not automatically created.
        prjFile = f"{outBufferShape[0:-4]}.prj"
        if not os.path.exists(prjFile):
            spatialRef = osr.SpatialReference()
            spatialRef.ImportFromEPSG(inputEPSG)
            spatialRef.MorphToESRI()
            file = open(prjFile, 'w')
            file.write(spatialRef.ExportToWkt())
            file.close()
            print(f"\tSuccessfully Created {inputEPSG} PRJ File: {prjFile}")
            
        print(f"\tBuffering Complete: {os.path.basename(outBufferShape)}")
        return outBufferShape,bufferExtents
            
    except:
        errorMsg()
        
## ================================================================================================================
def createOverlay(bufferLayer, bufferAtr, gridIndex, gridAttr, metaFileDict, bVerbose=False):
    """ This function will create a reference dictionary between the bufferLayer and gridIndex
        by selecting all polygons from gridIndex that intersect each polygon from bufferLayer.  
        
        i.e. if bufferLayer contains buffered HUC8 units and gridIndex contains 122880m grids then
        return dictionary with huc as keys and list of grids that interest that huc.
        {11040006: [pathTo366,pathTo399,pathTo400,pathTo401]}
        
    """
    try:
        
        AddMsgAndPrint(f"\nCreating reference overlay between {os.path.basename(bufferLayer)} and {os.path.basename(gridIndex)}")
        start = tic()
        overlayDict = dict()
        noOverlap = list()
        
        # Get information from elevation metadata file
        headerValues = metaFileDict['headers']
        demName = headerValues.index('dem_name')
        demPath = headerValues.index('dem_path')
            
        driver = ogr.GetDriverByName('ESRI Shapefile')
        
        bufferShp = driver.Open(bufferLayer,0)
        gridShp = driver.Open(gridIndex,0)
    
        bufferLyr = bufferShp.GetLayer()
        gridLyr = gridShp.GetLayer()
        
        # Iterate through buffered polygons
        for feature1 in bufferLyr:
            
           geom1 = feature1.GetGeometryRef()
           attribute1 = feature1.GetField(bufferAtr)
           
           if bVerbose:AddMsgAndPrint(f"\n\tHUC {attribute1}")
           
           # Log the grids that intersect with buffered polygon X
           for feature2 in gridLyr:
               
              geom2 = feature2.GetGeometryRef()
              attribute2 = str(feature2.GetField(gridAttr))
              
              # select only the intersections
              if geom2.Intersects(geom1): 
                  
                  if not attribute2 in metaFileDict:
                      if bVerbose:AddMsgAndPrint(f"\t\tGrid {attribute2} does not exist in elevation metadata file")
                      continue
                  
                    # concatenate dem_path and dem_name from elevation metadata file
                  dem = f"{metaFileDict[attribute2][demPath]}{os.sep}{metaFileDict[attribute2][demName]}"
                  
                  # Add info to urlDownloadDict
                  if attribute1 in overlayDict:
                      overlayDict[attribute1].append(dem)
                  else:
                      overlayDict[attribute1] = [dem]
                        
                  #intersection = geom2.Intersection(geom1)
                  if bVerbose:AddMsgAndPrint(f"\t\tGrid {attribute2}")
                    
           if not attribute1 in overlayDict:
               overlayDict[attribute1] = [None]
               noOverlap.append(attribute1)
                
        if noOverlap:
            AddMsgAndPrint(f"\tThere are {len(noOverlap)} {os.path.basename(bufferLayer)} polygons with no overlap with {os.path.basename(gridIndex)} layer")
                  
        AddMsgAndPrint(f"\n\t{toc(start)}")
        return overlayDict
    
    except:
        errorMsg()
        
## ================================================================================================================
def extractDEMs(items, bufferLayer, bndryDriverAttr, bufferExtents, bReplace):
    """ This function will create a raster from 1 or more DEMs using the bufferlayer polygon as a mask.
        items is a tuple = ('11040006', [366, 399, 400, 401])
    
    """
    try:
        extractStart = tic()
        
        # Establish dict of contents to return
        returnDict = dict()
        returnDict['msgs'] = []
        returnDict['rast'] = []
        returnDict['fail'] = []
        returnDict['pTime'] = []
        
        driverCode = items[0] # driver ID
        listOfDEMs = items[1] # list of DEMs that intersect the driver ID
        
        # tuple (minX, maxX, minY, maxY)
        extent = bufferExtents[driverCode]
        
        returnDict['msgs'].append(f"\n\tExtracting DEM for ID: {driverCode} -- # of DEMs to extract from {len(listOfDEMs)}")
        
        if os.name == 'nt':
            folder = r'F:\DSHub\WB_BreachDepressions_out'
        else:
            folder = getDownloadFolder(driverCode,None,True)
    
        mergeRaster = os.path.join(folder,f"tmp_{driverCode}_DEM.tif")
    
        if os.path.exists(mergeRaster):
            try:
                if bReplace:
                    os.remove(mergeRaster)
                    returnDict['msgs'].append(f"\t\tSuccessfully Deleted {os.path.basename(mergeRaster)}")
                    
                else:
                    returnDict['msgs'].append(f"\t\t{'Temp DEM Exists:':<15} {os.path.basename(mergeRaster):<60}")
                    return returnDict
            except:
                returnDict['fail'].append(f"\t\t{'Failed to Delete':<35} {mergeRaster:<60}")
                return returnDict
    
        # [Left, Bottom, Right, Top]
        args = gdal.WarpOptions(format="GTiff",
                                srcNodata=-999999.0,
                                dstNodata=-999999.0,
                                outputBounds=extent,
                                cutlineDSName=bufferLayer,
                                cutlineWhere=f"{bndryDriverAttr}='{driverCode}'",
                                cropToCutline=True,
                                creationOptions=["COMPRESS=DEFLATE", "TILED=YES","PREDICTOR=2","ZLEVEL=9",
                                                  "BIGTIFF=YES","PROFILE=GeoTIFF"],
                                multithread=True)

        g = gdal.Warp(mergeRaster, listOfDEMs, options=args)
        g = None
        
        returnDict['msgs'].append(f"\t\tOutput File: {mergeRaster}")
        returnDict['msgs'].append(f"\t\tProcessing Time: {toc(extractStart)}")
        returnDict['pTime'].append(f"{toc(extractStart)}")
        returnDict['rast'].append(mergeRaster)
        return returnDict

    except:
        returnDict['msgs'].append(errorMsg(errorOption=2))
        returnDict['msgs'].append(f"\t\t-Process Time: {toc(extractStart)}")
        returnDict['fail'].append(items)
        return returnDict                
                    
## ===================================================================================
def getDownloadFolder(id,abbreviation,tempDir=False):
    # Create 8-digit huc Directory structure in the designated EBS mount volume
    # '07060002' --> ['07', '06', '00', '02'] --> '07\\06\\00\\02'
    # /data03/gisdata/elev/09/0904/090400/09040004/

    try:

        # last digit
        ld = str(id[-1])

        if ld in ["1","9","0"]:
            root = "/data06/gisdata/elev"
        elif ld in ["2","7","8"]:
            root = "/data07/gisdata/elev"
        elif ld in ["3","6"]:
            root = "/data08/gisdata/elev"
        elif ld in ["4","5"]:
            root = "/data09/gisdata/elev"
            
        if tempDir:
            downloadFolder = f"{root}{os.sep}Temp"
        else:
            downloadFolder = f"{root}{os.sep}{abbreviation}"
            
        if not os.path.exists(downloadFolder):
            os.makedirs(downloadFolder)

        return downloadFolder

    except:
        errorMsg()
        return False
    
#### ===================================================================================

if __name__ == '__main__':
    
    funStarts = tic()
    gdal.UseExceptions()
    
    # Parameters
    bndryDriver = r'D:\projects\DSHub\elevationDers\wbdhu8_dsh3m_test_5070.shp'
    bndryDriverAttr = 'huc8'
    bufferDist = 1000
    gridIndex = r'D:\projects\DSHub\elevationDers\grid_122880m.shp'
    gridAttr = 'rid'
    elevationMetadataFile = r'D:\projects\DSHub\elevationDers\USGS_3DEP_DSH3M_Step5_Mosaic_Elevation_Metadata.txt'
    bReplace = False
    bMultiProcess = True
    
    # Step 1 - Get Boundary Driver Shapefile information
    driver = ogr.GetDriverByName('ESRI Shapefile')
    dataSource = driver.Open(bndryDriver, 0) # 0 means read-only. 1 means writeable
    
    # Check to see if shapefile is found.
    if dataSource is None:
        AddMsgAndPrint(f"\nFailed to open {bndryDriver} shapefile.  Exiting!")
        sys.exit()
    else:
        layer = dataSource.GetLayer()
        bndryDriverCount = layer.GetFeatureCount()
        AddMsgAndPrint(f"\n{os.path.basename(bndryDriver)} Info:")
        AddMsgAndPrint(f"\tNumber of features: {bndryDriverCount}")  
    
    # Step 2 - Convert input metadata elevation file to a dictionary
    # sourceID = [List of all attributes]
    metaFileDict = convertMasterDBfileToDict(elevationMetadataFile)
    
    # Step 3 - Buffer polygons from Boundary Driver
    bufferLayer,bufferExtents = createBuffer(bndryDriver, bndryDriverAttr, bufferDist)
    
    # Step 4 - Create Overlap dictionary to answer which DEMs intersect each bndryDriver poly
    # {'11040006': [pathTo366,pathTo399,pathTo400,pathTo401]}
    overlayDict = createOverlay(bufferLayer,bndryDriverAttr,gridIndex,gridAttr, metaFileDict, False)

    # Step 5 - Extract DEMs using the buffered bndryDriver as a mask; tmpID.tif
    listOfExtractedDEMs = list()
    failToExtract = list()
    processTimes = list()
    extractStart = tic()
    numOfCores = psutil.cpu_count(logical = False)
    tracker = 0

    if bMultiProcess:
        AddMsgAndPrint(f"\nExtracting DEMs for {bndryDriverCount} {os.path.basename(bndryDriver)} features - Multi-threading Mode")
        
        # Execute in Multi-threading mode
        with ThreadPoolExecutor(max_workers=numOfCores) as executor:
            ndProcessing = {executor.submit(extractDEMs, items, bufferLayer, bndryDriverAttr, bufferExtents, bReplace): items for items in overlayDict.items()}
    
            # yield future objects as they are done.
            for future in as_completed(ndProcessing):
                returnDict = future.result()
                
                tracker+=1
                j=1
                batchMsgs = list()
                
                for printMessage in returnDict['msgs']:
    
                    if j==1:
                        batchMsgs.append(f"{printMessage} -- ({tracker:,} of {bndryDriverCount:,})")
                    else:
                        batchMsgs.append(printMessage)
                    j+=1
    
                AddMsgAndPrint(None,msgList=batchMsgs)
    
                # Arrange results
                if returnDict['rast']:
                    listOfExtractedDEMs.append(returnDict['rast'][0])
                if returnDict['fail']:
                    failToExtract.append(returnDict['fail'][0])
                if returnDict['pTime']:
                    processTimes.append(returnDict['pTime'][0])
                
    else:
        AddMsgAndPrint(f"\nExtracting DEMs for {bndryDriverCount} {os.path.basename(bndryDriver)} features - Single Processing Mode")
        for items in overlayDict.items():
            returnDict = extractDEMs(items, bufferLayer, bndryDriverAttr, bufferExtents, bReplace)
            
            tracker+=1
            j=1
            batchMsgs = list()
            
            for printMessage in returnDict['msgs']:
                if j==1:
                    batchMsgs.append(f"{printMessage} -- ({tracker:,} of {bndryDriverCount:,})")
                else:
                    batchMsgs.append(printMessage)
                j+=1

            AddMsgAndPrint(None,msgList=batchMsgs)

            # Arrange results
            if returnDict['rast']:
                listOfExtractedDEMs.append(returnDict['rast'][0])
            if returnDict['fail']:
                failToExtract.append(returnDict['fail'][0])
            if returnDict['pTime']:
                processTimes.append(returnDict['pTime'][0])

    extractStop = toc(extractStart)
         
    AddMsgAndPrint(f"\n{toc(funStarts)}")
          
    
    # rasterIn = r'D:\projects\DSHub\WB_BreachDepressions\USGS_1M_11_x28y415_CA_SouthernSierra_2020_B20.tif'
    # rasterOut = r'F:\DSHub\WB_BreachDepressions_out\USGS_1M_11_x28y415_CA_SouthernSierra_2020_B20_BreachDep.tif'
   
    # wbt.verbose = True
    
    # wbt.breach_depressions(rasterIn, rasterOut)
    # wbt.breach_depressions_least_cost(rasterIn, rasterOut)
    
    
    