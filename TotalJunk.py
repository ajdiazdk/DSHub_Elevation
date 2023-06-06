#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Adolfo.Diaz
#
# Created:     31/10/2022
# Copyright:   (c) Adolfo.Diaz 2022
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import sys, time, os, traceback
from osgeo import gdal
from osgeo import osr
from osgeo import ogr
import geopandas as gp
from operator import itemgetter
import geopandas as gp
import random

## ===================================================================================
def AddMsgAndPrint(msg):

    # Print message to python message console
    print(msg)

## ===================================================================================
def errorMsg(errorOption=1):

    """ By default, error messages will be printed and logged immediately.
        If errorOption is set to 2, return the message back to the function that it
        was called from.  This is used in DownloadElevationTile function or in functions
        that use multi-threading functionality"""

    try:

        exc_type, exc_value, exc_traceback = sys.exc_info()
        theMsg = "\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[1] + "\n\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[-1]

        print(theMsg)

    except:
        print("Unhandled error in unHandledException method")
        pass

## ===================================================================================
def dummy(indexL):

    index = gp.read_file(indexL)
    headers = index.columns.tolist()

    messageList = list()
    messageList.append("Hello how are you?")
    messageList.append("I'm doing fine.")
    messageList.append("good to hear")

    del index
    return(messageList,[])


if __name__ == '__main__':

    try:

        idx = r'D:\projects\DSHub\reampling\dsh3m\metadata_index.shp'
        grid = r'D:\projects\DSHub\reampling\snapGrid\grid_122880m_test.shp'

        driver = ogr.GetDriverByName('ESRI Shapefile')

        idx_ds = driver.Open(idx, 0) # 0 means read-only. 1 means writeable.
        grid_ds = driver.Open(grid, 0)

        # Validate SoilE3M Elevation Index
        if idx_ds is None:
            AddMsgAndPrint(f"\tERROR: Could not open DSH3M Index Layer: {dsh3mIdxShp} -- EXITING!")

        else:
            idx_Lyr = idx_ds.GetLayer()
            idxFeatCount = idx_Lyr.GetFeatureCount()
            layerDefinition = idx_Lyr.GetLayerDefn()
            numOfFields = layerDefinition.GetFieldCount()
            AddMsgAndPrint(f"\n\tNumber of USGS Index features in {os.path.basename(idx)}: {idxFeatCount:,}")

        # Int - shapefile field count
        fieldCount = layerDefinition.GetFieldCount()
        headerValues = [layerDefinition.GetFieldDefn(i).GetName() for i in range(fieldCount)]

        # Validate grid layer
        if grid_ds is None:
            AddMsgAndPrint(f"\tERROR: Could not open Grid Layer: {grid} -- EXITING!")

        else:
            gridLyr = grid_ds.GetLayer()
            gridFeatCount = gridLyr.GetFeatureCount()
            AddMsgAndPrint(f"\tNumber of Grid Cells in {os.path.basename(grid)}: {gridFeatCount:,}\n")

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

            # Store unique source IDs to get an accurate DEM file count
            uniqueSourceIDs = list()

            # iterate through every feature and get DEM information
            for idxFeat in idx_Lyr:

                # List of attributes for 1 DEM
                idxFeatValList = list()

                sourceID = idxFeat.GetField("sourceid")
                if sourceID in uniqueSourceIDs:
                    continue
                else:
                    uniqueSourceIDs.append(sourceID)

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
            sourcePos = headerValues.index('source_res')
            lastUpdatePos = headerValues.index('lastupdate')
            dateSorted = sorted(listOfDEMlists, key=itemgetter(sourcePos,lastUpdatePos), reverse=True)
            gridIndexOverlayDict[rid] = dateSorted

            # Summarize the selected DEMs by resolution - unnecessary but useful
            oneMcnt = 0
            threeMcnt = 0
            tenMcnt = 0
            other = 0

            for demList in listOfDEMlists:
                if demList[sourcePos] == 1:
                    oneMcnt+=1
                elif demList[sourcePos] == 3:
                    threeMcnt+=1
                elif demList[sourcePos] == 10:
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


    except:
        errorMsg()