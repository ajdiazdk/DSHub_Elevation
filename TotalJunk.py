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

        idxCopyShpPath = r'D:\projects\DSHub\reampling\dsh3m\USGS_DSH3M_Pro_DSH3M.shp'
        idxCopy_ds = ogr.GetDriverByName('ESRI Shapefile').Open(idxCopyShpPath,1)
        #idxCopy_Lyr = idxCopy_ds.GetLayerByIndex(0)
        idxCopy_Lyr = idxCopy_ds.GetLayer()
        layerDefinition = idxCopy_Lyr.GetLayerDefn()
        feat = idxCopy_Lyr.GetNextFeature()

        # iterate through DEM tiles and update statistics
        while feat:
            # sourceID of the record
            randVal = str(random.randint(0,10000000000))
            #sourceID = feat.GetField("sourceID")
            #print(f"Updating values for {sourceID}")
            feat.SetField("sourceID", "DONE8888")
            #feat=None
            #print(f"\tUpdating field sourceID FROM: {sourceID} TO: {randVal}")
            #idxCopy_Lyr.SetFeature(feat)
            feat = idxCopy_Lyr.GetNextFeature()

        idxCopy_Lyr.SetFeature(feat)
        idxCopy_ds.Destroy()
        del idxCopy_ds


    except:
        errorMsg()