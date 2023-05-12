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

        indexLayer = r'D:\projects\DSHub\reampling\dsh3m\USGS_DSH3M_Pro.shp'
        gridLayer = r'D:\projects\DSHub\reampling\snapGrid\grid_122880m_test.shp'

        result = dummy(indexLayer)
        msgs = result[0]
        dsh3mDEMlist = result[1]

        exit()


##        # get headers
##        source = ogr.Open(dsh3mIdxShp)
##        layer = source.GetLayer()
##        headers = []
##        ldefn = layer.GetLayerDefn()
##        for n in range(ldefn.GetFieldCount()):
##            fdefn = ldefn.GetFieldDefn(n)
##            headers.append(fdefn.name)
##        print(headers)
##        exit()

##        import geopandas as gpd
##
##        poly1 = gpd.read_file(dsh3mIdxShp)
##        poly2 = gpd.read_file(gridShp)
##
##        dsh3mDict = dict()
##
##        for idx, row in poly2.iterrows():
##            print('---------------------------------')
##            subPoly1 = poly1.within(row)
##            print(subPoly1)
##        exit()
##
##        polygon = poly2.geometry[0]
##
##
##        poly1.within(polygon)
##        poly1.intersects(polygon)
##
##    except:
##        errorMsg()

        # Read the best available 3M index
        index = gp.read_file(indexLayer)

        # Read the AOI grid
        grid = gp.read_file(gridLayer)

        demDict = dict()

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
                listOfDEMlists.append(vals)

            # Sort all lists by resolution and last_update date
            dateSorted = sorted(listOfDEMlists, key=itemgetter(32,3))

            demDict[rid] = dateSorted

    except:
        errorMsg()