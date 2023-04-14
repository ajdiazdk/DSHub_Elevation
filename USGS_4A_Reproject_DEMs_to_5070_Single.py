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

import os
import threading, multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime
from osgeo import gdal, osr, ogr


if __name__ == '__main__':

    input_raster = r'D:\projects\DSHub\reampling\1M\USGS_1M_14_x34y421_KS_StatewideFordGray_2018_A18.tif'
    out_raster = f"{input_raster.split('.')[0]}_EPSG5070.{input_raster.split('.')[1]}"

    rds = gdal.Open(input_raster)
    rdsInfo = gdal.Info(rds,format="json")

    # ----------------------------------------------  From ONLINE
    # Get input SRS in EPSG -- this is temporary; SRID will be passed in
##    inSpatialRef = rds.GetSpatialRef()
##    sr = osr.SpatialReference(str(inSpatialRef))
##    res = sr.AutoIdentifyEPSG()
##    srid = sr.GetAuthorityCode(None)
##    inSpatialRef.ImportFromEPSG(int(srid))


    # ----------------------------------------------  From script #4
    # Set source Coordinate system to EPSG from input record
    inSpatialRef = osr.SpatialReference()
    inSpatialRef.ImportFromEPSG(4269)
    inputSRS = f"EPSG:{inSpatialRef.GetAuthorityCode(None)}"

    # Set output Coordinate system to 5070
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(5070)

    # create Transformation
    coordTrans = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)


##    # Temp code for bringing in Extent Coordinates
##    adfGeoTransform = rds.GetGeoTransform(can_return_null = True)
##    if adfGeoTransform is not None:
##        # Left
##        dfGeoXUL = adfGeoTransform[0]
##        # Top
##        dfGeoYUL = adfGeoTransform[3]
##        # Right
##        dfGeoXLR = adfGeoTransform[0] + adfGeoTransform[1] * rds.RasterXSize + adfGeoTransform[2] * rds.RasterYSize
##        # Bottom
##        dfGeoYLR = adfGeoTransform[3] + adfGeoTransform[4] * rds.RasterXSize + adfGeoTransform[5] * rds.RasterYSize

    right,top = rdsInfo['cornerCoordinates']['upperRight']   # Eastern-Northern most extent
    left,bottom = rdsInfo['cornerCoordinates']['lowerLeft']  # Western - Southern most extent

    # Create a geometry object of X,Y coordinates for LL, UL, UR, and LR in the source SRS
##    pointLL = ogr.CreateGeometryFromWkt("POINT ("+str(left)+" " +str(bottom)+")")
##    pointUL = ogr.CreateGeometryFromWkt("POINT ("+str(left)+" " +str(top)+")")
##    pointUR = ogr.CreateGeometryFromWkt("POINT ("+str(right)+" " +str(top)+")")
##    pointLR = ogr.CreateGeometryFromWkt("POINT ("+str(right)+" " +str(bottom)+")")

    # Reverse LAT/LONG
    pointLL = ogr.CreateGeometryFromWkt("POINT ("+str(bottom)+" " +str(left)+")")
    pointUL = ogr.CreateGeometryFromWkt("POINT ("+str(top)+" " +str(left)+")")
    pointUR = ogr.CreateGeometryFromWkt("POINT ("+str(top)+" " +str(right)+")")
    pointLR = ogr.CreateGeometryFromWkt("POINT ("+str(bottom)+" " +str(right)+")")

    #print(f"\n------- EPSG: {(int(srid))} Exents -------")
    print(f"Before LL Coords - {pointLL}")
    print(f"Before UL Coords - {pointUL}")
    print(f"Before UR Coords - {pointUR}")
    print(f"Before LR Coords - {pointLR}\n")

    # Project individual coordinates to 5070
    # 'POINT (800676.587222594 1918952.70626254)'
    pointLL.Transform(coordTrans)
    pointUL.Transform(coordTrans)
    pointUR.Transform(coordTrans)
    pointLR.Transform(coordTrans)

    print(f"\n------- EPSG: {outSpatialRef.GetAuthorityCode(None)} Exents -------")
    print(f"After LL Coords - {pointLL}")
    print(f"After UL Coords - {pointUL}")
    print(f"After UR Coords - {pointUR}")
    print(f"After LR Coords - {pointLR}")

    # Convert the Transform object into a List of projected coordinates
    # [(1308220.3216564057, -526949.4675336559)]
    prjLL = pointLL.GetPoints()[0]
    prjUL = pointUL.GetPoints()[0]
    prjUR = pointUR.GetPoints()[0]
    prjLR = pointLR.GetPoints()[0]

    # Get the highest Y-coord to determine the most northern (top) extent - Ymax
    prjTop = max(prjUL[1],prjUR[1])

    # Get the lowest Y-coord to determine the most southern (bottom) extent - Ymin
    prjBottom = min(prjLL[1],prjLR[1])

    # Get the lowest X-coord to determine the most western (left) extent - Xmin
    prjLeft = min(prjUL[0],prjLL[0])

    # Get the highest X-coord to determine the most eastern (right) extent - Xmax
    prjRight = max(prjUR[0],prjLR[0])

    # calculate number of rows and columns
    prjColumns = abs(prjLeft - prjRight)
    prjRows = abs(prjTop - prjBottom)

    print("\n------- Projected Exents ------")
    print(f"Top: {prjTop}")
    print(f"Bottom: {prjBottom}")
    print(f"Left: {prjLeft}")
    print(f"Right: {prjRight}\n")

    # Left (xMin) is the only coordinate with a possible negative value
    # if prjLeft coordinate is west of center longitude (-96) then coord is negative
    if prjLeft < 0:
        westOf96 = True
    else:
        westOf96 = False

    # Truncate the coordinate otherwise it mod(%) 3 will never equal 0.
    # coordinate precision is unnecessary here.
    newTop = int(prjTop)
    newBottom = int(prjBottom)
    newLeft = abs(int(prjLeft))
    newRight = int(prjRight)

    extPos = 0
    for extent in [newTop,newRight,newBottom,newLeft]:

        bDivisibleBy3 = False
        while not bDivisibleBy3:

            # Remainder of
            # Update new coordinate values if there is no remainder when dividing by 3.
            if extent % 3 == 0:

                # Update extent variables
                if extPos == 0:
                    newTop = extent + 3
                elif extPos == 1:
                    newRight = extent + 3
                elif extPos == 2:
                    newBottom = extent - 3
                elif extPos == 3:
                    if westOf96:
                        newLeft = -abs(extent - 3)
                    else:
                        newLeft = extent - 3

                extPos+=1
                bDivisibleBy3 = True

            else:
                # Top and Right coordinates will add 1 meter;
                # Left and Bottom coordinates will subtract 1 meter
                if extPos <= 1:
                    extent = extent + 1
                else:
                    extent = extent - 1

    print("\n------- Snapped Exents ------")
    print(f"New Top Extent: {newTop}")
    print(f"New Bottom Extent: {newBottom}")
    print(f"New Left Extent: {newLeft}")
    print(f"New Right Extent: {newRight}")

    #args = gdal.WarpOptions(xRes=3,yRes=3,srcSRS='EPSG:26916',dstSRS='EPSG:5070')
    args = gdal.WarpOptions(format='GTiff',
                            xRes=3,
                            yRes=3,
                            srcSRS='EPSG:26916',
                            dstSRS='EPSG:5070',
                            outputBounds=[newLeft, newBottom, newRight, newTop],
                            outputBoundsSRS="EPSG:5070",
                            resampleAlg=gdal.GRA_Bilinear)

    gdal.Warp(out_raster, input_raster, options=args)
    print(f"Successfully created {out_raster}")

##    del rds,rdsInfo
##
##    rds = gdal.Open(out_raster)
##    rdsInfo = gdal.Info(rds,format="json")
##
##    gdalRight,gdalTop = rdsInfo['cornerCoordinates']['upperRight']   # Eastern-Northern most extent
##    gdalLeft,gdalBottom = rdsInfo['cornerCoordinates']['lowerLeft']  # Western - Southern most extent
##
##    print("\n------- Snapped Exents from projected and resampled raster ------")
##    print(f"New Top Extent: {gdalTop}")
##    print(f"New Bottom Extent: {gdalBottom}")
##    print(f"New Left Extent: {gdalLeft}")
##    print(f"New Right Extent: {gdalRight}")