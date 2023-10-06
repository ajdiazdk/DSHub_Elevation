# -*- coding: utf-8 -*-
"""
Created on Wed Apr 12 12:48:16 2023

@author: 28200310160021036376
"""

# function ->
# takes input raster ->
# extracts to array ->
# numpy.where data/nodat ->
# writes binary data/nodata to new raster ->
# 'polygonize' to shapefile ->
# append shapefile to postgis table

import ray
@ray.remote
def ndras(ras, arg):
    
    # fstart and ffininsh are variables 
    # used to time each file
    # and return in msgs dict
    fstart = datetime.datetime.now()
    msgs = dict()
    msgs['00.File'] = ras
       
    try:
        
        dirname = os.path.dirname(ras)
        fname = os.path.basename(ras)[:-4]
        
        # random sequence to find 
        # for intermediate steps
        # for future deletes
        rid = 'fp-60586972'
        
        layername = rid + fname
        
        # # imnportant, don't rely on the epsg code, use the full sr object
        # # warp the input raster to 3338 to get a working epsg code
        # orgProj = osr.SpatialReference(orgRas.GetProjection()).Clone()
        # wo = gdal.WarpOptions(srcSRS=orgProj, creationOptions=["COMPRESS=LZW"])
        # gdal.Warp(dest, ras, dstSRS="EPSG:3338", warpOptions=wo)
        
        # orgRas is None
        
        # use the reprojected raster to build footprint from
        ds = gdal.Open(ras)
        
        # proj = osr.SpatialReference(wkt=ds.GetProjection())
        proj = osr.SpatialReference(ds.GetProjection()).Clone()
        srcSRS = proj.GetAttrValue('AUTHORITY',1)
        
        band = ds.GetRasterBand(1)
        nd = band.GetNoDataValue()
        rows, cols = ds.RasterYSize, ds.RasterXSize
        
        #  create the output data set
        dstBIN = os.path.join(dirname, fname + "_" + rid + '_bin.tif')
        driver = ds.GetDriver()

        ods = driver.Create(dstBIN, cols, rows, 1, gdal.GDT_Byte)
        oband = ods.GetRasterBand(1)
        
        arr = band.ReadAsArray()

        # Data =2, NoData =1
        arr = np.where(arr > nd, 2, 1)
        
        oband.WriteArray(arr, 0, 0)
        oband.FlushCache()
        
        ods.SetGeoTransform(ds.GetGeoTransform())
        ods.SetProjection(ds.GetProjection())
        ods.FlushCache()
        ods is None
        del ods
                
        msgs['01.NoData'] = str(nd)
            
        # polygonize DATA / NODATA = 1 /2
        # there was an attempt to stuff this into pg raster
        # gdal returned message saying this is not possible
        ds = gdal.Open(dstBIN)
        band = ds.GetRasterBand(1)
        
        drv = ogr.GetDriverByName("ESRI Shapefile")
        dst_ds = drv.CreateDataSource(dirname)    
        
        # srs is the full projection object from input raster
        # remember in AK we have to reproject to 3338
        dst_layer = dst_ds.CreateLayer(layername, srs = proj, geom_type = ogr.wkbPolygon)
        fld = ogr.FieldDefn("value", ogr.OFTInteger)
        dst_layer.CreateField(fld)
        dst_field = dst_layer.GetLayerDefn().GetFieldIndex("value")
        
        gdal.Polygonize(band, None, dst_layer, dst_field, [], callback=None)
        
        # add the tile name / id to the attribute table
        field_name = ogr.FieldDefn("tile_id", ogr.OFTString)
        dst_layer.CreateField(field_name)
        field_name.SetWidth(200)
        
        for i in range(0, dst_layer.GetFeatureCount()):
            feature = dst_layer.GetFeature(i)
            feature.SetField("tile_id", os.path.basename(ras))
            dst_layer.SetFeature(feature)
            
        dst_ds.FlushCache()
        dst_ds is None        
    
        # gather the shapefile from polygonize
        shape = os.path.join(dirname, rid + fname + '.shp')
        
        # clean up
        # doesn't seem to work
        # henece try statement
        try:
            dstBIN.FlushCache()            
            os.remove(dstBIN)
        except:
            pass
        
        # run the command, note the reprojection from srcSRS to epsg
        env={'PGPASSWORD':pwd,
             'PGHOST' : 'localhost',
             'PGPORT' : '5432',
             'PGUSER' : 'postgres',
             'PGDATABASE' : 'metadata',
             'SYSTEMROOT': os.environ['SYSTEMROOT']}
        
        
        # note we are appending (-a flag)
        # to a table created in main
        # '-s ' + str(srcSRS) + ':' + str(epsg) + \
        # '-s 0:' + str(epsg) + \
        shp2pgsql =  r'"C:\Program Files\PostgreSQL\15\bin\shp2pgsql.exe" ' \
                    '-a ' \
                    '-s ' + srcSRS + ':' + str(epsg) + \
                    ' -i ' \
                    + shape + \
                    ' metadata.footprint_' + reg + """_""" + argv1 + """_""" + epsg + '_meta | ' \
                    r'"C:\Program Files\PostgreSQL\15\bin\psql.exe" ' \
                    '-d  metadata ' \
                    '-h localhost ' \
                    '-p 5432 ' \
                    '-U postgres'
        
        # print to debug in DOS, if needed
        # print(shp2pgsql)
        
        shp2 = subprocess.Popen(shp2pgsql, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, universal_newlines=True)
       
        shp2msgs, shp2errors = shp2.communicate()
        shp2errors = ','.join(shp2errors.strip().split('\n'))  # some errors return mutliple carriage returns
        
        # subprocess.run(shp2pgsql, shell=True, env=env)
        
        # if not shp2errors is None or not shp2.returncode == 0:
        if not shp2.returncode == 0:
        
            msgs['03.shp2pg'] = 'Fail-' + shp2errors
        
        else:
            
            msgs['03.shp2pg'] = 'Success'
        
        del ds
        
        # too verbose for debugging only
        # result = subprocess.run(shp2pgsql, capture_output=True, text=True, env=env, shell=True)
        # print("stdout:", result.stdout)
        # print("stderr:", result.stderr)
        
        msgs['04.function'] = 'Success'
        
        ffinish = datetime.datetime.now()
        ftime = ffinish-fstart
        msgs['05.filetime'] = str(ftime)
        
        return 'function success', msgs

    except Exception as e:
        
        msgs['04.function'] = 'Fail-' + str(e) 
        
        ffinish = datetime.datetime.now()
        ftime = ffinish-fstart
        msgs['05.filetime'] = str(ftime)
        
        print(e)
        
        return 'function failure', msgs
        
        
from contextlib import closing
import ray, numpy as np
import os, psycopg2, datetime, subprocess, sys
from osgeo import gdal
from osgeo import ogr, osr

# srcTable is used to read
# dem_path and dem_name
# it's hard coded, could be raw_input
# same with db password
srcTable = 'us_elevation_1m_5070_meta'
pwd = input('Enter database password:')

#  can't remember but think this was something multiprocessor needed on Windows
# obsolete with ray???
os.environ['OPENBLAS_NUM_THREADS'] = '1'

# where you need to start
# searching for rasters
# on prod this requires walk, not listdirs
root = r'D:\GIS\PROJECT_23\DS-HUB\TEST'
files = os.listdir(root)

# delete all files with random id (rid)
# in previous runs
dfiles = [x for x in files if x.find('fp-60586972') > -1]
for d in dfiles:
    os.remove(os.path.join(root, d))

start = datetime.datetime.now()

gdal.UseExceptions()

srcCount = srcTable.count("_")
if not srcCount == 4:
    err = """Inappropriate table name.  Must include vlaues for
    1. Region
    2. Raster resolution
    3. Destination epsg"""

    raise ValueError(err)

# split srcTable to list, evaluate 
# region, resolution, output srs desired
# THIS HAS TO COME FROM srcTable METADATA TABLE NAME!!!!!!
params = srcTable.split("_")

# region
reg = params[0]
if reg not in ['us', 'conus', 'ak', 'ak3', 'ak2']:
    err = "region reference is not a member of ['conus', 'ak', 'ak3', 'ak2']"
    raise ValueError(err)
    
# resolution argv1 is orig definition, kept it
argv1 = params[2]
if argv1 not in ['1m', '3m', '5m', '10m', 'opr', '30m']:
    err = ("resolution reference is not a member of ['1m', '3m', '5m', '10m', 'opr']")
    raise ValueError(err)

# source resoultion
# drop m for meter
source_res = argv1[:-1]

# spatial reference
epsg = params[3]
if not int(epsg):
    err = 'epsg reference is not a valid integer'
    raise ValueError(err)

q1 = """
DROP TABLE IF EXISTS metadata.footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta;

CREATE TABLE metadata.footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta AS TABLE metadata.""" + srcTable + """ WITH NO DATA;

ALTER TABLE metadata.footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta
ADD COLUMN tile_id varchar, 
ADD COLUMN "value" integer;

SELECT AddGeometryColumn('metadata', 'footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta', 'geom', """ + epsg + """, 'MULTIPOLYGON', 2);

SELECT concat_ws('\\', dem_path, dem_name) as filepath, rds_size FROM metadata.""" + srcTable + """;""" #"ORDER BY rds_size;"""

with closing(psycopg2.connect(host='localhost', user='postgres', \
                              password=pwd, \
                              dbname='metadata', port=5432)) as conn:
    with conn, conn.cursor() as cursor:
        
        cursor.execute(q1)
        res = cursor.fetchall()
        
files = [i[0] for i in res]

# if needed trim list down for debug
files = files[0:100]       

wdir = os.path.dirname(sys.argv[0])
jobs = str(len(files))
job = 0       
with open(os.path.join(wdir, reg + '_nodata-logger_' + argv1 + '.log'), mode='w') as f:

    
    # this was an attempt to split the files list
    # into lists of 24 (# of my local cores) to let multiporcessor
    # not step on its own toes (incrementally increasing process time)
    # didn't seem to have much affect, if any

    # splits = np.split(files, np.arange(24,len(files),24))
    # for split in splits:
    #     aslist = split.tolist()
    
    # ray builds the jobs here
    futures = [ndras.remote(file, argv1) for file in files]        
    
    # the return is a list of tuples
    # [
    #     ('function success/function failure', {msgs dict}),
    #     ('function success/function failure', {msgs dict}),
    #     ('function success/function failure', {msgs dict}),
    #     ...
    # ]
    # they aren't accessible until all 
    # jobs (futures) are finished
    results = ray.get(futures)
    
    for r in results:
        
        # get msgs dict
        # write to log file
        for k,v in r[1].items():
            f.write(k + ': ' + v + '\n')
        f.write('\n')
    
    ray.shutdown()
    # job +=1
    # print('completed job ' + str(job) + ' of ' + jobs)        
    
    finish = datetime.datetime.now()
    
    run = 'Runtime: ' + str(finish-start)
    print(run)
    f.write(run)

# these queries update the spatial footprint table
# created in q1 that we've been appending to 
# using srcTable to populate all footprint attributes
# based on the footprint file_id and metadata dem_name 
# a new table dshub_data... is created
# carrying geometries witha value of Data (2), NoData (1) is dropped
q2 = """UPDATE metadata.footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta fp
SET (poly_code,prod_title,pub_date,lastupdate,rds_size,format,sourceid,meta_url,downld_url,dem_name,dem_path,rds_column,rds_rows,bandcount,cellsize,rdsformat,bitdepth,nodataval,srs_type,epsg_code,srs_name,rds_top,rds_left,rds_right,rds_bottom,rds_min,rds_mean,rds_max,rds_stdev,blk_xsize,blk_ysize) = (org.poly_code,org.prod_title,org.pub_date,org.lastupdate,org.rds_size,org.format,org.sourceid,org.meta_url,org.downld_url,org.dem_name,org.dem_path,org.rds_column,org.rds_rows,org.bandcount,org.cellsize,org.rdsformat,org.bitdepth,org.nodataval,org.srs_type,org.epsg_code,org.srs_name,org.rds_top,org.rds_left,org.rds_right,org.rds_bottom,org.rds_min,org.rds_mean,org.rds_max,org.rds_stdev,org.blk_xsize,org.blk_ysize)
FROM metadata.""" + srcTable + """ org
WHERE fp.tile_id = org.dem_name;

CREATE INDEX fp""" + reg + """_""" + argv1 + """_""" + epsg + """_idx ON metadata.footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta USING GIST (geom);

DROP TABLE IF EXISTS metadata.dshub_data_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta; 
CREATE TABLE metadata.dshub_data_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta AS SELECT * FROM metadata.footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta WHERE "value" = 1;
CREATE INDEX idfp""" + reg + """_""" + argv1 + """_""" + epsg + """_idx ON metadata.dshub_data_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta USING GIST (geom);
"""

with closing(psycopg2.connect(host='localhost', user='postgres', \
                              password=pwd, \
                              dbname='metadata', port=5432)) as conn:
    with conn, conn.cursor() as cursor:
        
        cursor.execute(q2)
    
