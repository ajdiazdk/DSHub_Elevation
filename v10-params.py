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
def ndras(ras, arg):

    msgs = dict()
    msgs['00.File'] = ras
    print(ras)


    try:

        dirname = os.path.dirname(ras)
        fname = os.path.basename(ras)[:-4]
        rid = 'fp-60586972-'

        #dest = os.path.join(os.path.dirname(ras), 'rpj_' + os.path.basename(ras))
        #gdal.Warp(dest, ras, dstSRS="EPSG:3338")

        ds = gdal.Open(ras)

        proj = osr.SpatialReference(wkt=ds.GetProjection())
        srcSRS = proj.GetAttrValue('AUTHORITY',1)

        band = ds.GetRasterBand(1)
        nd = band.GetNoDataValue()

        msgs['01.NoData'] = str(nd)

        dstBIN = os.path.join(dirname, fname + rid + 'bin.tif')

        # create binary raster NODATA=0, DATA = 1
        calc = '"numpy.where(A > ' + str(nd) + ', 1, A)"'
        # calcOrg = '"numpy.where(A>=887, 1, A)"'
        gdal_calc = 'python3 /bin/gdal_calc.py' \
                    ' -A {0} ' \
                    '--outfile={1} ' \
                    '--calc={2} ' \
                    '--type=Byte ' \
                    '--overwrite'.format(ras, dstBIN, calc)
                    # '--overwrite'.format(dstTIF, dstBIN, calc)

        # print(gdal_calc)
        # subprocess.run(gdal_calc, shell=True, stdout=subprocess.DEVNULL)
        calcres = subprocess.Popen(gdal_calc, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,universal_newlines=True)

        calcmsgs, calcerrors = calcres.communicate()
        calcerrors = ','.join(calcerrors.strip().split('\n'))  # some errors return mutliple carriage returns

        # if not calcerrors is None or not calcres.returncode == 0:
        if not calcerrors == '':

            msgs['02.gdal_calc'] = 'Fail-' + calcerrors

        else:

            msgs['02.gdal_calc'] = 'Success'

        del ds

        # polygonize DATA / NODATA
        # there was an attempt to stuff this into postgres
        # gdal returned message saying this is not possible
        ds = gdal.Open(dstBIN)
        band = ds.GetRasterBand(1)

        layername = rid + fname

        # check to see if the shapefile had been created in prev run
        extensions = ['.shp', '.shx', '.dbf','.prj']
        components = [os.path.join(dirname, layername + x) for x in extensions]
        for c in components:
            if os.path.exists(c):
                os.remove(c)

        drv = ogr.GetDriverByName("ESRI Shapefile")
        dst_ds = drv.CreateDataSource(dirname)

        sp_ref = osr.SpatialReference()
        sp_ref.SetFromUserInput('EPSG:' + str(srcSRS))

        dst_layer = dst_ds.CreateLayer(layername, srs = sp_ref, geom_type = ogr.wkbPolygon)
        fld = ogr.FieldDefn("value", ogr.OFTInteger)
        dst_layer.CreateField(fld)
        dst_field = dst_layer.GetLayerDefn().GetFieldIndex("value")

        gdal.Polygonize(band, None, dst_layer, dst_field, [], callback=None)

        dst_ds.FlushCache()

        # add the tile name / id to the attribute table
        field_name = ogr.FieldDefn("tile_id", ogr.OFTString)
        dst_layer.CreateField(field_name)
        field_name.SetWidth(200)

        for i in range(0, dst_layer.GetFeatureCount()):
            feature = dst_layer.GetFeature(i)
            feature.SetField("tile_id", os.path.basename(ras))
            dst_layer.SetFeature(feature)


        dst_ds.FlushCache()

        # gather a collection of shapefile, full path name and coord sys
        shape = os.path.join(dirname, rid + fname + '.shp')

        # clean up
        os.remove(dstBIN)

        # run the command, note the reprojection from srcSRS to epsg
        shp2pgsql = 'shp2pgsql ' \
                    '-a ' \
                    '-s ' + str(srcSRS) + ':' + str(epsg) + \
                    '-i ' \
                    + shape + \
                    ' metadata.footprint_' + reg + """_""" + argv1 + """_""" + epsg + '_meta | ' \
                    'PGPASSWORD=gotmeta ' \
                    'psql ' \
                    '-d metadata ' \
                    '-h 10.11.11.10 ' \
                    '-p 6432 ' \
                    '-U meta'

        shp2 = subprocess.Popen(shp2pgsql, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        shp2msgs, shp2errors = shp2.communicate()
        shp2errors = ','.join(shp2errors.strip().split('\n'))  # some errors return mutliple carriage returns

        # if not shp2errors is None or not shp2.returncode == 0:
        if not shp2.returncode == 0:

            msgs['03.shp2pg'] = 'Fail-' + shp2errors

        else:

            msgs['03.shp2pg'] = 'Success'

        del ds
        # clean up
        #shpdir = os.path.dirname(shape)
        #shpid =  os.path.basename(shape)[:-4]

        #os.remove(os.path.join(shpdir, shpid + '.shp'))
        #os.remove(os.path.join(shpdir, shpid + '.shx'))
        #os.remove(os.path.join(shpdir, shpid + '.dbf'))
        #os.remove(os.path.join(shpdir, shpid + '.prj'))

        # too verbose for debugging only
        # result = subprocess.run(shp2pgsql, capture_output=True, text=True, env=env, shell=True)
        # print("stdout:", result.stdout)
        # print("stderr:", result.stderr)

        msgs['04.function'] = 'Success'

        return 'function success', msgs

    except Exception as e:

        msgs['04.function'] = 'Fail-' + str(e)

        print(e)

        return 'Function failure', msgs




from contextlib import closing
import os, psycopg2, datetime, subprocess, sys
from osgeo import gdal
from osgeo import ogr, osr
import threading, multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

os.environ['OPENBLAS_NUM_THREADS'] = '1'

start = datetime.datetime.now()

gdal.UseExceptions()

srcTable = 'us_elevation_3m_4269_meta'

srcCount = srcTable.count("_")
if not srcCount == 4:
    err = """Inappropriate table name.  Must include vlaues for
    1. Region
    2. Raster resolution
    3. Destination epsg"""

    raise ValueError(err)

params = srcTable.split("_")

# region
reg = params[0]
if reg not in ['conus', 'ak', 'ak2', 'us']:
    err = "Unknown region, must be member of  ['conus', 'ak','us','ak2']"
    raise ValueError(err)


# resolution
# argv1 is old method
argv1 = params[2]
if argv1 not in ['1m', '3m', '5m', '10m', 'opr','30m']:
    err = ("table reference is not a member of ['1m', '3m', '5m', '10m','30m', 'opr']")
    raise ValueError(err)

# source resoultion
# drop m for meter
source_res = argv1[:-1]

# spatial reference
epsg = params[3]
if not int(epsg):
    err = 'epsg is not a valid integer'
    raise ValueError(err)

q1 = """
DROP TABLE IF EXISTS metadata.footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta;

CREATE TABLE metadata.footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta AS TABLE metadata.""" + srcTable + """ WITH NO DATA;

ALTER TABLE metadata.footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta
ADD COLUMN tile_id varchar,
ADD COLUMN "value" integer;

SELECT AddGeometryColumn('metadata', 'footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta', 'geom', """ + epsg + """, 'MULTIPOLYGON', 2);

SELECT concat_ws('/', dem_path, dem_name) as filepath FROM metadata.""" + srcTable + """;"""

with closing(psycopg2.connect(host='10.11.11.10', user='meta', \
                              password='gotmeta', \
                              dbname='metadata', port=6432)) as conn:
    with conn, conn.cursor() as cursor:


        cursor.execute(q1)
        res = cursor.fetchall()


        files = [i[0] for i in res]
#        files = files[0:5]
        print(files)

wdir = os.path.dirname(sys.argv[0])

with open(os.path.join(wdir, reg + '_nodata-logger_' + argv1 + '.log'), mode='w') as f:


    jobs = str(len(files))
    job = 0
    # for raster in files:
    with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        future = {executor.submit(ndras, raster, argv1): raster for raster in files}

        # use a set comprehension to start all tasks.  This creates a future object
        # runR2pgsqlCmd = {executor.submit(runRaster2pgsql, cmd): cmd for cmd in raster2pgsqlList}

        for response in as_completed(future):

            resp, ans = response.result()
            print(resp)
            for k,v in ans.items():
                f.write(k + ': ' + v + '\n')
            f.write('\n')

            job +=1
            print('completed job ' + str(job) + ' of ' + jobs)


    # doing admin stuff
    # populating the empty fields
    # where value = 1 (isdata)
    # adding index
    #

    finish = datetime.datetime.now()

    run = 'Runtime: ' + str(finish-start)
    print(run)
    f.write(run)


# ALTER TABLE metadata.footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta ADD COLUMN "source_res" smallint DEFAULT """ + source_res + """;

q2 = """UPDATE metadata.footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta fp
SET (poly_code,prod_title,pub_date,lastupdate,rds_size,format,sourceid,meta_url,downld_url,dem_name,dem_path,rds_column,rds_rows,bandcount,cellsize,rdsformat,bitdepth,nodataval,srs_type,epsg_code,srs_name,rds_top,rds_left,rds_right,rds_bottom,rds_min,rds_mean,rds_max,rds_stdev,blk_xsize,blk_ysize) = (org.poly_code,org.prod_title,org.pub_date,org.lastupdate,org.rds_size,org.format,org.sourceid,org.meta_url,org.downld_url,org.dem_name,org.dem_path,org.rds_column,org.rds_rows,org.bandcount,org.cellsize,org.rdsformat,org.bitdepth,org.nodataval,org.srs_type,org.epsg_code,org.srs_name,org.rds_top,org.rds_left,org.rds_right,org.rds_bottom,org.rds_min,org.rds_mean,org.rds_max,org.rds_stdev,org.blk_xsize,org.blk_ysize)
FROM metadata.""" + srcTable + """ org
WHERE fp.tile_id = org.dem_name;

CREATE INDEX fp""" + reg + """_""" + argv1 + """_""" + epsg + """_idx ON metadata.footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta USING GIST (geom);

DROP TABLE IF EXISTS metadata.dshub_data_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta;
CREATE TABLE metadata.dshub_data_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta AS SELECT * FROM metadata.footprint_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta WHERE "value" = 1;
CREATE INDEX idfp""" + reg + """_""" + argv1 + """_""" + epsg + """_idx ON metadata.dshub_data_""" + reg + """_""" + argv1 + """_""" + epsg + """_meta USING GIST (geom);
"""

with closing(psycopg2.connect(host='10.11.11.10', user='meta', \
                              password='gotmeta', \
                              dbname='metadata', port=6432)) as conn:
    with conn, conn.cursor() as cursor:

        cursor.execute(q2)

