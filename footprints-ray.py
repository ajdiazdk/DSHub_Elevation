# -*- coding: utf-8 -*-
"""
Created on Wed Apr 12 12:48:16 2023
Update on Mon Sep 25 1:24:50 2023

@author: 28200310160021036376
@updated: 28162023070706025871800
"""

import os

os.environ["OPENBLAS_NUM_THREADS"] = "1"

import argparse
import datetime
import subprocess
import sys
from getpass import getpass
from pathlib import Path

import numpy as np
import ray
from osgeo import gdal, ogr, osr

from utils import clean, db_params, execute_query, get_db_connection, list_meta_tables

gdal.UseExceptions()


def get_params_from_meta_table(table_name: str):
    """
    Get SQL parameters from metadata table name: region, resolution, and epsg

    Parameters
    ----------
    table_name : str
        The metadata table name

    Returns
    -------
     tuple[str, str, str]
        The region, resolution, and epsg of the table name

    Raises
    ------
    ValueError
        Inappropriate table name
    ValueError
        Unknown region
    ValueError
        Resolution is not recognized
    ValueError
        EPSG not an integer
    """
    if table_name is None:
        raise ValueError("Missing table name as input")
    if isinstance(table_name, str) == False:
        raise TypeError("Table name must be a string")

    src_count = table_name.count("_")
    if src_count != 4:
        err = """Inappropriate table name.  Must include values for
        1. Region
        2. Raster resolution
        3. Destination epsg"""
        raise ValueError(err)

    params = table_name.split("_")

    # geogrpahic region
    region = params[0]
    if region not in ["conus", "ak", "pb", "prvi"]:
        err = "Unknown region, must be member of  ['conus', 'ak', 'pb', 'prvi']"
        raise ValueError(err)

    # resolution
    resolution = params[2]
    if resolution not in ["1m", "3m", "5mopr", "5mifsar", "10m", "30m"]:
        err = "Table reference is not a member of ['1m', '3m', '5mopr', '5mifsar', '10m', '30m']"
        raise ValueError(err)

    # spatial reference
    epsg = params[3]
    if not int(epsg):
        err = "EPSG is not a valid integer"
        raise ValueError(err)

    return region, resolution, epsg


def generate_q1(srctable, region, resolution, epsg):
    query = (
        f"""DROP TABLE IF EXISTS metadata.{region}_elevation_{resolution}_{epsg}_footprint;"""
        + f"""CREATE TABLE metadata.{region}_elevation_{resolution}_{epsg}_footprint
                AS TABLE metadata.{srctable} WITH NO DATA;"""
        + f"""ALTER TABLE metadata.{region}_elevation_{resolution}_{epsg}_footprint
                ADD COLUMN tile_id varchar, ADD COLUMN "value" integer;"""
        + f"""ALTER TABLE metadata.{region}_elevation_{resolution}_{epsg}_footprint
                SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);"""
        + f"""SELECT AddGeometryColumn(
                'metadata',
                '{region}_elevation_{resolution}_{epsg}_footprint',
                'geom', {epsg}, 'MULTIPOLYGON', 2);"""
        + f"""SELECT concat_ws('/', dem_path, dem_name) as filepath
                FROM metadata.{srctable};"""
    )
    return query


def generate_q2(srctable, region, resolution, epsg):
    query = (
        f"""UPDATE metadata.{region}_elevation_{resolution}_{epsg}_footprint fp
                SET (poly_code,prod_title,pub_date,lastupdate,rds_size,format,
                    sourceid,meta_url,downld_url,dem_name,dem_path,rds_column,
                    rds_rows,bandcount,cellsize,rdsformat,bitdepth,nodataval,
                    srs_type,epsg_code,srs_name,rds_top,rds_left,rds_right,
                    rds_bottom,rds_min,rds_mean,rds_max,rds_stdev,blk_xsize,
                    blk_ysize) = 
                    (org.poly_code,org.prod_title,org.pub_date,org.lastupdate,org.rds_size,org.format,
                    org.sourceid,org.meta_url,org.downld_url,org.dem_name,org.dem_path,org.rds_column,
                    org.rds_rows,org.bandcount,org.cellsize,org.rdsformat,org.bitdepth,org.nodataval,
                    org.srs_type,org.epsg_code,org.srs_name,org.rds_top,org.rds_left,org.rds_right,
                    org.rds_bottom,org.rds_min,org.rds_mean,org.rds_max,org.rds_stdev,org.blk_xsize,
                    org.blk_ysize)
                FROM metadata.{srctable} org
                WHERE fp.tile_id = org.dem_name;"""
        + f"""CREATE INDEX fp{region}_{resolution}_{epsg}_idx 
                ON metadata.{region}_elevation_{resolution}_{epsg}_footprint 
                USING GIST (geom);"""
        + f"""DROP TABLE IF EXISTS metadata.{region}_elevation_{resolution}_{epsg}_footprint_dshub;"""
        + f"""CREATE TABLE metadata.{region}_elevation_{resolution}_{epsg}_footprint_dshub 
                AS SELECT * FROM metadata.{region}_elevation_{resolution}_{epsg}_footprint 
                WHERE "value" = 2;"""
        + f"""CREATE INDEX idfp{region}_{resolution}_{epsg}_idx 
                ON metadata.{region}_elevation_{resolution}_{epsg}_footprint_dshub 
                USING GIST (geom);"""
    )
    return query


def create_binary_file(input_raster_file: str, output_raster_file: str):
    """
    Create a binary raster (.tif) consisting of NODATA=1, DATA=2

    Parameters
    ----------
    input_raster_file : str
        Input GeoTiff raster file
    output_raster_file : str
        Output GeoTiff raster file

    Returns
    -------
    dict
        msgs dictionary for file

    Raises
    ------
    TypeError
        Input raster does not exist
    """
    if Path(input_raster_file).exists() == False:
        raise TypeError(
            f"Raster file {input_raster_file} does not exist. Check that raster exists."
        )

    msgs = dict()

    try:
        ds = gdal.Open(input_raster_file)
        nd = ds.GetRasterBand(1).GetNoDataValue()
        rows, cols = ds.RasterYSize, ds.RasterXSize
        arr = ds.GetRasterBand(1).ReadAsArray()
        #  create the output data set
        ods = ds.GetDriver().Create(output_raster_file, cols, rows, 1, gdal.GDT_Byte)
        ods.SetGeoTransform(ds.GetGeoTransform())
        ods.SetProjection(ds.GetProjection())
        #  create binary array, NODATA:1, DATA:2
        oband = ods.GetRasterBand(1)
        binary_array = np.where(arr > nd, 2, 1)
        oband.WriteArray(binary_array, 0, 0)
        # clean up
        oband.FlushCache()
        ods.FlushCache()
        del ods
        del ds
        msgs["3.NODATA"] = "SUCCESS"
    except Exception as nde:
        msgs["3.NODATA"] = f"FAIL - {nde}"

    return msgs


def polygonize_binary_file(
    input_raster_file: str, binary_raster: str, dirname: str, layername: str
):
    """
    Polygonzie binary raster as a shapefile

    Parameters
    ----------
    input_raster_file : str
        Original GeoTiff raster file
    binary_raster : str
        Binary GeoTiff raster file created via `create_binary_file()`
    dirname : str
        Directory to store shapefile
    layername : str
        Filename for shapefile

    Returns
    -------
    dict
        msgs dictionary for file

    Raises
    ------
    TypeError
        Binary raster does not exist
    """
    msgs = dict()

    if Path(binary_raster).exists() == False:
        msgs["4.POLYGONIZE"] = f"FAIL - Raster file {binary_raster} does not exist"
    else:
        try:
            ds = gdal.Open(binary_raster)
            band = ds.GetRasterBand(1)
            proj = osr.SpatialReference(ds.GetProjection()).Clone()
            # srcSRS = proj.GetAttrValue("AUTHORITY", 1)
            drv = ogr.GetDriverByName("ESRI Shapefile")
            dst_ds = drv.CreateDataSource(dirname)
            # srs is the full projedsction obj from reprojected raster
            dst_layer = dst_ds.CreateLayer(
                layername, srs=proj, geom_type=ogr.wkbPolygon
            )
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
                feature.SetField("tile_id", os.path.basename(input_raster_file))
                dst_layer.SetFeature(feature)
            dst_ds.FlushCache()
            del dst_layer
            del ds
            msgs["4.POLYGONIZE"] = "SUCCESS"
        except Exception as e:
            msgs["4.POLYGONIZE"] = f"FAIL - {e}"

    return msgs


def shp2sql(
    input_raster_file: str, shapefile: str, region: str, resolution: str, epsg: str
):
    """
    Load shapefile into Postgres, reprojecting the shapefile before load.

    Parameters
    ----------
    input_raster_file : str
        Original GeoTiff filepath
    shapefile : str
        Shapefile filepath
    region : str
        region parsed from metadata table name
    resolution : str
        resolution parsed from metadata table name
    epsg : str
        EPSG code parsed from metadata table name

    Returns
    -------
    dict
        msgs dictionary for file
    """
    msgs = dict()

    if Path(shapefile).exists() == False:
        msgs["5.SHP2PG"] = f"FAIL - Shapefile {shapefile} does not exist"
    else:
        env = {"PGPASSWORD": pwd}
        ds = gdal.Open(input_raster_file)
        proj = osr.SpatialReference(ds.GetProjection()).Clone()
        srcSRS = proj.GetAttrValue("AUTHORITY", 1)
        del ds

        shp2pgsql = f"/usr/bin/shp2pgsql -a -s {srcSRS}:{epsg} -i {shapefile} \
            metadata.{region}_elevation_{resolution}_{epsg}_footprint \
            | /usr/bin/psql -d elevation -h 10.11.11.214 -p 6432 -U elevation"

        try:
            subprocess.run(
                ["/bin/bash", "-c", shp2pgsql],
                env=env,
                check=True,
                stderr=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
            )
            msgs["5.SHP2PG"] = "SUCCESS"
        except subprocess.CalledProcessError as e:
            msgs["5.SHP2PG"] = f"FAIL - {e}"

    return msgs


@ray.remote
def nodata(input_raster: str, random_id: str, region: str, resolution: str, epsg: str):
    msgs = dict()
    msgs["1.FILE"] = input_raster
    start = datetime.datetime.now()
    msgs["2.START"] = f"{start.strftime('%Y-%m-%d %H:%M:%S')}"

    dirname = os.path.dirname(input_raster)
    fname = os.path.basename(input_raster)[:-4]
    layername = f"{fname}_{random_id}"
    output_raster = os.path.join(dirname, f"{layername}.tif")
    shapefile = os.path.join(dirname, f"{layername}.shp")

    binary_dict = create_binary_file(input_raster, output_raster)
    polygonize_dict = polygonize_binary_file(
        input_raster, output_raster, dirname, layername
    )
    shp2sql_dict = shp2sql(input_raster, shapefile, region, resolution, epsg)

    msgs.update(**binary_dict, **polygonize_dict, **shp2sql_dict)
    stop = datetime.datetime.now()
    msgs["6.STOP"] = f"{stop.strftime('%Y-%m-%d %H:%M:%S')}"
    ftime = stop - start
    msgs["7.FILETIME"] = str(ftime)

    return msgs


def main(srctable: str, random_id: str, cleanup: bool, limit: int):
    params = db_params(
        host="10.11.11.214", user="elevation", password=pwd, dbname="elevation"
    )
    conn = get_db_connection(db_params=params)
    if conn is None:
        print("[*] COULD NOT MAKE DATABASE CONNECTION")
        sys.exit("[*] EXITING")

    table_list = list_meta_tables(connection=conn)
    if srctable not in table_list:
        print(
            f"[*] COULD NOT FIND {srctable} IN METADATA TABLE LIST:\n[*] {table_list}"
        )
        sys.exit("[*] EXITING")

    ### clean up old files from previous runs
    if cleanup == True:
        q0 = f"""SELECT DISTINCT dem_path as dirpath FROM metadata.{srctable};"""
        for dir in execute_query(conn, query=q0, return_results=True):
            clean(directory=dir, substring=random_id)

    #### prep
    print("[*] PREPARING METADATA TABLES")
    region, resolution, epsg = get_params_from_meta_table(srctable)
    q1 = generate_q1(srctable, region, resolution, epsg)
    files = execute_query(conn, query=q1, return_results=True)
    if limit and limit < len(files):
        files = files[:limit]

    ### process
    print("[*] RUNNING PROCESS: nodata")
    fstart = datetime.datetime.now()
    nodata_futures = [
        nodata.remote(file, random_id, region, resolution, epsg) for file in files
    ]
    nodata_result = ray.get(nodata_futures)
    ray.shutdown()
    fstop = datetime.datetime.now()

    ### log results
    print("[*] LOGGING RESULTS")
    begin = f"START: {fstart.strftime('%Y-%m-%d %H:%M:%S')}\n"
    end = f"END: {fstop.strftime('%Y-%m-%d %H:%M:%S')}\n"
    run = f"RUNTIME: {(fstop - fstart)}\n"
    proc = f"FILES PROCESSED: {len(files)}\n"
    filetime = (fstop - fstart).total_seconds() / len(files)
    fps = f"AVERAGE FILETIME: {filetime:.2f} seconds"
    print("[*] RESULTS SUMMARY")
    print(f"[*][*] {begin}[*][*] {end}[*][*] {run}[*][*] {proc}[*][*] {fps}")

    wdir = os.path.dirname(sys.argv[0])
    nodata_log_file_name = (
        f"{fstart.strftime('%Y%m%d_%H%M%S')}_{region}_{resolution}_NODATA.log"
    )
    nodata_log_file_path = os.path.join(wdir, nodata_log_file_name)
    with open(nodata_log_file_path, mode="w") as f:
        for i in nodata_result:
            for k, v in i.items():
                f.write(k + ": " + v + "\n")
            f.write("\n")
        f.write(f"{begin}{end}{run}{proc}{fps}\n[*]\n[*]")
    print(f"[*] LOG FILE LOCATION: {nodata_log_file_path}")

    ### update metadata tables
    print("[*] UPDATING METADATA TABLES")
    q2 = generate_q2(srctable, region, resolution, epsg)
    execute_query(conn, query=q2, return_results=False)

    ### clean up intermediate files from this run
    if cleanup == True:
        q0 = f"""SELECT DISTINCT dem_path as dirpath FROM metadata.{srctable};"""
        for dir in execute_query(conn, query=q0, return_results=True):
            clean(directory=dir, substring=random_id)

    print("[*] FINISHED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Footprints",
        description="Generate NODATA footprints from raster files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-t",
        "--table",
        required=True,
        type=str,
        help="Source metadata table used to construct footprints",
    )
    parser.add_argument(
        "-r",
        "--random_id",
        type=str,
        default="NODATA_60586972",
        help="Random ID appended to intermediate file names",
    )
    parser.add_argument(
        "-c",
        "--clean",
        type=bool,
        default=True,
        help="Whether to delete intermediate files from previous run",
    )
    parser.add_argument(
        "-l",
        "--limit",
        type=int,
        default=None,
        help="Limit the number of files to process. Only use for testing",
    )
    args = parser.parse_args()

    pwd = getpass("Postgres Password:")

    main(args.table, args.random_id, args.clean, args.limit)
