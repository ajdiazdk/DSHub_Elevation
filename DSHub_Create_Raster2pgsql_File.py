# -*- coding: utf-8 -*-
"""
Created on Tue Jan 23 15:02:05 2024

@author: Adolfo.Diaz
"""

import os, traceback, sys, re, subprocess

## ===================================================================================
def errorMsg():

    """ By default, error messages will be printed and logged immediately.
        If errorOption is set to 2, return the message back to the function that it
        was called from.  This is used in DownloadElevationTile function or in functions
        that use multi-threading functionality"""

    try:

        exc_type, exc_value, exc_traceback = sys.exc_info()
        theMsg = "\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[1] + "\n\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[-1]

        return theMsg

    except:
        print("Unhandled error in unHandledException method")
        pass
    
## ===================================================================================
def createRaster2pgSQLFile(file,schema,table):

    # Mutually exclusive options
    # -a Append raster(s) to an existing table.

    # Raster processing: Optional parameters used to manipulate input raster dataset
    # -s - <SRID> Assign output raster with specified SRID.
    # -b - Index (1-based) of band to extract from raster.
    # -t - Tile Size; Cut raster into tiles to be inserted one per table row.
    # -R - Register; Register the raster as a filesystem (out-db) raster.

    # Optional parameters used to manipulate database objects
    # -F - Add a column with the name of the file (necessary for elevation but not others)
    #     This will be handy when you merge tilesets together
    # -I - Create a GiST index on the raster column.
    # -R  Register the raster as a filesystem (out-db) raster.
    #     Only the metadata of the raster and path location to the raster is
    #     stored in the database (not the pixels).

    # STDOUT parameters
    # | - A pipe is a form of redirection in Linux used to connect the STDOUT
    #     of one command into the STDIN of a second command.
    # PGPASSWORD = itsnotflat
    # -U elevation - user in the schemas
    # -d elevation - database name
    # -h 10.11.11.10 host - localhost
    # -p port

    # raster2pgsql -s 4269 -b 1 -t 507x507 -F -R -I /data03/gisdata/dsmgroup/aspct_16.tif elevation.$name | PGPASSWORD=itsnotflat psql -U elevation -d elevation -h 10.11.11.10 -p 5432

    # raster2pgsql -s 5070 -b 1 -I -t 560x560 -F -R /data03/gisdata/dsmgroup/aspct_16.tif
    # covariate.conus_aspect_16 | PGPASSWORD=itsnotflat psql -U covariate -d elevation -h 10.11.11.10 -p 5432

    # Before

    try:
        total = len(open(file).readlines())
        recCount = 0
        missingElements = 0
        
        # Ouput Raster2pgsql file
        r2pgsqlFilePath = f"{os.path.dirname(file)}{os.sep}ElevDerivative_{table}_raster2pgsql.txt"
        g = open(r2pgsqlFilePath,'a+')
        
        # Raster2pgsql parameters
        tileSize = '512x512'
        dbName = 'elevation_derivatives'
        username = 'elevation'
        password = 'itsnotflat'
        localHost = '10.11.11.214'
        port = '6432'

        """ ------------------- Open Master Elevation File and write raster2pgsql statements ---------------------"""
        with open(file, 'r') as fp:
            for line in fp:
                
                items = line.strip().split(',')
                demPath = f"{items[12]}/{items[11]}"
                srid = items[21]
                
                try:
                    #srid = line.split('_')[-2]
                    int(srid)
                except:
                    print("WARNING: SRID Could not be determined")
                    srid = '#'
                    missingElements +=1 
                
                r2pgsqlCommand = f"raster2pgsql -s {srid} -b 1 -t {tileSize} -F -a -R {demPath} {schema}.{table} | PGPASSWORD={password} psql -U {username} -d {dbName} -h {localHost} -p {port}"
                #r2pgsqlCommand = f"raster2pgsql -s {srid} -b 1 -t {tileSize} -F -a -R {line.strip()} {schema}.{table} | PGPASSWORD={password} psql -U {username} -d {dbName} -h {localHost} -p {port}"

                if recCount == total:
                    g.write(r2pgsqlCommand)
                else:
                    g.write(r2pgsqlCommand + "\n")
                    
                recCount+=1

        print(f"\t\tSuccessfully wrote raster2pgsql command -- ({recCount:,} of {total:,})")
        
        if missingElements:
            print(f"\t\tThere are {missingElements} raster2pgsql commands with missing elements")

        g.close()

        return r2pgsqlFilePath

    except:
        errorMsg()
        
## ===================================================================================
if __name__ == '__main__':

    try:
        
        # PARAM#1 -- Path to the Elevation Resolution Index Layer from the original elevation data
        file = input("\nEnter full path of File containing list of rasters: ")
        while not os.path.exists(file):
            print(f"{file} does NOT exist. Try Again")
            fileList = input("\nEnter full path of File containing list of rasters: ")
            
        # PARAM#2 -- Schema to where the data will be registered.
        schema = input("\nEnter Database Schema Name where these files will be registered: ")
        
        # PARAM#3 -- Schema Table to where the data will be registered.
        table = input("\nEnter Schema Table Name where these files will be registered: ")
        
        createRaster2pgSQLFile(file, schema, table)
            
    except:
        print(errorMsg())