# -*- coding: utf-8 -*-
"""
Created on 1/26/2024

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
def runRaster2pgsql(input_file,schema,table):
    """ This function will create and execute raster2pgsql commands via a subprocess to OS
    
    Parameter 1 - absolute file path of a raster file i.e.
                  r'/data02/gisdata/elev/10m/0/USGS_13_n27w100_20201228_5070_slope.tif'
    Parameter 2 - String name of Database Schema Name; i.e. slope
    Paramter 3 -  String name of Database Schema Table name; i.e. conus_slope_10m_5070
    
    returns a list of messages produced; Ideally only SUCCESS messages are created.
    """

    try:
        messageList = list()
        rasterFileName = os.path.basename(input_file)
        
        # Hard coded Raster2pgsql parameters
        tileSize = '512x512'
        dbName = 'elevation_derivatives'
        username = 'elevation'
        password = 'itsnotflat'
        localHost = '10.11.11.214'
        port = '6432'
        
        try:
            srid = rasterFileName.split('_')[-2]
            int(srid)
        except:
            messageList.append(f"\nWARNING: SRID Could not be determined for {rasterFileName}")
            srid = '#'
            
        r2pgsqlCommand = f"raster2pgsql -s {srid} -b 1 -t {tileSize} -F -a -R {input_file.strip()} {schema}.{table} | PGPASSWORD={password} psql -U {username} -d {dbName} -h {localHost} -p {port}"

        # Send command to operating system
        execCmd = subprocess.Popen(r2pgsqlCommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)

        """ ------------------------ Assess Messages,Warnings,Errors --------------------------"""
        # returns a tuple (stdout_data, stderr_data)
        msgs, errors = execCmd.communicate()
        errors = ','.join(errors.strip().split('\n')).lower()  # some errors return mutliple carriage returns
        
        errorList = ['error','failed','fail','uncommit','aborted','notice','memory',
                     'warning','unable','not recognized','inoperable','syntax', 'cannot']
        words_re = re.compile("|".join(errorList))

        # Collect messages from subprocess
        # 0 = completed, however there may be warnings
        errorResults = words_re.search(errors)

        if errorResults:
            errorGroup = errorResults.group()

            # Warning messages
            if errorGroup in ('warning') and execCmd.returncode == 0:

                # number of warnings; isolate 1 message
                # If there are multiple warnings I should compare them to see if they are unique;
                # Assumption is that the warnings are duplicated
                warningCounts = re.findall(r'\b(?:' + '|'.join(['warning']) + r')\b', errors)
                warningIndices = [m.start() for m in re.finditer(r"\bwarning\b", errors)]

                # Extract warning message
                if len(warningCounts) > 1:
                    warningMessage = errors[warningIndices[0]:warningIndices[1]]
                else:
                    warningMessage = errors[warningIndices[0]:]

                messageList.append(f"\nWARNING -- {rasterFileName} -- {warningMessage}")

            # Error messages; What is the error group here? Investigate after more errors arise
            else:
                messageList.append(f"\nERROR -- {rasterFileName} -- {errors}")

        else:
            messageList.append(f"\nSUCCESS -- {rasterFileName}")

        return messageList

    except:
        messageList.append(f"\nERROR -- {rasterFileName} -- Error encountered running raster2pgsql")
        return messageList

## ===================================================================================
if __name__ == '__main__':

    try:
        
        testList = [r'/data02/gisdata/elev/10m/0/USGS_13_n27w100_20201228_5070_slope.tif',
                    r'/data02/gisdata/elev/10m/0/USGS_13_n28w100_20201228_5070_slope.tif',
                    r'/data02/gisdata/elev/10m/0/USGS_13_n29w100_20201228_5070_slope.tif']
        
        for file in testList:
            msgList = runRaster2pgsql(file,'slope','slope_junk')
            
            for msg in msgList:
                print(f"\n{msg}")
            
    except:
        print(errorMsg())