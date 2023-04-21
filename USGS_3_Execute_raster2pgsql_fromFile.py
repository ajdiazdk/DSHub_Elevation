# -*- coding: utf-8 -*-
"""
Script Name: USGS_3_Execute_raster2pgsql_fromFile.py
Created on Thu Jan 26 2023
updated 2/6/2023

@author: Adolfo.Diaz
GIS Business Analyst
USDA - NRCS - SPSD - Soil Services and Information
email address: adolfo.diaz@usda.gov
cell: 608.215.7291

This is script #3 in the USGS Elevation acquisition workflow developed for the DS Hub.
# Need to figure out if a raster is already registered.  The following error gets
# printed many times if a raster is registered.
# ERROR:  current transaction is aborted, commands ignored until end of transaction block

---------------- UPDATES
2/24/2023
    -update to print and log invalid raster2pgsql commands to a seperate log file so
     it is easy to reference, fix and rerun seperate raster2pgsql commands.

4/15/2023
    - Updated the names of the output files to be more intuitive.
        - USGS_3DEP_1M_Metadata_Elevation_02242023_RASTER2PGSQL_ConsoleMsgs.txt -- > USGS_3DEP_1M_Step3_Exec_RASTER2PGSQL_ConsoleMsgs.txt
        - USGS_3DEP_1M_Metadata_Elevation_02242023_RASTER2PGSQL_FAILED.txt --> USGS_3DEP_1M_Step3_Exec_RASTER2PGSQL_FAILED.txt

"""

#-------------------------------------------------------------------------------

import subprocess, traceback, os, sys, time, re
from datetime import datetime
import threading, multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

## ===================================================================================
def AddMsgAndPrint(msg):

    # Print message to python message console
    print(msg)

    # Add message to log file
    try:
        h = open(raster2pgsqlLogFile,'a+')
        h.write("\n" + msg)
        h.close
        del h
    except:
        pass

## ===================================================================================
def errorMsg(errorOption=1):
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
def toc(_start_time):
    """ Returns the total time by subtracting the start time - finish time"""

    try:

        t_sec = round(time.time() - _start_time)
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

## ===================================================================================
def print_progress_bar(index, total, label):

    "prints generic percent bar to indicate progress. Cheesy but works."

    n_bar = 50  # Progress bar width
    progress = index / total
    sys.stdout.write('\r')
    sys.stdout.write(f"\t[{'=' * int(n_bar * progress):{n_bar}s}] {int(100 * progress)}%  {label}")
    sys.stdout.flush()

## ===================================================================================
def runRaster2pgsql(cmd):
# Incorporate check to see if raster exists
# Execute a child program in a new process
# safepoints
# commit

    try:
        cmdItems = cmd.split(' ')
        DEMname = os.path.basename(cmdItems[10])

        msgDict = dict()
        errorList = ['error','failed','fail','uncommit','aborted','notice',
                     'warning','unable','not recognized','inoperable']
        words_re = re.compile("|".join(errorList))

        # Send command to operating system
        execCmd = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
        #execCmd = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
        #execCmd = subprocess.run(rasterLine, shell=True, check=True) # alternate method

        # returns a tuple (stdout_data, stderr_data)
        msgs, errors = execCmd.communicate()
        errors = ','.join(errors.strip().split('\n'))  # some errors return mutliple carriage returns

        # Collect messages
        if words_re.search(errors.lower()) or not execCmd.returncode == 0:
            msgDict['Error'] = f"{cmd}\n\t{errors}"
        else:
            msgDict['Success'] = f"Successfully loaded {DEMname}"

        return msgDict

    except:
        msgDict['Error'] = f"{cmd}\n\t{errorMsg(errorOption=2)}"
        return msgDict

## ===================================================================================
if __name__ == '__main__':

    try:
        start = tic()

        # raster2pgsql FILE
        raster2pgsqlFile = input("\nEnter full path of text file containing RASTER2PGSQL statements: ")
        while not os.path.exists(raster2pgsqlFile):
            print(f"{raster2pgsqlFile} does NOT exist. Try Again")
            raster2pgsqlFile = input("Enter full path to Raster2pgsql File: ")

        resolution = raster2pgsqlFile.split(os.sep)[-1].split('_')[2]
        raster2pgsqlLogFile = f"{os.path.dirname(raster2pgsqlFile)}{os.sep}USGS_3DEP_{resolution}_Step3_Exec_RASTER2PGSQL_ConsoleMsgs.txt"
        raster2pgsqlErrorFile = f"{os.path.dirname(raster2pgsqlFile)}{os.sep}USGS_3DEP_{resolution}_Step3_Exec_RASTER2PGSQL_FAILED.txt"

        recCount = sum(1 for line in open(raster2pgsqlFile))
        today = datetime.today().strftime('%m%d%Y')

        AddMsgAndPrint(f"Executing: USGS_3_Execute_raster2pgsql_fromFile.py {today}\n")
        AddMsgAndPrint(f"Raster2pgsql File: {raster2pgsqlFile})")
        AddMsgAndPrint(f"Number of Raster2pgsql records: {recCount:,}")
        AddMsgAndPrint(f"\n{'='*125}")

        iCount = 1
        raster2pgsqlList = list() # List of commnands to run in multi-thread
        invalidCommands = list() # List of invalid commands or that contain an unknown parameter ('#')

        AddMsgAndPrint(f"Checking Raster2pgsql Statements")
        with open(raster2pgsqlFile, 'r') as fp:
            for rasterLine in fp:

                # The command to be executed
                # raster2pgsql -s 4269 -b 1 -I -t 507x507 -F -R /data05/gisdata/elev/07/0708/070802/07080205/3m/ned19_n42x00_w091x75_ia_central_eastcentral_2008.zip
                # elevation.ned19_n42x00_w091x75_ia_central_eastcentral_2008.zip | PGPASSWORD=itsnotflat psql -U elevation -d elevation -h 10.11.11.10 -p 5432
                cmd = rasterLine.strip()
                cmdItems = cmd.split(' ')

                # Line inludes a '#'
                if cmd.find('#') > -1 or not cmdItems[0] == 'raster2pgsql':
                    AddMsgAndPrint(f"\tRecord #{iCount:,} is an invalid raster2pgsql command or contains invalid parameters. Skipping")
                    #AddMsgAndPrint(f"\tLine #{iCount:,} contains an invalid 'raster2pgsql' parameter. Skipping")
                    invalidCommands.append(cmd)
                    iCount+=1
                    continue

                iCount+=1
                raster2pgsqlList.append(cmd)

        numOfInvalidCommands = len(invalidCommands)
        if numOfInvalidCommands:
            AddMsgAndPrint(f"\tThere are {numOfInvalidCommands:,} invalid raster2pgsql command or that contain invalid parameters:")
            logError = open(raster2pgsqlErrorFile,'a+')
            for invalidCmd in invalidCommands:
                logError.write(f"\n{invalidCmd}")
            logError.close
            del logError

        # Run commands in multi-threading mode
        if len(raster2pgsqlList):
            AddMsgAndPrint(f"\nRunning raster2pgsql on {len(raster2pgsqlList):,} DEM files")
            r2pTracker = 0
            successCount = 0
            failedCount = 0

            with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:

                # use a set comprehension to start all tasks.  This creates a future object
                runR2pgsqlCmd = {executor.submit(runRaster2pgsql, cmd): cmd for cmd in raster2pgsqlList}

                # yield future objects as they are done.
                for future in as_completed(runR2pgsqlCmd):
                    msgs = future.result()
                    for status in msgs:
                        r2pTracker+=1
                        if status == 'Error':
                            AddMsgAndPrint(f"\n\tFailed: {msgs['Error']} -- ({r2pTracker:,} of {recCount:,})")
                            failedCount+=1

                            isolatedCommand = msgs['Error'].split('\n')[0]
                            logError = open(raster2pgsqlErrorFile,'a+')
                            logError.write(f"\n{isolatedCommand}")
                            logError.close
                            del logError

                        elif status == 'Success':
                            AddMsgAndPrint(f"\t{msgs['Success']} -- ({r2pTracker:,} of {recCount:,})")
                            successCount+=1
                        else:
                            AddMsgAndPrint("\tStatus message not returned by raster2pgsql function")

        else:
            AddMsgAndPrint(f"\nThere were no valid 'RASTER2PGSQL' records to execute.  EXITING!")
            exit()

        """ ------------------------------------ SUMMARY -------------------------------------- """
        end = toc(start)
        AddMsgAndPrint(f"\n{'-'*40}SUMMARY{'-'*40}")
        AddMsgAndPrint(f"Total raster2pgsql DEM Loading Time: {end}")

        # Invalid RASTER2PGSQL command
        if len(invalidCommands):
            AddMsgAndPrint(f"\nThere were {len(invalidCommands):,} of {recCount:,} records that were invalid")

        # All rasters loaded successfully
        if successCount == recCount:
            AddMsgAndPrint(f"\nALL {recCount:,} DEMs were successfully loaded to the database")

        if failedCount > 0:
            AddMsgAndPrint(f"\nThere were {failedCount:,} of {len(raster2pgsqlList):,} DEMS that failed loading process")

    except:
        errorMsg()