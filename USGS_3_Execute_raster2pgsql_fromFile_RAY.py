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

7/7/2023
    - Updated raster2pgsql function to distinguish messages returned between errors and
      warnings.  Added a 'Warning' category.

      Encountered the following errors with Alaska:
      'warning: this file used to have optimizations in its layout, but thoseartly, invalidated
       by later changes'
       
9/28/2023
    -Made a copy of this script to switch from multithreading to ray.  Following changes were made:
        1) created a main function

Things to consider/do:
    - Warning messages are assumed to be duplicated.  If there are multiple warnings I should compare
      them to see if they are unique.
    - Try to get a list of raster2pgsql errors to handle messages better.

"""

#-------------------------------------------------------------------------------

import subprocess, traceback, os, sys, time, re
from datetime import datetime
import ray

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
@ray.remote
def runRaster2pgsql(cmd):
    """ This function was desinged to execute raster2pgsql commands via a subprocess to OS
        
    """

    try:
        cmdItems = cmd.split(' ')
        DEMname = os.path.basename(cmdItems[11 if os.name == 'nt' else 10])

        msgDict = dict()
        errorList = ['error','failed','fail','uncommit','aborted','notice','memory',
                     'warning','unable','not recognized','inoperable','syntax', 'cannot']
        words_re = re.compile("|".join(errorList))
        
        # Windows (nt) vs Linux (posix)
        if os.name == 'nt':
            env={'PGPASSWORD':"ilovesql",
                 'PGHOST' : 'localhost',
                 'PGPORT' : '5432',
                 'PGUSER' : 'postgres',
                 'PGDATABASE' : "elevation",
                 'SYSTEMROOT': os.environ['SYSTEMROOT'],
                 'PROJ_LIB': r"C:\Program Files\PostgreSQL\15\share\contrib\postgis-3.3\proj"}
            
            # Send command to operating system
            execCmd = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True, env=env)
        
        else:
            # Send command to operating system
            execCmd = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
            #execCmd = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
            #execCmd = subprocess.run(rasterLine, shell=True, check=True) # alternate method

        # returns a tuple (stdout_data, stderr_data)
        msgs, errors = execCmd.communicate()
        errors = ','.join(errors.strip().split('\n')).lower()  # some errors return mutliple carriage returns

        # Collect messages
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

                msgDict['Warning'] = f"Loaded {DEMname} but with the following Warning: {warningMessage}\n"

            # Error messages; What is the error group here? Investigate after more errors arise
            else:
                msgDict['Error'] = f"{cmd}: ERROR: {errors}\n"

        else:
            msgDict['Success'] = f"Successfully loaded {DEMname}"

        return msgDict

    except:
        msgDict['Error'] = f"\"{cmd}\"\n\t{errorMsg(errorOption=2)}"
        return msgDict
    
## ===================================================================================
def main(raster2pgsqlFile):
    
    try:
        global raster2pgsqlLogFile
        
        start = tic()
        resolution = raster2pgsqlFile.split(os.sep)[-1].split('_')[2]

        """ ---------------------------- Establish Console LOG FILE ---------------------------------------------------"""
        raster2pgsqlLogFile = f"{os.path.dirname(raster2pgsqlFile)}{os.sep}USGS_3DEP_{resolution}_Step3_Exec_RASTER2PGSQL_ConsoleMsgs.txt"
        raster2pgsqlErrorFile = f"{os.path.dirname(raster2pgsqlFile)}{os.sep}USGS_3DEP_{resolution}_Step3_Exec_RASTER2PGSQL_FAILED.txt"

        recCount = sum(1 for line in open(raster2pgsqlFile))
        
        h = open(raster2pgsqlLogFile,'a+')
        today = datetime.today().strftime('%m%d%Y')
        h.write(f"Executing: USGS_3_Execute_raster2pgsql_fromFile.py {today}\n")
        h.write("\nUser Selected Parameters:")
        h.write(f"\tRaster2pgsql File: {raster2pgsqlFile})")
        h.write(f"\tNumber of Raster2pgsql records: {recCount:,}")
        h.write(f"\n{'='*125}")
        h.close()

        iCount = 1
        raster2pgsqlList = list() # List of commnands to run in multi-thread
        invalidCommands = list() # List of invalid commands or that contain an unknown parameter ('#')

        """ ---------------------------- Inspect and report raster2pgsql statements ---------------------------------------------------"""
        # Look for '#' in raster2pgsql commands; Likely added from script#2
        AddMsgAndPrint("Checking Raster2pgsql Statements")
        with open(raster2pgsqlFile, 'r') as fp:
            for rasterLine in fp:

                # The command to be executed
                # raster2pgsql -s 4269 -b 1 -I -t 507x507 -F -R /data05/gisdata/elev/07/0708/070802/07080205/3m/ned19_n42x00_w091x75_ia_central_eastcentral_2008.zip
                # elevation.ned19_n42x00_w091x75_ia_central_eastcentral_2008.zip | PGPASSWORD=itsnotflat psql -U elevation -d elevation -h 10.11.11.10 -p 5432
                cmd = rasterLine.strip()
                cmdItems = cmd.split(' ')

                # windows = "C:\Program Files\PostgreSQL\15\bin\raster2pgsql.exe"
                # linux = raster2pgsl
                if cmd.find('#') > -1 or not cmdItems[1 if os.name == 'nt' else 0].find('raster2pgsql') > -1:
                    AddMsgAndPrint(f"\tRecord #{iCount:,} is an invalid raster2pgsql command or contains invalid parameters. Skipping")
                    #AddMsgAndPrint(f"\tLine #{iCount:,} contains an invalid 'raster2pgsql' parameter. Skipping")
                    invalidCommands.append(cmd)
                    iCount+=1
                    continue

                iCount+=1
                raster2pgsqlList.append(cmd)

        # Report invalid commands
        numOfInvalidCommands = len(invalidCommands)
        if numOfInvalidCommands:
            AddMsgAndPrint(f"\tThere are {numOfInvalidCommands:,} invalid raster2pgsql command or that contain invalid parameters:")
            logError = open(raster2pgsqlErrorFile,'a+')
            for invalidCmd in invalidCommands:
                logError.write(f"\n{invalidCmd}")
            logError.close
            del logError
            
        """ ---------------------------- Execute raster2pgsql statements in MT mode ---------------------------------------------------"""
        # Run commands in multi-threading mode
        if len(raster2pgsqlList):
            AddMsgAndPrint(f"\nRunning raster2pgsql on {len(raster2pgsqlList):,} DEM files")
            r2pTracker = 0
            successCount = 0
            warningCount = 0
            failedCount = 0

            runR2pgsqlCmd = [runRaster2pgsql.remote(cmd) for cmd in raster2pgsqlList]
            msgResults = ray.get(runR2pgsqlCmd)
            
            # yield future objects as they are done.
            for msgs in msgResults:

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

                    elif status == 'Warning':
                        AddMsgAndPrint(f"\t{msgs['Warning']} -- ({r2pTracker:,} of {recCount:,})")
                        warningCount+=1

                    elif status == 'Success':
                        AddMsgAndPrint(f"\t{msgs['Success']} -- ({r2pTracker:,} of {recCount:,})")
                        successCount+=1
                    else:
                        AddMsgAndPrint("\tStatus message not returned by raster2pgsql function")
            
            # with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:

            #     # use a set comprehension to start all tasks.  This creates a future object
            #     runR2pgsqlCmd = {executor.submit(runRaster2pgsql, cmd): cmd for cmd in raster2pgsqlList}

            #     # yield future objects as they are done.
            #     for future in as_completed(runR2pgsqlCmd):
            #         msgs = future.result()

            #         for status in msgs:
            #             r2pTracker+=1

            #             if status == 'Error':
            #                 AddMsgAndPrint(f"\n\tFailed: {msgs['Error']} -- ({r2pTracker:,} of {recCount:,})")
            #                 failedCount+=1

            #                 isolatedCommand = msgs['Error'].split('\n')[0]
            #                 logError = open(raster2pgsqlErrorFile,'a+')
            #                 logError.write(f"\n{isolatedCommand}")
            #                 logError.close
            #                 del logError

            #             elif status == 'Warning':
            #                 AddMsgAndPrint(f"\t{msgs['Warning']} -- ({r2pTracker:,} of {recCount:,})")
            #                 warningCount+=1

            #             elif status == 'Success':
            #                 AddMsgAndPrint(f"\t{msgs['Success']} -- ({r2pTracker:,} of {recCount:,})")
            #                 successCount+=1
            #             else:
            #                 AddMsgAndPrint("\tStatus message not returned by raster2pgsql function")

        else:
            AddMsgAndPrint("\nThere were no valid 'RASTER2PGSQL' records to execute.  EXITING!")
            sys.exit()

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

        else:
            if successCount > 0:
                AddMsgAndPrint(f"\nThere were {successCount:,} of {len(raster2pgsqlList):,} DEMs were successfully loaded.")

            if warningCount > 0:
                AddMsgAndPrint(f"\nThere were {warningCount:,} of {len(raster2pgsqlList):,} DEMS that loaded with warnings.")

            if failedCount > 0:
                AddMsgAndPrint(f"\nThere were {failedCount:,} of {len(raster2pgsqlList):,} DEMS that failed loading process.")

    except:
        errorMsg()

## ===================================================================================
if __name__ == '__main__':


    # raster2pgsql FILE
    raster2pgsqlFile = input("\nEnter full path of text file containing RASTER2PGSQL statements: ")
    while not os.path.exists(raster2pgsqlFile):
        print(f"{raster2pgsqlFile} does NOT exist. Try Again")
        raster2pgsqlFile = input("Enter full path to Raster2pgsql File: ")
        
    main(raster2pgsqlFile)
