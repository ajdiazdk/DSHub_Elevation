#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Adolfo.Diaz
#
# Created:     02/12/2022
# Copyright:   (c) Adolfo.Diaz 2022
# Licence:     <your licence>

# Need to figure out if a raster is already registered.  The following error gets
# printed many times if a raster is registered.
# ERROR:  current transaction is aborted, commands ignored until end of transaction block

# Need to embed multithreading capability

#-------------------------------------------------------------------------------

import subprocess, traceback, os, sys, time, re

def errorMsg(errorOption=1):
    try:

        exc_type, exc_value, exc_traceback = sys.exc_info()
        theMsg = "\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[1] + "\n\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[-1]

        print(theMsg)

    except:
        print("Unhandled error in unHandledException method")
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
##def runRaster2pgsql(textFile):
##
##    try:
##
##        msgList = []
##
##        with open(textFile, 'r') as fp:
##            for rasterLine in fp:
##
##                # The command to be executed
##                # raster2pgsql -s 4269 -b 1 -I -t 507x507 -F -R /data05/gisdata/elev/07/0708/070802/07080205/3m/ned19_n42x00_w091x75_ia_central_eastcentral_2008.zip
##                # elevation.ned19_n42x00_w091x75_ia_central_eastcentral_2008.zip | PGPASSWORD=itsnotflat psql -U elevation -d elevation -h 10.11.11.10 -p 5432
##                cmd = rasterLine.strip()
##
##                # This is one way
##                temp = subprocess.Popen(cmd, shell=True, stdout = subprocess.PIPE)
##
##                # This is a second way
##                #temp = subprocess.run(rasterLine, shell=True, check=True)
##
##                # get the output as a string
##                msg = str(temp.communicate())
##                print(msg)
##                msgList.append(msg)
##
##        return(msgList)
##
##    except:
##        errorMsg()

if __name__ == '__main__':

    try:
        start = tic()

        # raster2pgsql FILE
        raster2pgsqlFile = input("\nEnter full path to Raster2pgsql File: ")
        while not os.path.exists(raster2pgsqlFile):
            print(f"{raster2pgsqlFile} does NOT exist. Try Again")
            raster2pgsqlFile = input("Enter full path to Raster2pgsql File: ")

        #raster2pgsqlFile = r'E:\python_scripts\DSHub\LinuxWorkflow\raster2pgsql.txt'
        raster2pgsqlLogFileName = os.path.basename(raster2pgsqlFile).split('.')[0] + "_LOG.txt"
        raster2pgsqlLogFile = f"{os.path.dirname(raster2pgsqlFile)}{os.sep}{raster2pgsqlLogFileName}"
        raster2pgsqlRerunName = os.path.basename(raster2pgsqlFile).split('.')[0] + "_RERUN.txt"
        raster2pgsqlRerun = f"{os.path.dirname(raster2pgsqlFile)}{os.sep}{raster2pgsqlRerunName}"

        total = sum(1 for line in open(raster2pgsqlFile))
        iCount = 1
        label = "Running Raster2pgsql"

        msgList = []
        errorList = ['Error','error','ERROR','Fail','FAILED','FAIL','fail','UNCOMMIT']
        words_re = re.compile("|".join(errorList))

        # List
        unregisteredDEMs = list()

        print(f"About to run RASTER2PGSQL on {total} DEM files")

        with open(raster2pgsqlFile, 'r') as fp:
            for rasterLine in fp:

                # The command to be executed
                # raster2pgsql -s 4269 -b 1 -I -t 507x507 -F -R /data05/gisdata/elev/07/0708/070802/07080205/3m/ned19_n42x00_w091x75_ia_central_eastcentral_2008.zip
                # elevation.ned19_n42x00_w091x75_ia_central_eastcentral_2008.zip | PGPASSWORD=itsnotflat psql -U elevation -d elevation -h 10.11.11.10 -p 5432
                cmd = rasterLine.strip()

                # Line inludes a '#'
                if cmd.find('#'):
                    unregisteredDEMs.append(cmd)
                    continue

                try:

                    temp = subprocess.Popen(cmd, shell=True, stdout = subprocess.PIPE)
                    msg = str(temp.communicate())

                    if words_re.search(msg):

                        g = open(raster2pgsqlLogFile,'a+')
                        g.write(f"\n{'='*25}")
                        g.write(f"\n{cmd}")
                        g.write(msg)
                        g.close()

                        h = open(raster2pgsqlRerun, 'a+')
                        h.write(f"{cmd}\n")

                    print_progress_bar(iCount, total, label)
                    iCount+=1
                    continue

                except:
                    print("inside except")
                    g = open(raster2pgsqlLogFile,'a+')
                    g.write(f"\n{'='*25}")
                    g.write(f"\n{cmd}")
                    try:
                        g.write(str(temp.communicate()))
                    except:
                        g.write(f"SOMETHING FAILED HERE -- NEED TO CAPTURE ERROR")
                    g.close()

                    h = open(raster2pgsqlRerun, 'a+')
                    h.write(cmd + "\n")
                    errorMsg()
                    continue

        if len(unregisteredDEMs):
            print(f"\nThere were {len(unregisteredDEMs)}")

        end = toc(start)
        print("FINISHED")
        print(f"\nTotal raster2pgsql Time: {end}\n")

    except:
        errorMsg()