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

import subprocess, traceback

def errorMsg(errorOption=1):
    try:

        exc_type, exc_value, exc_traceback = sys.exc_info()
        theMsg = "\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[1] + "\n\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[-1]

        return theMsg

    except:
        print("Unhandled error in unHandledException method")
        pass

def runRaster2pgsql(textFile):

    try:

        msgList = []

        with open(textFile, 'r') as fp:
            for rasterLine in fp:

                # The command to be executed
                # raster2pgsql -s 4269 -b 1 -I -t 507x507 -F -R /data05/gisdata/elev/07/0708/070802/07080205/3m/ned19_n42x00_w091x75_ia_central_eastcentral_2008.zip
                # elevation.ned19_n42x00_w091x75_ia_central_eastcentral_2008.zip | PGPASSWORD=itsnotflat psql -U elevation -d elevation -h 10.11.11.10 -p 5432
                cmd = rasterLine.strip()

                # This is one way
                temp = subprocess.Popen(cmd, shell=True, stdout = subprocess.PIPE)

                # This is a second way
                #temp = subprocess.run(rasterLine, shell=True, check=True)

                # get the output as a string
                msg = str(temp.communicate())
                print(msg)
                msgList.append(msg)

        return(msgList)

    except:
        errorMsg()

if __name__ == '__main__':

    #raster2pgsqlFile = r'/data05/gisdata/elev/scripts/raster2pgsql.txt'
    raster2pgsqlFile = r'E:\python_scripts\DSHub\LinuxWorkflow\raster2pgsql.txt'
    runRaster2pgsql(raster2pgsqlFile)
