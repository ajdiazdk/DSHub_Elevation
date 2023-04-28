#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Adolfo.Diaz
#
# Created:     13/12/2022
# Copyright:   (c) Adolfo.Diaz 2022
# Licence:     <your licence>
#-------------------------------------------------------------------------------

# Import modules
import sys, string, os, traceback, time, glob
from datetime import datetime

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

## ===================================================================================
def AddMsgAndPrint(msg):

    # Print message to python message console
    print(msg)

    # Add message to log file
    try:
        h = open(deleteLogFile,'a+')
        h.write("\n" + msg)
        h.close
        del h
    except:
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
if __name__ == '__main__':

    try:
        start = tic()

        # Master Elevation file used to delete DEMS
        masterElevFile = input("\nEnter full path to the Original Metadata_Elevation Text File: ")
        while not os.path.exists(masterElevFile):
            print(f"{masterElevFile} does NOT exist. Try Again")
            masterElevFile = input("\nEnter full path to the Original Metadata_Elevation Text File: ")

        #masterElevFile = r'D:\python_scripts\DSHub\LinuxWorkflow\TEMP_Update_DL_StatusFile\TEMP_testingFiles\USGS_3DEP_3M_Metadata_Elevation_11192022_TEST_MASTER_DB.txt'

        deleteLogFileName = os.path.basename(masterElevFile).split('.')[0] + "_DELETE_LOG.txt"
        deleteLogFile = f"{os.path.dirname(masterElevFile)}{os.sep}{deleteLogFileName}"

        recCount = sum(1 for line in open(masterElevFile)) -1
        today = datetime.today().strftime('%m%d%Y')

        AddMsgAndPrint(f"Executing: USGS_2B_DeleteDems_using_MasterElevationFile.py {today}")
        AddMsgAndPrint(f"Master Elevation File: {masterElevFile})")
        AddMsgAndPrint(f"Number of records to delete: {recCount:,}")
        AddMsgAndPrint(f"\n{'='*125}")

        dlFileHeaderItems = {
            "huc_digit":0,
            "prod_title":1,
            "pub_date":2,
            "last_updated":3,
            "size":4,
            "format":5,
            "sourceID":6,
            "metadata_url":7,
            "download_url":8,
            "DEMname":9,
            "DEMpath":10}

        bHeader = True
        downloadDict = dict()  # contains download URLs and sourceIDs grouped by HUC; 07040006:[[ur1],[url2]]
        dlFile_recCount = 0
        deletedFile = 0
        invalidFiles = 0

        """ ---------------------------- Open Download File and Parse Information ----------------------------------"""
        with open(masterElevFile, 'r') as fp:
            for line in fp:
                items = line.split(',')

                # Skip header line and empty lines
                if bHeader and dlFile_recCount == 0 or line == "\n":
                    dlFile_recCount+=1
                    continue

                sourceID = items[dlFileHeaderItems["sourceID"]]
                DEMname = items[dlFileHeaderItems["DEMname"]]
                DEMpath = items[dlFileHeaderItems["DEMpath"]]

                # Delete all files associated with the DEMname (.xml, aux...etc)
                for file in glob.glob(f"{DEMpath}os.sep{DEMname.split('.')[0]}*"):
                    if os.path.isfile(file):
                        os.remove(file)
                        AddMsgAndPrint(f"Successfully Deleted -- {sourceID} -- {os.path.basename(file)}")
                        deletedFile+=1
                    else:
                        AddMsgAndPrint(f"NOT a valid file -- {sourceID} -- {fullPath}")
                        invalidFiles+=1

##                fullPath = os.path.join(DEMpath,DEMname)
##                if os.path.isfile(fullPath):
##                    os.remove(fullPath)
##                    AddMsgAndPrint(f"Successfully Deleted -- {sourceID} -- {fullPath}")
##                    deletedFile+=1
##                else:
##                    AddMsgAndPrint(f"NOT a valid file -- {sourceID} -- {fullPath}")
##                    invalidFiles+=1

                dlFile_recCount+=1


        """ ------------------------------------ SUMMARY -------------------------------------- """
        end = toc(start)
        AddMsgAndPrint(f"\n{'-'*40}SUMMARY{'-'*40}")
        AddMsgAndPrint(f"\nNumber of files deleted: {deletedFile:,}")
        AddMsgAndPrint(f"\nNumber of invalid files: {invalidFiles:,}")

    except:
        errorMsg()
