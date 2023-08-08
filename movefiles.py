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
import sys, string, os, traceback, time, glob, shutil
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
def getDownloadFolder2(filename):
    """ This function returns a directory that was created in LINUX.  Directory
        structure was determined by parsing the filename for each unique USGS
        Dataset product.  The following describes the product and dir determination
         - 1M: isolate lat long from name (xy) and take the last digit
               'USGS_one_meter_x41y492_ME_NRCS_Lot4_2013.tif' --> x41y492 --> 2
         - 3M: isolate upper left xy and take the first digit
               'ned19_n27x25_w082x25_fl_wcnt5co04_eastsarasota_2007.zip' --> n27x25 --> 2
         - 10M: isolate 1-x1 degree block and take the last digit
               'USGS_13_n58w171_20200415.tif' --> n58w171 --> 5
         - 30M: isolate 1-x1 degree block and take the last digit
               'USGS_1_n07e158_20130911.tif' --> n07e158 --> 0
         - Other: take the last digit
    """

    try:
        # ['USGS', '1M', '19', 'x44y515', 'ME', 'CrownofMaine', '2018', 'A18.tif']
        fileElements = filename.split('_')

        # Remove USGS from the name
        if fileElements[0] == 'USGS':
            del fileElements[0]

        # Lists of unique naming patterns by dataset product
        oneM = ['1M', 'one','ONE','One', '1m']
        threeM = ['ned19','imgned19']
        tenM = ['13']
        thirtyM = ['1']
        ak_opr = ['opr','OPR']

        firstElement = fileElements[0]

        # ------------------------ File is a 1M USGS File
        if firstElement in oneM:
            i=0
            block = ''
            ld = ''
            for element in fileElements:

                # x41y492
                if element.startswith('x'):
                    block = fileElements[i]
                    break
                i+=1

            if block[-1].isdigit():
                ld = block[-1]

            res = '1m'

        # ------------------------ File is a 3M USGS File
        # The EBS distribution of 3M files is very uneven.
        # Need to develop a better way to distribute files
        elif firstElement in threeM:
            i=0
            block = ''
            ld = ''

            if fileElements[0] in threeM:
                del fileElements[0]

            for element in fileElements:
                # n39x00
                if element.startswith('n'):
                    block = fileElements[i]
                    break
                i+=1

            for digit in block:
                if digit.isdigit():
                    ld = digit
                    break

            res = '3m'

        # ------------------------ File is a 10M USGS File
        elif firstElement in tenM:
            i=0
            block = ''
            ld = ''

            if fileElements[0] in tenM:
                del fileElements[0]

            for element in fileElements:

                # n61w166
                if element.startswith('n') or element.startswith('s'):
                    block = fileElements[i]
                    break
                i+=1

            for digit in reversed(block):
                if digit.isdigit():
                    ld = digit
                    break

            res = '10m'

        # ------------------------ File is a 30M USGS File
        elif firstElement in thirtyM:
            i=0
            block = ''
            ld = ''

            if fileElements[0] in thirtyM:
                del fileElements[0]

            for element in fileElements:

                # n61w166 or s14w170
                if element.startswith('n') or element.startswith('s'):
                    block = fileElements[i]
                    break
                i+=1

            for digit in reversed(block):
                if digit.isdigit():
                    ld = digit
                    break

            res = '30m'

        elif firstElement in ak_opr:
            pass

        # ------------------------ oddball files that don't follow the majority convention
        # i.e. n65w158.zip
        else:
            ld = None

            for char in reversed(filename):
                if char.isdigit():
                    ld = char
                    break

            # assign arbitrary drive
            if ld is None:
                ld = 5
            res = resolution.lower()

        # last digit
        if ld in ["1","9","0"]:
            if res in ('1m','30m'):
                drive = '06'
            else:
                drive = '02'

        elif ld in ["2","7","8"]:
            if res in ('1m','30m'):
                drive = '07'
            else:
                drive = '03'

        elif ld in ["3","6"]:
            if res in ('1m','30m'):
                drive = '08'
            else:
                drive = '04'

        elif ld in ["4","5"]:
            if res in ('1m','30m'):
                drive = '09'
            else:
                drive = '05'

        else:
            AddMsgAndPrint("\t\t\tLook into download directory for {filename}")
            drive = '09'

        root = f"{os.sep}data{drive}{os.sep}gisdata{os.sep}elev"
        downloadFolder = f"{root}{os.sep}{res}{os.sep}{ld}"

        # In case another multi-thread is creating the same directory
        try:
            if not os.path.exists(downloadFolder):
                os.makedirs(downloadFolder)
        except:
            pass

        return downloadFolder

    except:
        errorMsg()
        AddMsgAndPrint("\t\t\tFailed to determine download directory for {filename}")
        return False

## ===================================================================================
if __name__ == '__main__':

    try:
        start = tic()

        # Master Elevation file used to delete DEMS
##        masterElevFile = input("\nEnter full path to the Original Metadata_Elevation Text File: ")
##        while not os.path.exists(masterElevFile):
##            print(f"{masterElevFile} does NOT exist. Try Again")
##            masterElevFile = input("\nEnter full path to the Original Metadata_Elevation Text File: ")

        masterElevFile = r'F:\DSHub\Elevation\USGS_Elevation\USGS_3DEP_1M_Step2_Elevation_Metadata.txt'

        deleteLogFileName = os.path.basename(masterElevFile).split('.')[0] + "_MoveFiles_LOG.txt"
        deleteLogFile = f"{os.path.dirname(masterElevFile)}{os.sep}{deleteLogFileName}"

        recCount = sum(1 for line in open(masterElevFile)) -1
        today = datetime.today().strftime('%m%d%Y')

        AddMsgAndPrint(f"Executing: MoveFiles.py {today}")
        AddMsgAndPrint(f"Master Elevation File: {masterElevFile})")
        AddMsgAndPrint(f"Number of records to move: {recCount:,}")
        AddMsgAndPrint(f"\n{'='*125}")

        dlFileHeaderItems = {
            "huc_digit":0,
            "prod_title":1,
            "pub_date":2,
            "last_updated":3,
            "size":4,
            "format":5,
            "sourceid":6,
            "metadata_url":7,
            "download_url":8,
            "dem_name":9,
            "dem_path":10}

        bHeader = True
        downloadDict = dict()  # contains download URLs and sourceIDs grouped by HUC; 07040006:[[ur1],[url2]]
        movedFile = 0
        invalidFiles = 0

        """ ---------------------------- Open Download File and Parse Information ----------------------------------"""
        with open(masterElevFile, 'r') as fp:
            for line in fp:
                items = line.split(',')

                # Skip header line and empty lines
                if bHeader and movedFile == 0 or line == "\n":
                    movedFile+=1
                    continue

                sourceID = items[dlFileHeaderItems["sourceid"]]
                DEMname = items[dlFileHeaderItems["dem_name"]]
                DEMpath = items[dlFileHeaderItems["dem_path"]]

                movedFile+=1
                destFolder = getDownloadFolder2(DEMname)

                # Delete all files associated with the DEMname (.xml, aux...etc)
                for file in glob.glob(f"{DEMpath}{os.sep}{DEMname.split('.')[0]}*"):
                    if os.path.isfile(file):
                        dest = os.path.join(destFolder,os.path.basename(file))
                        if not os.path.exists(dest):
                            shutil.copy2(file,dest)
                            AddMsgAndPrint(f"Successfully Moved -- {os.path.basename(file)} TO {dest} -- ({movedFile:,} of {recCount:,})")

                        else:
                            AddMsgAndPrint(f"File -- {os.path.basename(file)} already existed in {destFolder} -- ({movedFile:,} of {recCount:,})")

                    else:
                        AddMsgAndPrint(f"NOT a valid file -- {sourceID} -- {file}")
                        invalidFiles+=1

        del fp, deleteLogFileName

        """ ------------------------------------ SUMMARY -------------------------------------- """
        end = toc(start)
        AddMsgAndPrint(f"\n{'-'*40}SUMMARY{'-'*40}")

        AddMsgAndPrint(f"\nNumber of files to move: {recCount:,}")
        if movedFile > recCount:
            AddMsgAndPrint(f"\nNumber of total associated files moved: {movedFile:,}")

        if invalidFiles:
            AddMsgAndPrint(f"\nNumber of invalid files: {invalidFiles:,}")

    except:
        errorMsg()
