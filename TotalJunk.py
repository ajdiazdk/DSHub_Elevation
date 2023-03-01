#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Adolfo.Diaz
#
# Created:     31/10/2022
# Copyright:   (c) Adolfo.Diaz 2022
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import sys, time



if __name__ == '__main__':

    bHeader = True
    downloadFile = r'E:\DSHub\Elevation\USGS_3DEP_1M_Metadata_Elevation_02082023.txt'

    # ['huc_digit','prod_title','pub_date','last_updated','size','format'] ...etc
    headerValues = open(downloadFile).readline().rstrip().split(',')

    urlDownloadDict = dict()  # contains download URLs and sourceIDs grouped by HUC; 07040006:[[ur1],[url2]]
    elevMetadataDict = dict() # contains all input info from input downloadFile.  sourceID:dlFile items
    recCount = 0
    badLines = 0

    uniqueSourceIDList = list()
    duplicateSourceIDs = 0
    uniqueURLs = list()
    duplicateURLs = 0

    """ ---------------------------- Open Download File and Parse Information into dictionary ------------------------"""
    with open(downloadFile, 'r') as fp:
        for line in fp:
            items = line.split(',')

            # Skip header line and empty lines
            if bHeader and recCount == 0 or line == "\n":
                recCount+=1
                continue

            # Skip if number of items are incorrect
            if len(items) != len(headerValues):
                badLines+=1
                recCount+=1
                continue

            hucDigit = items[headerValues.index("huc_digit")]
            prod_title = items[headerValues.index("prod_title")]
            pub_date = items[headerValues.index("pub_date")]
            last_updated = items[headerValues.index("last_updated")]
            size = items[headerValues.index("size")]
            fileFormat = items[headerValues.index("format")]
            sourceID = items[headerValues.index("sourceID")]
            metadata_url = items[headerValues.index("metadata_url")]
            downloadURL = items[headerValues.index("download_url")].strip()

            if sourceID in uniqueSourceIDList:
                recCount+=1
                duplicateSourceIDs+=1
                continue
            else:
                uniqueSourceIDList.append(sourceID)

            if downloadURL in uniqueURLs:
                recCount+=1
                duplicateURLs+=1
                continue
            else:
                uniqueURLs.append(downloadURL)

            # Add info to urlDownloadDict
            if hucDigit in urlDownloadDict:
                urlDownloadDict[hucDigit].append([downloadURL,sourceID])
            else:
                urlDownloadDict[hucDigit] = [[downloadURL,sourceID]]

            # Add info to elevMetadataDict
            elevMetadataDict[sourceID] = [hucDigit,prod_title,pub_date,last_updated,
                                          size,fileFormat,sourceID,metadata_url,downloadURL]
            recCount+=1

    # subtract header for accurate record count
    if bHeader: recCount = recCount -1
    print(f"Rec Count: {recCount:,} -- Duplicate SourceIDs: {duplicateSourceIDs:,} -- Duplicate URLs: {duplicateURLs:,}")
