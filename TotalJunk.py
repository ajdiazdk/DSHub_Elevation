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

import sys, string, os, traceback, glob, textwrap
import urllib, re, time, json, socket, zipfile
import threading, multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime

from urllib.request import Request, urlopen, URLError
from urllib.error import HTTPError

urllibEncode = urllib.parse.urlencode


if __name__ == '__main__':


    try:
        hucURL = r'https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n43x50_w092x50_ia_northeast_2007.z'

        # 'https://websoilsurvey.sc.egov.usda.gov/DSD/Download/Cache/SSA/wss_SSA_WI021_[2021-09-07].zip'
        fileName = hucURL.split('/')[-1]

        downloadFolder = r'E:\DSHub\Elevation\3M_test'

        # set the download's output location and filename
        local_file = f"{downloadFolder}\\{fileName}"

        # Open request to Web Soil Survey for that zip file
        request = urlopen(hucURL)

        # save the download file to the specified folder
        output = open(local_file, "wb")
        output.write(request.read())
        output.close()

    except URLError as e:
        print(f"{'Failed to Download:':<35} {fileName:<60} {str(e):>15}")
        #print(str(e))
        #print(f"{theTab}{'URL Error:':<35} {fileName:<60}")
        #print(f"{e:<35} {fileName:<60}")
        #messageList.append(f"\t{theTab}{e.__dict__}")
        #print(f"{e['code']}")