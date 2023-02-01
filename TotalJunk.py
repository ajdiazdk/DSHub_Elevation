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


def print_progress_bar(index, total, label):
    n_bar = 50  # Progress bar width
    progress = index / total
    sys.stdout.write('\r')
    sys.stdout.write(f"[{'=' * int(n_bar * progress):{n_bar}s}] {int(100 * progress)}%  {label}")
    sys.stdout.flush()


if __name__ == '__main__':


    foo_list = ["a", "b", "c", "d"]
    total = len(foo_list)

    for index, item in enumerate(foo_list):
        print(item)
        #print_progress_bar(index, total, "foo bar")
        #time.sleep(0.5)

##    try:
##        hucURL = r'https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n43x50_w092x50_ia_northeast_2007.z'
##
##        # 'https://websoilsurvey.sc.egov.usda.gov/DSD/Download/Cache/SSA/wss_SSA_WI021_[2021-09-07].zip'
##        fileName = hucURL.split('/')[-1]
##
##        downloadFolder = r'E:\DSHub\Elevation\3M_test'
##
##        # set the download's output location and filename
##        local_file = f"{downloadFolder}\\{fileName}"
##
##        # Open request to Web Soil Survey for that zip file
##        request = urlopen(hucURL)
##
##        # save the download file to the specified folder
##        output = open(local_file, "wb")
##        output.write(request.read())
##        output.close()
##
##    except URLError as e:
##        print(f"{'Failed to Download:':<35} {fileName:<60} {str(e):>15}")
##        #print(str(e))
##        #print(f"{theTab}{'URL Error:':<35} {fileName:<60}")
##        #print(f"{e:<35} {fileName:<60}")
##        #messageList.append(f"\t{theTab}{e.__dict__}")
##        #print(f"{e['code']}")total = 10000000

