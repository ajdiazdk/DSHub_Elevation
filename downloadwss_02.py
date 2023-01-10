# c:\notes\2022\09\Gaia-2380\hack\C:\notes\2022\09\Gaia-2380\hack\downloadwss_02.py
# Gaia-2380 - SPIKE: Review Adolfo Diaz's SSURGO data downloader for potential incorporation into SSURGO Portal UI
#
# (Earlier versions named c:\notes\2022\09\Gaia-2380\hack\legacy##.py)
# 00 Gaia-2380 - SPIKE: Review Adolfo Diaz's SSURGO data downloader for potential incorporation into SSURGO Portal UI
#   Copy of C:\notes\2022\09\Gaia-2380\Soil-Data-Development-Tools---ArcGIS-Pro_AD\Scripts\SSURGO_BatchDownload.py
# 01 Gaia-2380 - SPIKE: Review Adolfo Diaz's SSURGO data downloader for potential incorporation into SSURGO Portal UI
#   Hard code some parameters - can I run the legacy script?
#   The ArcPy library references are replaces with some shims
# 02 Gaia-2380 - SPIKE: Review Adolfo Diaz's SSURGO data downloader for potential incorporation into SSURGO Portal UI
#   Requires "pip install requests"
#   Usage: downloadwss_02.py "whereClause" "rootPath"

import concurrent.futures, csv, itertools, json, locale, os, re, requests, shutil, sys, time, traceback, zipfile
from datetime import datetime
from io import BytesIO

def errorMsg():
    try:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        theMsg = "\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[1] + "\n\t" + traceback.format_exception(exc_type, exc_value, exc_traceback)[-1]

        if theMsg.find("exit") > -1:
            print("\n\n")
        else:
            print(theMsg,2)
    except:
        print("Unhandled error in unHandledException method", 2)

def getNowTime():
    return datetime.now().strftime("%H:%M:%S")

def tic():
    """ Returns the current time """
    return time.time()

def toc(_start_time):
    """ Returns the total time by subtracting the start time - finish time"""
    try:
        t_sec = round(time.time() - _start_time)
        (t_min, t_sec) = divmod(t_sec,60)
        (t_hour, t_min) = divmod(t_min,60)
        return f'{t_hour:02d}:{t_min:02d}:{t_sec:02d} HH:MM:SS'
    except:
        errorMsg()

def splitThousands(someNumber):
    """ will determine where to put a thousands separator if one is needed.
        Input is an integer.  Integer with or without thousands seperator is returned."""
    try:
        return re.sub(r'(\d{3})(?=\d)', r'\1,', str(someNumber)[::-1])[::-1]
    except:
        errorMsg()
        return someNumber

def Number_Format(num, places=0, bCommas=True):
    try:
    # Format a number according to locality and given places
        #locale.setlocale(locale.LC_ALL, "")
        if bCommas:
            theNumber = locale.format("%.*f", (places, num), True)
        else:
            theNumber = locale.format("%.*f", (places, num), False)
        return theNumber
    except:
        errorMsg()
        return num

def getCanonicalDatetimeFromSaverest(saverest):
    # Given an saverest value such as 
    #   '9/4/2021 3:25:29 AM'
    #   '12/31/2022 3:25:29 PM'
    # returns a canonical datetime formatted as
    #   %Y-%m-%d %H:%M:%s
    # for example,
    #   '2021-09-04 03:25:29'
    #   '2022-12-31 15:25:29'
    # These may be lexically compared.

    if len(saverest.split(' ')) == 2:
        p = 'n/a'
        (d,t) = saverest.split(' ')
    else:
        (d,t,p) = saverest.split(' ')

    (mon, day, year) = d.split('/')
    (h, m, s) = t.split(':')
    hr = int(h)
    if p == 'PM' and hr < 12: hr += 12
    canonicalDateTime = f"{year}-{mon.zfill(2)}-{day.zfill(2)} {format(hr).zfill(2)}:{m.zfill(2)}:{s.zfill(2)}"
    return canonicalDateTime

def executeSdaQuery(query, sdaPostRestUrl):
    # Retrieve results from an SDA query.
    # Usage:
    #   (status, responseObject, errormessage) = executeSdaQuery(query, sdaPostRestUrl)
    # where, if successful, the responseObject is the response from the SDA post.rest query.

    params = {'format':'json', 'query':query}
    try:
        r = requests.post(url = sdaPostRestUrl, data = params)
        if r.status_code == 200:
            return (True, r, None)
        else:
            errormessage = f'Error: status={r.status_code}, reason={r.reason}, params={format(params)}'
            return (False, r, errormessage)
    except Exception as ex:
        return (False, r, f'Exception in executeSdaQuery: {format(ex)} at {traceback.format_exc()}')

def getAreasymbolZipfileNames(areasymbolTuples):
    # Given a list of the tuple (areasymbol, saverest), where saverest may be similar to 
    # one of:
    #   '9/13/2021 3:25:29 PM'
    #   '12/31/2022 3:25:29 PM'
    # creates the corresponding WSS zipfile name and adds it to the end of the tuple.
    # The name is assumed to be similar to:
    #   wss_SSA_TX299_soildb_US_2003_[2021-09-13].zip
    for tuple in areasymbolTuples:
        (areasymbol, saverest) = tuple
        mdy= saverest[0:saverest.find(' ')]
        mdyParts = mdy.split('/')
        reformattedDate = f"{mdyParts[2]}-{mdyParts[0].zfill(2)}-{mdyParts[1].zfill(2)}"
        zipName = f"wss_SSA_{areasymbol}_soildb_US_2003_[{reformattedDate}].zip"
        tuple[2] = zipName
    return areasymbolTuples

def getAreasymbolData(baseUrl, rootPath, whereClause):
    # Return a list of dictionaries, with each dictionary containing values for:
    #   areasymbol, saverest, canonicalSaverest, zipfileName, zipfileUrl, rootPath
    #  - note that the zipfileName indicates that no MDB file is included
    # Usage:
    #   The "whereClause" contains the body of a 
    #   "where" clause (excluding the word "WHERE")
    # The response is a list of two-tuples containing the 
    # areasymbol and saverest.
    # For example, using whereClause = "areasymbol like 'WY66%'"
    # yields
    #   [['WY661', '9/13/2021 3:25:29 PM'], ['WY662', '9/13/2021 3:27:59 PM'], 
    #   ['WY663', '9/13/2021 3:28:44 PM'], ['WY665', '9/13/2021 3:29:59 PM'], 
    #   ['WY666', '9/13/2021 3:31:14 PM'], ['WY667', '9/13/2021 3:32:00 PM']]
    # Usage:
    #   (status, responseObject, errormessage) = getAreasymbolData(whereClause = '')
    # where, if successful, the responseObject is the list of two-tuples with 
    # the saverest values converted into WSS zip file names.

    r = None
    try:
        query = f"SELECT areasymbol, saverest FROM sacatalog "
        if whereClause:
            query += f" WHERE {whereClause}"
        query += " ORDER BY areasymbol ASC"
        sdaPostRestUrl = 'https://sdmdataaccess-dev.dev.sc.egov.usda.gov/tabular/post.rest'

        (status, responseObject, errormessage) = executeSdaQuery(query, sdaPostRestUrl)
        if not status:
            return  (status, responseObject, errormessage)

        # Successful retrieval, convert the JSON string to a Python object,
        # convert each saverest value into a WSS zip file name, and return the 
        # table contents as list of the tuples (areasymbol, saverest, WSS zipfile name, zipfileUrl).
        if "Table" in responseObject.text:
            areasymbolTuples = json.loads(responseObject.text)["Table"]
        else:
            areasymbolTuples = []

        # Create the dictionary entry for each tuple
        areasymbolDictionaries = []
        for tuple in areasymbolTuples:
            (areasymbol, saverest) = tuple
            canonicalSaverest = getCanonicalDatetimeFromSaverest(saverest)
            zipDate = canonicalSaverest.split(' ')[0]
            zipfileName = f"wss_SSA_{areasymbol}_[{zipDate}].zip"
            areasymbolDictionaries.append({
                'areasymbol':areasymbol,
                'saverest':saverest,
                'canonicalSaverest':canonicalSaverest,
                'zipfileName':zipfileName,
                'zipfileUrl': f"{baseUrl}/{zipfileName}",
                'rootPath':rootPath
            })

        return (True, areasymbolDictionaries, None)

    except Exception as ex:
        return (False, r, f'Exception in getAreasymbolData: {format(ex)} at {traceback.format_exc()}')

def filterCurrentAreasymbols(areasymbolDictionaries):
    # For each areasymbol, split the list into
    #   skippedAreasymbols: areasymbols that already are "well formed" do not need to be replaced
    #   filteredAreasymbolDictionaries: areasymbols that are newer or need to be replaced.
    # Usage:
    #   (skippedAreasymbols, filteredAreasymbolDictionaries) = filterCurrentAreasymbols(areasymbolDictionaries)
    # A valid areasymbol exists if:
    #   1. The tabular\sacatlog.txt and spatial\<sapolygon_shapefile> files exist
    #   2. The saverest can be extracted from the tabular\sacatlog.txt file.
    # A "filteredReason" is added to the individual dictionaries
    # We're only interested in areasymbols that are exactly five characters in length.

    skippedAreasymbols = []
    filteredAreasymbolDictionaries = []
    filterReason = ""

    for areasymbolDictionary in areasymbolDictionaries:
        # Dictionary keys: areasymbol, saverest, canonicalSaverest, zipfileName, zipfileUrl, rootPath
        areasymbol = areasymbolDictionary["areasymbol"]
        rootPath = areasymbolDictionary["rootPath"]
        saverest = areasymbolDictionary["saverest"]
        canonicalSaverest = areasymbolDictionary["canonicalSaverest"]

        # For SSURGO we want areasymbols that are 5 characters long.
        if len(areasymbol) == 5:
            folderPath = os.path.join(rootPath, areasymbol)
            tabularPath = os.path.join(folderPath,'tabular')
            spatialPath = os.path.join(folderPath,'spatial')
            sacatlogPath = os.path.join(tabularPath, 'sacatlog.txt')
            sapolygonShapefileName = f'soilsa_a_{areasymbol}.shp'
            sapolygonPath = os.path.join(spatialPath, sapolygonShapefileName)

            # The local saverest only exists if the paths are ok and the 
            # local saverest can be read.
            localSaverest = False
            pathsExist = os.path.isfile(sacatlogPath) and os.path.isfile(sapolygonPath)
            if pathsExist:
                with open(sacatlogPath, 'r', encoding='UTF-8') as file:
                    csvreader = csv.reader(file, delimiter='|', quotechar='"')
                    for row in csvreader:
                        localSaverest = str(row[3])
                        canonicalLocalSaverest = getCanonicalDatetimeFromSaverest(localSaverest)
                        break
                if localSaverest and canonicalLocalSaverest < canonicalSaverest:
                    filterReason = f"Local version date ({localSaverest}) < WSS date ({saverest})"
                    areasymbolDictionary["filterReason"] = filterReason
                    filteredAreasymbolDictionaries.append(areasymbolDictionary)
                else:
                    fileReason =  f"Local version date ({localSaverest}) >= WSS date ({saverest})"
                    areasymbolDictionary["filterReason"] = filterReason
                    skippedAreasymbols.append(areasymbolDictionary)
            else:
                filterReason = f"Expected tabular ({sacatlogPath}) and spatial ({sapolygonPath}) files not found."
                areasymbolDictionary["filterReason"] = filterReason
                filteredAreasymbolDictionaries.append(areasymbolDictionary)
        else:
            fileReason =  "Areasymbol must be five characters in length."
            areasymbolDictionary["filterReason"] = filterReason
            skippedAreasymbols.append(areasymbolDictionary)

    return (skippedAreasymbols, filteredAreasymbolDictionaries)

def concurrently(fn, inputs, useThreads, max_concurrency):
    # Concurrently.py from 
    #   https://github.com/alexwlchan/concurrently
    #   https://github.com/alexwlchan/concurrently/blob/main/concurrently.py
    #   https://alexwlchan.net/2019/10/adventures-with-concurrent-futures/
    """
    Calls the function ``fn`` on the values ``inputs``.

    ``fn`` should be a function that takes a single input, which is the
    individual values in the iterable ``inputs``.

    Generates (input, output) tuples as the calls to ``fn`` complete.

    See https://alexwlchan.net/2019/10/adventures-with-concurrent-futures/ for an explanation
    of how this function works.

    """
    # Make sure we get a consistent iterator throughout, rather than
    # getting the first element repeatedly.
    fn_inputs = iter(inputs)

    if useThreads:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(fn, input): input
                for input in itertools.islice(fn_inputs, max_concurrency)
            }

            while futures:
                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )

                for fut in done:
                    original_input = futures.pop(fut)
                    yield original_input, fut.result()

                for input in itertools.islice(fn_inputs, len(done)):
                    fut = executor.submit(fn, input)
                    futures[fut] = input
    else:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = {
                executor.submit(fn, input): input
                for input in itertools.islice(fn_inputs, max_concurrency)
            }

            while futures:
                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )

                for fut in done:
                    original_input = futures.pop(fut)
                    yield original_input, fut.result()

                for input in itertools.islice(fn_inputs, len(done)):
                    fut = executor.submit(fn, input)
                    futures[fut] = input        

## ===================================================================================
def DownloadSSURGOdatasetFromWSS(areasymbolDictionary):
    # Description
    #   Retrieve and unzip the WSS data.
    # Usage:
    #   areasymbolResponse = DownloadSSURGOdatasetFromWSS(areasymbolDictionary)
    # Where:
    #   areasymbolDictionary: dictionary with keys
    #       areasymbol, saverest, canonicalSaverest, zipfileName, zipfileUrl, rootPath
    #   Returns: areasymbolDictionary with additional keys:
    #       status:     boolean
    #       message     explanitory message

    try:
        messageList = list()

        areasymbol = areasymbolDictionary["areasymbol"]
        zipfileUrl = areasymbolDictionary["zipfileUrl"]
        rootPath = areasymbolDictionary["rootPath"]

        # Grab URL response as a file-like object
        r = requests.get(zipfileUrl)
        if r.status_code != 200:
            errormessage = f"Status_code {r.status_code} from {zipfileUrl}"
            return (False, None, errormessage)

        # Kill any existent folder
        targetPath = os.path.join(rootPath, areasymbol)
        if os.path.exists(targetPath):
            shutil.rmtree(targetPath)

        # Get zip file contents
        #   https://docs.python.org/3/library/zipfile.html
        #   https://realpython.com/python-zipfile/
        #   https://medium.com/dev-bits/ultimate-guide-for-working-with-i-o-streams-and-zip-archives-in-python-3-6f3cf96dca50
        with BytesIO(r.content) as zipStream:
            with zipfile.ZipFile(zipStream) as zipFile:
                #zipFile.printdir()
                zipFile.extractall(rootPath)

        areasymbolDictionary["status"] = True
        areasymbolDictionary["message"] = f"Populated folder {targetPath}"
        return areasymbolDictionary

    except zipfile.BadZipFile as ex:
        areasymbolDictionary["status"] = False
        areasymbolDictionary["message"] = f"Unable to unzip file from {zipfileUrl}"
        return areasymbolDictionary

    except Exception as ex:
        message = f"Unable to unzip file from {zipfileUrl}, format(ex)"
        areasymbolDictionary["status"] = False
        areasymbolDictionary["message"] = message
        return areasymbolDictionary

def main(argv):
    # Usage: downloadwss_02.py "whereClause" "rootPath"
    if len(argv) < 3:
        print('Usage: downloadwss_02.py "whereClause" "rootPath"')
        print('  whereClause: body of an sacatalog  "WHERE" clause')
        print('    Note 1: the argument must be surrounded by quotation marks.')
        print('    Note 2: the argument is case-insensitive.')
        print('    Note 3: areasymbols that are not five characters long will be ignored.')
        print("    Example 1: \"areasymbol = 'WY601'")
        print("    Example 2: \"areasymbol IN ('CO634','CO657','NH605')\"")
        print("    Example 3: \"areasymbol LIKE 'TX1%")
        print("  rootPath: the folder that will receive the unzipped WSS SSURGO downloads.")
        print('    The folder will be created if it does not exist.')
        print('    Note that the entire argument must be surrounded by quotation marks.')
        print('')
        print('For each areasymbol, if "valid" SSURGO data exists (both sacatlog.txt')
        print('and sapolygon shapefile exist) and the date of this local data is newer')
        print('than data in WSS, then SSURGO data for the areasymbol will not be')
        print('downloaded.')
        print('')
        print('PERFORMANCE CAUTION:')
        print('  Do not download large numbers of SSAs to a OneDrive-hosted folder.')
        return
    else:
        whereClause = argv[1]
        rootPath = argv[2]
        # Create rootPath if needed
        if not os.path.exists(rootPath):
            os.makedirs(rootPath)
    baseUrl = 'https://websoilsurvey.sc.egov.usda.gov/DSD/Download/Cache/SSA'

    # Internal parameters used for development
    overwriteAll = False
    concurrencyLimit = 16    # Only use concurrency if the limit > 0
    useThreads = True
    verbose = False

    try:
        start = tic()

        # Get the listof areasymbol dictionaries given the whereClause. Each contains values for the keys
        #   areasymbol, saverest, canonicalDateTime, zipFileName, zipUrl
        print(f"{getNowTime()} Getting WSS catalog")
        (status, areasymbolDictionaries, errormessage) = getAreasymbolData(baseUrl, rootPath, whereClause)
        if not areasymbolDictionaries:
            print(f"{getNowTime()} No areasymbols identified.")    
            return
        else:
            print(f"{getNowTime()} Length of returned list: {len(areasymbolDictionaries)}")

        # Drop prior folders if requested
        if overwriteAll:
            for areasymbolDictionary in areasymbolDictionaries:
                targetPath = os.path.join(areasymbolDictionary["rootPath"], areasymbolDictionary["areasymbol"] )
                if os.path.exists(targetPath):
                    shutil.rmtree(targetPath)

        # Filter to skip over local "good" folders that are older than ones in WSS.
        print(f"{getNowTime()} Filtering WSS areasymbols")
        (skippedAreasymbols, filteredAreasymbolDictionaries) = filterCurrentAreasymbols(areasymbolDictionaries)

        if len(skippedAreasymbols) > 0:
            print(f'{getNowTime()} Skipped areasymbols: ')
            skipped = []
            for skippedAreasybol in skippedAreasymbols:
                skipped.append(skippedAreasybol["areasymbol"])
            print(skipped)

        print(f"{getNowTime()} Number of areasymbols to download: {len(filteredAreasymbolDictionaries)}")

        if len(filteredAreasymbolDictionaries) == 0:
            print(f'{getNowTime()} No WSS import required')
            return
        else:
            print(f"{getNowTime()} Starting Web Soil Survey Download process.")

            # Manage concurrent requests
            #   https://alexwlchan.net/2019/10/adventures-with-concurrent-futures/
            #   https://github.com/alexwlchan/concurrently
            if concurrencyLimit > 0:
                # (useThreads, max_concurrency, fn, inputs)
                for (input, output) in concurrently(DownloadSSURGOdatasetFromWSS, filteredAreasymbolDictionaries, useThreads, concurrencyLimit):
                    if verbose:
                        print(getNowTime())
                        print(output)
                    else:
                        try:
                            print(f'{getNowTime()} Completed {output["areasymbol"]}')
                        except Exception as ex:
                            print(f'{getNowTime()} Exception: {format(ex)}')
                            if output:
                                print('output object (followed by exit):')
                                print(output)
                                sys.exit()
                            else:
                                print('output object is false, will exit')
                                sys.exit()
            else:
                for filteredAreasymbolDictionary in filteredAreasymbolDictionaries:
                    output = DownloadSSURGOdatasetFromWSS(filteredAreasymbolDictionary)
                    if verbose:
                        print(getNowTime())
                        print(output)
                    else:
                        print(f'{getNowTime()} Completed {output["areasymbol"]}    (iterated retrieval)')

        end = print(f"{getNowTime()} Elapsed time: {toc(start)}")

    except Exception as ex:
        print(f'{getNowTime()} Exception: {format(ex)}')
        errorMsg()

if __name__ == '__main__':
    main(sys.argv)
    #main(['', "areasymbol LIKE 'VT001'", r"c:\test\root"])
