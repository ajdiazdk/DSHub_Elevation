# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 23 09:57:18 2023

@author: Charles.Ferguson
"""
import ray
@ray.remote
def download(url, location):
    
    raster = os.path.basename(url)
    destfile = os.path.join(dest, raster)
    
    r = requests.get(url, allow_redirects=True)
    open(destfile, 'wb').write(r.content)

import requests, os, subprocess, shutil

s = """https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n41x75_w103x75_ne_northplatteriver_2010.zip
https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n42x00_w103x50_ne_northplatteriver_2010.zip
https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n42x00_w103x75_ne_northplatteriver_2010.zip
https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n42x00_w104x00_ne_northplatteriver_2010.zip
https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n42x00_w104x25_ne_northplatteriver_2010.zip
https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n42x25_w103x75_ne_northplatteriver_2010.zip
https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n42x25_w104x00_ne_northplatteriver_2010.zip
https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/19/IMG/ned19_n42x25_w104x25_ne_northplatteriver_2010.zip"""

theURLs = s.split('\n')

#pwd = input('Enter db password:')
pwd = 'ilovesql'

dest = r"F:\DSHub\Elevation\USGS_Elevation\30M" 
#futures = [download.remote(url, dest) for url in theURLs]

#result = ray.get(futures)

#zips = [os.path.join(dest,os.path.basename(x)) for x in theURLs]

# for z in zips:
#     shutil.unpack_archive(z, dest)
   
os.chdir(dest)
env={'PGPASSWORD':pwd,
     'PGHOST' : 'localhost',
     'PGPORT' : '5432',
     'PGUSER' : 'postgres',
     'PGDATABASE' : "fp",
     'SYSTEMROOT': os.environ['SYSTEMROOT'],
     'PROJ_LIB': r"C:\Program Files\PostgreSQL\15\share\contrib\postgis-3.3\proj"}


#os.environ['PROJ_LIB'] = r"C:\Program Files\PostgreSQL\15\share\contrib\postgis-3.3\proj"
# the cmd below will go into public schema unless
# search_path has been changed ahead of time
# alter database fp set search_path = hub,public;
pLst = [os.path.join(dest, x) for x in os.listdir(dest) if x.endswith('.tif')]

for raster in pLst[0:20]:
    
    #cmd  = r'"C:\Program Files\PostgreSQL\15\bin\raster2pgsql.exe" -s 4269 -b 1 -I -t 507x507 -F -R '+ raster + ' | "C:\\Program Files\\PostgreSQL\\15\\bin\\psql.exe" -U postgres -d fp -h localhost -p 5432'
    cmd  = r'"C:\Program Files\PostgreSQL\15\bin\raster2pgsql.exe" -s 4269 -b 1 -I -t 507x507 -F -a -R '+ raster + ' elevation.elevation_30m | "C:\\Program Files\\PostgreSQL\\15\\bin\\psql.exe" -U postgres -d elevation -h localhost -p 5432'
    exe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True, env=env)
    msgs, errors = exe.communicate()
    errors = ','.join(errors.strip().split('\n')).lower()
    
    if errors:
        print(errors)
    else:
        print(msgs)