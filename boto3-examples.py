# -*- coding: utf-8 -*-
"""
Created on Wed May 31 15:14:58 2023

@author: Charles.Ferguson
"""


import boto3, os

f = open(r"D:\GIS\PROJECT_23\DS-HUB\DOC\aws_cred.cred", 'rt')
lines = f.readlines()
f.close()

aws_access_key_id=lines[0][:-1]
aws_secret_access_key=lines[1][:-1]

session = boto3.Session( aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
s3 = session.resource('s3')
bucket_name = 's3-fpac-nrcs-dshub-prod'
my_bucket = s3.Bucket(bucket_name)

# list all file paths
for my_bucket_object in my_bucket.objects.all():
    key = my_bucket_object.key
    print(key)

# list all files 
# start in a specific directory
for my_bucket_object in my_bucket.objects.filter(Prefix="ferguson/"):
    key = my_bucket_object.key
    print(key)
    
# downlaod a file
import boto3
s3 = boto3.client('s3')
bucket_name = 's3-fpac-nrcs-dshub-prod'
# dest = r'D:\TEMP\v7-params.py'
dest = r'D:\GIS\PROJECT_23\DS-HUB\V\metadata_index.zip'
s3.download_file(bucket_name, 'ferguson/metadata_index.zip', dest)

# upload a file
import boto3
s3 = boto3.client('s3')
bucket_name = 's3-fpac-nrcs-dshub-prod'
file = r"D:\GIS\GitHub\DS-Hub\footprint\v8-params-test.py"
s3.upload_file(file, bucket_name, 'ferguson/' + os.path.basename(file))

# delete file
import boto3
s3 = boto3.client('s3')
bucket_name = 's3-fpac-nrcs-dshub-prod'
key = 'ferguson/install.cmd'
s3.delete_object(Bucket = bucket_name, Key=key)

