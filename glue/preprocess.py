#!/usr/bin/env python
# coding: utf-8

# Import pandas library for csv import
import zipfile
import boto3
import pandas as pd
import sys
import os
from awsglue.utils import getResolvedOptions

s3 = boto3.client('s3')


def download_and_extract(bucket, fileuri, csv_dir):
    dst_path = os.path.join('/tmp', fileuri)
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
    
    s3.download_file(bucket, fileuri, dst_path)

    with zipfile.ZipFile(dst_path, 'r') as zipf:
        zipf.extractall(csv_dir)


def preprocess(csv_dir):
    # Load csv
    tts_df = pd.read_csv(os.path.join(csv_dir, 'TTS.csv'))
    im_df = pd.read_csv(os.path.join(csv_dir, 'IM.csv'))
    # and do nothing because our sample dataset has been refined already
    # you can implement your own preprocessing logic here if you want to

def save_to_s3(bucket, csv_dir):
    csv_list = os.listdir(csv_dir)

    for single_csv in csv_list:
        object_key = os.path.join('input/', os.path.basename(single_csv))
        s3.upload_file(os.path.join(csv_dir, single_csv), bucket, object_key)
        

# main
args = getResolvedOptions(sys.argv, ['bucket', 'fileuri'])
bucket = args['bucket']
fileuri = args['fileuri']

csv_dir = 'input/'
download_and_extract(bucket, fileuri, csv_dir)
preprocess(csv_dir)
save_to_s3(bucket, csv_dir)
