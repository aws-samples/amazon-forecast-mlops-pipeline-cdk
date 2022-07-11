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


def download_csvs(bucket, export_dir_key, local_dst):
    os.makedirs(local_dst, exist_ok=True)

    obj_list = s3.list_objects(Bucket=bucket, Prefix=export_dir_key)['Contents']

    for obj in obj_list:
        if obj['Key'].endswith('.csv'):
            basename = os.path.basename(obj['Key'])
            s3.download_file(bucket, obj['Key'], os.path.join(local_dst, basename))


# merge split csvs into a single one 
def merge(csv_dir, dst_path):
    split_list = os.listdir(local_dst)

    df_list = []
    for split_csv in split_list:
        appendant = pd.read_csv(os.path.join(local_dst, split_csv))
        df_list.append(appendant)

    merged_df = pd.concat(df_list, axis=1, join='inner').sort_index()
    merged_df.to_csv(dst_path, index=False)


def save_to_s3(bucket, merged_csv_path, s3_key):
    s3.upload_file(merged_csv_path, bucket, s3_key)
        

# main
args = getResolvedOptions(sys.argv, ['bucket', 'uid'])
bucket = args['bucket']
uid = args['uid']

export_dir_key = f'export/{uid}'
local_dst = '/tmp/export'

download_csvs(bucket, export_dir_key, local_dst)
merged_csv_path = '/tmp/merged.csv'
merge(local_dst, merged_csv_path)

s3_key = f'forecast/{uid}.csv'
save_to_s3(bucket, merged_csv_path, s3_key)
