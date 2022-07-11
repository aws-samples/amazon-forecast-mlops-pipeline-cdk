import boto3
import os
import pandas as pd


bucket = 'forecast-mlops-pipeline-resource-bucket-699690702171'
prefix = 'export/20220628T173905'
dst_dir = 'test'

s3 = boto3.client('s3')
obj_list = s3.list_objects(Bucket=bucket, Prefix=prefix)['Contents']

for obj in obj_list:
    if obj['Key'].endswith('.csv'):
        basename = os.path.basename(obj['Key'])
        s3.download_file(bucket, obj['Key'], os.path.join(dst_dir, basename))

csvs = os.listdir('test')

df_list = []
for csv in csvs:
    if csv.endswith('.csv'):
        appendant = pd.read_csv(os.path.join('test', csv))
        df_list.append(appendant)

merged = pd.concat(df_list, axis=1, join='inner').sort_index()
merged.to_csv('test/merged.csv')
