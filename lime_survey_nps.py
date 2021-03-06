#!/usr/bin/python

from limesurvey import Api
import base64
import pandas
import json
import datetime
import re
from boto3.session import Session
from io import StringIO
import pandas_redshift as pr

from configparser import ConfigParser

PATH = '/home/pgarcia/repos/prod/lime_etl'

config = ConfigParser()
config.read( PATH + '/.config.ini' )

s3_credentials_AWS_ACCESS_KEY=config['AWS_S3']['AWS_ACCESS_KEY']
s3_credentials_AWS_SECRET_KEY=config['AWS_S3']['AWS_SECRET_KEY']
s3_credentials_BUCKET=config['AWS_S3']['BUCKET']

redshift_credentials_dbname=config['AWS_REDSHIFT']['DBNAME']
redshift_credentials_host=config['AWS_REDSHIFT']['HOST']
redshift_credentials_port=config['AWS_REDSHIFT']['PORT']
redshift_credentials_user=config['AWS_REDSHIFT']['USER']
redshift_credentials_password=config['AWS_REDSHIFT']['PASSWORD']


user=config['LIME_CONFIG']['LIME_USER']
key=config['LIME_CONFIG']['LIME_KEY']
url=config['LIME_CONFIG']['LIME_API_URL']
sid=config['LIME_CONFIG']['LIME_SID_NPS']
token=config['LIME_CONFIG']['LIME_TOKEN_BASE']

DATE_NOW = datetime.datetime.now().strftime('%Y%m%d')

# Build the API
lime = Api(url, user, key)

export_res_token = lime.export_responses(sid=sid, status='all', heading='code', response='short', fields='')
OUTPUT_PATH = PATH+"/lime_export_{s}.txt".format(s=sid)

with open(OUTPUT_PATH, 'w') as outfile:
  json.dump(export_res_token, outfile)

d = pandas.read_json(OUTPUT_PATH)
df = pandas.DataFrame()
for row in d.index:
    data = pandas.DataFrame(d.loc[row].loc['responses']).transpose().reset_index(drop=False)
    df = df.append(data,ignore_index=True)

df = df[['index', 'submitdate', 'lastpage', 'startlanguage', 'startdate', 'datestamp', 'q01', 'q03', 'q06']]


cols = {'index'         : 'id_answer',        # ,'ID da resposta'
        'submitdate'    : 'date_sent',        #'Data de envio'
        'lastpage'      : 'last_page',        # 'Última página'
        'startlanguage' : 'language',         #'Idioma inicial'
        'startdate'     : 'start_date',       #'Data de início'
        'datestamp'     : 'last_action_date', # 'Data da última ação'
        'q01'           : 'nps',              #'NPS Score'
        'q03'           : 'email',            #'E-MAIL'
        'q06'           : 'cohort'            #, 'Cohort'
        }

df.rename(index=str, columns=cols, inplace=True)
df["updated_ts"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df = df.dropna(axis=0, how='any', thresh=None, subset=['email','nps'], inplace=False)
#df["nps"] = df.nps.apply(lambda x : float(x.replace("A","")))

df["nps"] = df.nps.apply(lambda x : float(re.sub("A|N", "", x)))

BUCKET_FOLDER = "limesurvey"
SCHEMA = "manual_data_sources"

S3_FULL_FILE_NAME = "limesurvey/limesurvey_nps_{d}.csv".format(d=DATE_NOW)
REDSHIFT_TABLE_NAME = "manual_data_sources.limesurvey_nps_survey"

print('5. Export to csv buffer')
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

print('6. Upload to S3')
session = Session(aws_access_key_id     = s3_credentials_AWS_ACCESS_KEY,
                  aws_secret_access_key = s3_credentials_AWS_SECRET_KEY)

FILE_NAME = BUCKET_FOLDER + "/" + REDSHIFT_TABLE_NAME + "_" + DATE_NOW + ".csv"
s3 = session.resource('s3')
bucket = s3.Bucket(s3_credentials_BUCKET)
s3.Object(s3_credentials_BUCKET, FILE_NAME).put(Body=csv_buffer.getvalue())


print('3. Connect to S3')
pr.connect_to_s3(aws_access_key_id     = s3_credentials_AWS_ACCESS_KEY,
                 aws_secret_access_key = s3_credentials_AWS_SECRET_KEY,
                 bucket                = s3_credentials_BUCKET,
                 subdirectory          = BUCKET_FOLDER)

print('4. Connect to Redshift')
pr.connect_to_redshift(dbname    = redshift_credentials_dbname,
                        host     = redshift_credentials_host,
                        port     = redshift_credentials_port,
                        user     = redshift_credentials_user,
                        password = redshift_credentials_password)


def delete_from_date(date):
    """ Delete data from table """
    query = "DELETE FROM {table} WHERE start_date >= '{datef}'".format(table=REDSHIFT_TABLE_NAME, datef=date)
    print("PRINT SQL STATEMENT: ",query)
    pr.exec_commit(query)
    return None

delete_from_date(date='2018-01-01')


print('5. Create table')
pr.exec_commit("""
CREATE TABLE IF NOT EXISTS
    {table}
         (id_answer varchar(256),
          date_sent varchar(256),
          last_page varchar(256),
          language varchar(256),
          start_date varchar(256),
          last_action_date varchar(256),
          nps varchar(256),
          email varchar(256),
          cohort varchar(256),
          updated_ts varchar(256));""".format(table=REDSHIFT_TABLE_NAME))


print('6. Upload to Redshift')
pr.exec_commit("""
  COPY {table}
  FROM 's3://amaro-bi/{filepath}'
  ACCEPTINVCHARS 
  delimiter ','
  ignoreheader 1
  csv quote as '"'
  dateformat 'auto'
  timeformat 'auto'
  region 'sa-east-1'
  access_key_id '{access_key}'
  secret_access_key '{secret_key}';
""".format(table = REDSHIFT_TABLE_NAME,
           filepath = FILE_NAME,
           access_key = s3_credentials_AWS_ACCESS_KEY,
           secret_key = s3_credentials_AWS_SECRET_KEY))

print('Finished processing')


pr.close_up_shop()