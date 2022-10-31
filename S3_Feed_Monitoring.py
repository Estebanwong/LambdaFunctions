import json
import boto3
import os
from operator import attrgetter
from datetime import datetime, date, timedelta, timezone
import pandas as pd
from pathlib_mate import Path

s3_res = boto3.resource("s3")
ENV = os.environ["ENV"]

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

# what is the prefix for the jobs that are expected to have their most recent files provided yesterday
day_lag_jobs = ['CSAT-csv/processed/']

# what is the prefix for the jobs that are expected to have their most recent files provided on the last work day
business_day_jobs = ['FundPrices/processed/']

# what is the prefix for the jobs that are expected to have their most recent files provided on the every 15 minutes
every_15_minutes = ['conversationlog/']

# test today and last workday paths
paths = ["s3.console.aws.amazon.com/s3/buckets/sandbox-raw-alightanalytics?region=us-east-1&prefix=participant_satisfaction_survey/processed/&showversions=false",
"s3.console.aws.amazon.com/s3/buckets/sandbox-raw-alightanalytics?region=us-east-1&prefix=participant_satisfaction_survey/interim/&showversions=false"]

# paste any new htmls in the path variable below
# paths = ["s3.console.aws.amazon.com/s3/buckets/prod-raw-salesforce?prefix=CSAT-csv/processed/&region=us-east-1"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-alightanalytics?region=us-east-1&prefix=participant_satisfaction_survey/interim/&showversions=false"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-alightanalytics?region=us-east-1&prefix=AgentPortal/processed/&showversions=false"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-provisioned?region=us-east-1&prefix=alight_5001_5555_reponse/processed-by-script/&showversions=false"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-ava?prefix=conversationlog/&region=us-east-1"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-tba?prefix=FundPrices/processed/&region=us-east-1"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-tba?region=us-east-1&prefix=BusinessProcessEvents/report_interim/&showversions=false"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-tba?prefix=DataChangeEvents/report_interim/&region=us-east-1"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-tba?region=us-east-1&prefix=UDPDataChangeEvents/report_interim/&showversions=false"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-upoint?region=us-east-1&prefix=BusinessEvent/interim/&showversions=false"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-upoint?region=us-east-1&prefix=UxPageUsage/interim/&showversions=false"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-salesforce?prefix=Account/&region=us-east-1"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-salesforce?prefix=Case/&region=us-east-1"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-salesforce?prefix=CX_Address__c/interim/&region=us-east-1"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-salesforce?prefix=CX_Email__c/interim/&region=us-east-1"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-salesforce?prefix=CX_OnlineSetupSubmission__c/interim/&region=us-east-1"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-salesforce?prefix=CX_Phone__c/interim/&region=us-east-1"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-salesforce?region=us-east-1&prefix=CX_Transaction__c/interim/&showversions=false"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-niceincontact?region=us-east-1&prefix=cdr/interim/&showversions=false"
# ,"https://us-east-1.console.aws.amazon.com/s3/buckets/prod-raw-okta?prefix=converge_logs/interim/year%3D2022/&region=us-east-1"
# ,"https://s3.console.aws.amazon.com/s3/buckets/prod-raw-okta?prefix=myplan_logs/processed/&region=us-east-1"]

def utc_now() -> datetime:
    return datetime.utcnow().replace(tzinfo=timezone.utc)

def read_date_lookup_table_from_local_file() -> pd.DataFrame:
    path = Path.dir_here(__file__) / "date_lookup_table.csv"
    return pd.read_csv(path.abspath)

def find_previous_business_day(df: pd.DataFrame, today: date) -> date:
    sub_df = df[((df["date"] < str(today)) & (df["is_business_day"] == "Y"))]
    return datetime.strptime(sub_df["date"].max(), "%Y-%m-%d").date()

def notify(msg):
    subject = "failed to get most recent data: Do not reply"
    client = boto3.client("sns")
    client.publish(TopicArn=SNS_TOPIC_ARN, Message=msg, Subject=subject)

def lambda_handler(event, context):
    message_list = ''
    for path in paths:
        needed_info = path.split(ENV)[1].split("?")
        bucket_name = ENV + needed_info[0]
        key = needed_info[1].split('prefix=')[1].split('&')[0]
        my_bucket = s3_res.Bucket(bucket_name).objects.filter(Prefix=key)
        sorted_objs_by_latest = sorted(my_bucket, key=attrgetter("last_modified"))
        
        most_recent_file = sorted_objs_by_latest[-1]
        most_recent_file_datetime_dt = most_recent_file.last_modified.replace(tzinfo=timezone.utc)
        most_recent_file_day = str(most_recent_file_datetime_dt)[:10]

        now = utc_now()
        now_minus_15_min = utc_now() - timedelta(minutes=15)
        df_lkup = read_date_lookup_table_from_local_file()
        today = now.date()
        yesterday = str(utc_now() - timedelta(days=1))[:10]
        previous_business_day = find_previous_business_day(df_lkup, today)
        
        if key in day_lag_jobs and most_recent_file_day != str(yesterday):
            message_list += f"bucket: {bucket_name} key: {key} should have a file yesterday so there should be one on {yesterday} but last file was on {most_recent_file_day} \n"
        elif key in business_day_jobs and most_recent_file_day != str(previous_business_day):
            message_list += f"bucket: {bucket_name} key: {key} should have a file every business day so there should be one on {previous_business_day} but last file was on {most_recent_file_day} \n"
        elif key in every_15_minutes and not (now_minus_15_min <= most_recent_file_datetime_dt <= now):
            message_list += f"path={bucket_name}{most_recent_file.key} last file was created on {most_recent_file_datetime_dt}, but should be between {now_minus_15_min} and {now} \n"
        elif key not in day_lag_jobs +business_day_jobs +every_15_minutes and most_recent_file_day != str(today):
            message_list += f"bucket: {bucket_name} key: {key} should have a file daily so there should be one on {today} but last file was on {most_recent_file_day} \n"
        print(f"path={bucket_name}{most_recent_file.key}, last_modified={most_recent_file.last_modified}, size={most_recent_file.size} bytes")
        
    if message_list != '':
        notify(message_list)
