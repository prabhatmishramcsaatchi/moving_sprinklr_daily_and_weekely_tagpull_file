import boto3
import json
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO
import time
import os
from io import StringIO
import botocore
import io  
      
# Define S3 client
s3 = boto3.client('s3')
    
source_bucket='amazon-global-gsmc-sprinklr'
source_weekely_folder='RAWDATA/Fluency-Weekly/'
source_daily_folder='RAWDATA/Fluency-Monthly/'
source_tag_folder='RAWDATA/Tag-Pull/'
destination_bucket='wikitablescrapexample'
destination_weekely_folder='amazon_sprinklr_pull/Fluency-Weekly/'
destination_daily_folder='amazon_sprinklr_pull/fluency/'
destination_tag_folder='amazon_sprinklr_pull/Tag-Pull/'
follower_data_destination='amazon_sprinklr_pull/follower/'
follower_data_filename='Follower_Data_7_FluencyWeekly.json'
ADDITIONAL_FILES = [
    "Paid_Data_11_FluencyWeekly"
]
   

def read_json_from_s3(bucket, key):
    s3_object = s3.get_object(Bucket=bucket, Key=key)
    json_data = s3_object['Body'].read()
    json_data_str = json_data.decode('utf8')
    data = pd.read_json(json_data_str, lines=True)
    return data  


def read_excel_from_s3(bucket, key):
    """Read Excel file from S3."""
    response = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_excel(BytesIO(response['Body'].read()))

def write_results_to_s3(bucket, key, data):
    """Write DataFrame to CSV in S3."""
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())


def folder_for_today(prefix):
    """Return the specific folder for today's date under the given prefix"""
    today = datetime.now().strftime('%Y-%m-%d')
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=prefix, Delimiter='/')
    
    # We will store potential folders with today's date here
    potential_folders = []

    # Check common prefixes (representing folder names) for today's date
    for folder in response.get('CommonPrefixes', []):
        if today in folder['Prefix']:
            potential_folders.append(folder['Prefix'])

 
    if potential_folders:
        return potential_folders[0]

    print(f"No folder for today found in prefix {prefix}")  # Logging
    return None



def copy_files(src_prefix, dest_prefix):
    """Copy files from source prefix to destination prefix, maintaining the source folder structure"""
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=src_prefix)

    # Ensure we are copying the complete folder, not just the files within.
    folder_name = src_prefix.split('/')[-2] + '/'  # Extract the folder name (assuming the format is '.../folder_name/')
    
    for obj in response.get('Contents', []):
        copy_source = {'Bucket': source_bucket, 'Key': obj['Key']}
        dest_key = dest_prefix + folder_name + obj['Key'].replace(src_prefix, "")
        s3.copy_object(Bucket=destination_bucket, CopySource=copy_source, Key=dest_key)
        
 

def read_s3_file(bucket, file_key):
    """Read a file from S3 and return its content as a pandas DataFrame"""
    response = s3.get_object(Bucket=bucket, Key=file_key)
    
    if file_key.endswith('.csv'):
        return pd.read_csv(BytesIO(response['Body'].read()))
    elif file_key.endswith('.xlsx'):
        return pd.read_excel(BytesIO(response['Body'].read()))

def process_files_for_missing_accounts():
    """Function to process specified weekly files if today is Monday"""
    master_table_key = 'amazon_sprinklr_pull/result/master_table.csv'
    country_mapping_key = 'amazon_sprinklr_pull/mappingandbenchmark/countrymapping.xlsx'
            
    master_table_df = read_s3_file(destination_bucket, master_table_key)
    country_mapping_df = read_s3_file(destination_bucket, country_mapping_key)
    
    missing_accounts, new_accounts = get_account_analysis(master_table_df, country_mapping_df)

    if missing_accounts:
        subject = "Missing Accounts Notification"
        body = body = f"The following accounts have not appeared in the master_table_df for the last two weeks: {', '.join(map(str, missing_accounts))}"
        send_email_notification(subject, body)

    if new_accounts:
        subject = "Newly Added Accounts Notification"
        body = body = f"The following accounts are newly added to the master_table_df: {', '.join(map(str, new_accounts))}"

        send_email_notification(subject, body)

  #  print(master_table_df.head())
   # print(country_mapping_df.head())

  

def get_account_analysis(master_table_df, country_mapping_df):
    """Analyze accounts in master_table_df and country_mapping_df to identify missing and new accounts."""
    
    # Accounts in country_mapping_df but missing in master_table_df
    missing_accounts = set(country_mapping_df["Account"]) - set(master_table_df["ACCOUNT"])
    
    # Accounts in master_table_df but missing in country_mapping_df (Newly added accounts)
    new_accounts = set(master_table_df["ACCOUNT"]) - set(country_mapping_df["Account"])
    
    two_weeks_ago_date = (datetime.now() - timedelta(weeks=2)).date()  # Extracting only the date part
    
    accounts_to_notify_missing = []
    for account in missing_accounts:
        last_appearance = master_table_df[master_table_df["ACCOUNT"] == account]["Pull Date"].max()
        
        # Check for NaT values and perform comparison
        if pd.isna(last_appearance) or pd.Timestamp(last_appearance).date() < two_weeks_ago_date:
            accounts_to_notify_missing.append(account)
    
    return accounts_to_notify_missing, list(new_accounts)
   
  
   
def send_email_notification(subject, body):
    """Send an email notification."""
    ses = boto3.client('ses')
   # email_address = ['prabhatmishra160@gmail.com','david.lawton@mcsaatchi.com','prabhat.mishra@mcsaatchi.com','dlawtonmcsaatchi@gmail.com']
    email_address = ['prabhatmishra160@gmail.com','prabhat.mishra@mcsaatchi.com']
 
    try:
        ses.send_email(
            Source=email_address[0],
            Destination={'ToAddresses': email_address},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body}}
            }
        )
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error sending email: {e}")

     

  
            
def lambda_handler(event, context):
    
   # process_files_for_missing_accounts()
    #find_unique_ad_objectives_and_last_pull_date_optimized()
        # Check if today is Monday
    today = datetime.now().weekday()  # Monday is 0 and Sunday is 6
    print(today)
    #is_monday = today == 1
    # Fetch the specific folders with today's date in the source locations
    weekely_folder = folder_for_today(source_weekely_folder)
    daily_folder = folder_for_today(source_daily_folder)
    tag_folder = folder_for_today(source_tag_folder)
                
    if today == 0 and weekely_folder:
        print("Today is Monday. Proceeding to copy follower data.")
        follower_data_src_file_key = weekely_folder + follower_data_filename
        follower_data_dest_file_key = follower_data_destination + follower_data_filename
        # Perform the copy operation
        copy_source = {'Bucket': source_bucket, 'Key': follower_data_src_file_key}
        s3.copy_object(Bucket=destination_bucket, CopySource=copy_source, Key=follower_data_dest_file_key)
        print("Follower data file copied successfully.")
    else:
        print("Not Monday or weekly folder not found. Skipping follower data copy.")
      
    if all([weekely_folder, daily_folder,tag_folder]):
        # Copy the specific folders' contents to the destination
        copy_files(weekely_folder, destination_weekely_folder)
        
        copy_files(tag_folder, destination_tag_folder)
        
        copy_files(daily_folder, destination_daily_folder)
        # After copying, if today is Monday, process the weekly files
        if 1:
            time.sleep(100) 
            process_files_for_missing_accounts()
        
        return {
            'statusCode': 200,
            'body': json.dumps('Files copied successfully!')
        }
    else:
        return {
            'statusCode': 400,
            'body': json.dumps('Not all source folders for today were found.')
        }

         
