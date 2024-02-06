import pandas as pd
import requests
from util import generate_schema, get_redshift_connection,\
execute_sql, create_bucket,create_transformed_bucket,create_database_conn,\
write_to_s3
from io import StringIO
import logging
import io
import json
import boto3
import psycopg2
from datetime import datetime
import ast
from dotenv import dotenv_values
from botocore.exceptions import NoCredentialsError


config = dict(dotenv_values('.env'))
def get_api_data():
    try:
        # Load environment variables from .env file
        url = config.get('URL')
        querystring = ast.literal_eval(config.get('QUERYSTRING'))
        headers = ast.literal_eval(config.get('HEADERS'))
        # Make the API request
        response = requests.get(url, headers=headers, params=querystring)
            # Check if the 'data' key is present in the JSON response
        if 'data' in response.json():
            df = pd.json_normalize(response.json()['data'])
            #print(df)

            # Specify a complete file path for saving the JSON file
            file_path = 'data/raw_data.json'
            # Save the DataFrame to a JSON file using the 'with' statement
            with open(file_path, 'w') as json_file:
                df.to_json(json_file, orient='records', lines=True)

            print(f"Data successfully saved to {file_path}")
        else:
            print("No 'data' key found in the JSON response.")
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
# Call the function to extract data and save to JSON
#get_api_data()

# # STEP 2 CREATING A BUCKET AND LOADING THE JSON DATA to S3 BUCKET
        #STEP 1 :CREATING A DATABASE CONNECTION


#     #== LOADING DATA INTO S3 BUCKET ====
        
def load_json_data():
    try:
        s3, bucket_name, region = create_database_conn()

        # Specify the local path to your JSON file
        local_file_path = 'data/raw_data.json'

        # Specify the S3 key (file name in the S3 bucket)
        s3_key = f'{bucket_name}/{local_file_path}'

        # Upload the JSON file to S3
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"JSON file '{local_file_path}' uploaded to S3 bucket '{bucket_name}' as '{s3_key}'.")
    except NoCredentialsError:
        print("Credentials not available or incorrect.")
    except Exception as e:
        print(f"Error: {e}")

# Call the functions
# create_bucket()
# load_json_data()
print('JSON FILE FINALLY UPLOADED TO S3 BUCKECT')
        

# #STEP 3:DATA TRANSFORMATION
def transformed_data_fnx():
    # Specify the path to your JSON file and CSV file
    json_file_path = 'data/raw_data.json'
    csv_file_path = 'data/transformed_data.csv'
    # Initialize lists to store JSON objects and data
    records = []
    data_list = []
    required_data = ['employer_website', 'job_id', 'job_employment_type', 'job_title', 'job_apply_link',
                    'job_city', 'job_country', 'job_posted_at_timestamp', 'employer_company_type']

    # Read the JSON file line by line
    with open(json_file_path, 'r') as json_file:
        for line_number, line in enumerate(json_file, start=1):
            try:
                # Load JSON data for each line
                record = json.loads(line)
                records.append(record)

                # Access required data from the JSON object
                data_to_append = {key: record[key] for key in required_data}
                data_list.append(data_to_append)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON on line {line_number}: {line}")
                print(f"Error details: {e}")

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(data_list)

    # Print the DataFrame
    # print(df.head())
    
    # ========== TRANSFORMATION STAGE ==========
    df['job_posted_at_timestamp'] = pd.to_datetime(df['job_posted_at_timestamp']).dt.date
    # print(df['job_posted_at_timestamp'].head())

    # Save the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False)
    print(f"Data has been saved to {csv_file_path}")

# Call the function to execute the code
# transformed_data()

# Write data to S3 Bucket
# print('JSON FILE FINALLY UPLOADED TO S3 BUCKECT')
# #create_transformed_bucket()
# csv_data = read_local_csv()
# write_to_s3(csv_data)
# # #============================================================================================
# COPYING  INTO THE AWS_REDSHIFT
def load_to_redshift(tranbucket_name, file_name,foldername, tablename):
    iam_role = config.get('ARN')
    conn = get_redshift_connection()
    file_path = f's3://{tranbucket_name}/{tranbucket_name}/{foldername}/{file_name}.csv'
    copy_query = f"""
        COPY {tablename}
        FROM '{file_path}'
        IAM_ROLE '{iam_role}'
        CSV
        DELIMITER ','
        QUOTE '"'
        ACCEPTINVCHARS
        TIMEFORMAT 'auto'
        IGNOREHEADER 1;
    """
    try:
        execute_sql(copy_query, conn)
        print('Data successfully loaded to Redshift')
        logging.info('Data successfully loaded to Redshift')
    except Exception as e:
        print(f'Error loading data to Redshift: {e}')
        logging.error(f'Error loading data to Redshift: {e}')


