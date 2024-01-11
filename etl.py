import pandas as pd
import requests
import json
import boto3
import psycopg2
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
def create_database_conn():
    try:
        # Load environment variables from .env file
        config = dict(dotenv_values('.env'))
        Access_key = config.get('ACCESS_KEY')
        Secret_key = config.get('SECRET_KEY')
        bucket_name = config.get('BUCKET_NAME')
        transform_bucket_name = config.get('TRANSFORMED_DATA')
        region = config.get('REGION')
        

        # Create an S3 client
        s3 = boto3.client(
            's3',
            aws_access_key_id=Access_key,
            aws_secret_access_key=Secret_key,
            region_name=region
        )
        
        return s3, bucket_name, region

    except Exception as e:
        print(f"Error creating S3 client: {e}")
        return None, None, None
        
#     #======BUCKET CREATION=====

def create_bucket():
    try:
        s3, bucket_name, region = create_database_conn()

        # Check if the bucket already exists
        response = s3.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]

        if bucket_name not in buckets:
            # Create an S3 bucket if it doesn't exist
            s3.create_bucket(Bucket=bucket_name, 
                            CreateBucketConfiguration={'LocationConstraint': region}
            )
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")

    except NoCredentialsError:
        print("Credentials not available or incorrect.")
    except Exception as e:
        print(f"Error: {e}")


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
def transformed_data():
    # Specify the path to your JSON file and CSV file
    json_file_path = 'data/raw_data.json'
    csv_file_path = 'data/transformed_1_data.csv'

    # Initialize an empty list to store JSON objects
    records = []
    required_data = ['employer_website', 'job_id', 'job_employment_type', 'job_title', 'job_apply_link',
                    'job_description', 'job_city', 'job_country', 'job_posted_at_timestamp', 'employer_company_type']

    # Initialize an empty DataFrame
    df = pd.DataFrame(columns=required_data)

    # Read the JSON file line by line
    with open(json_file_path, 'r') as xrates_file:
        for line in xrates_file:
            try:
                # Load JSON data for each line
                record = json.loads(line)
                records.append(record)

                # Access required data from the JSON object
                data_to_append = {key: record[key] for key in required_data}
                df = df.append(data_to_append, ignore_index=True)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON on line: {line}")
                print(f"Error details: {e}")

    # Print the DataFrame
    #print(df.head())
    # ===========TRANSFORMATION STAGE=========
    df['job_posted_at_timestamp'] = pd.to_datetime(df['job_posted_at_timestamp']).dt.date
    # print(df['job_posted_at_timestamp'].head())

    # Save the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False)
    print(f"Data has been saved to {csv_file_path}")

transformed_data()







