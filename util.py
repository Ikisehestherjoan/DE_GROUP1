import boto3
import psycopg2
import pandas as pd
from dotenv import dotenv_values
from botocore.exceptions import NoCredentialsError
import logging
dotenv_values()


# Get credentials from environment variable file
config = dotenv_values('.env')

# Create a boto3 s3 client for bucket operations
s3_client = boto3.client('s3')

# function to get Reshift data warehouse connection
def get_redshift_connection():
    iam_role = config.get('ARN')
    user = config.get('USER')
    password = config.get('PASSWORD')
    host = config.get('HOST')
    database_name = config.get('DATABASE_NAME')
    port = config.get('PORT')
    conn = psycopg2.connect(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')
    return conn

def execute_sql(query, conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()
        logging.info(f'Successfully executed SQL query: {query}')
    except Exception as e:
        conn.rollback()
        logging.error(f'Error executing SQL query: {query}. Error: {e}')
        raise

def read_local_csv(file_data):
    csv_data = pd.read_csv(file_data)
    return csv_data
        
def execute_sql(query, conn):
    # Placeholder for executing SQL queries (replace with your logic)
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()

def generate_schema(data, table_name ='job_data'):
    create_table_statement = f'CREATE TABLE IF NOT EXISTS {table_name}(\n'
    column_type_query = ''
    
    types_checker = {
        'INT':pd.api.types.is_integer_dtype,
        'VARCHAR':pd.api.types.is_string_dtype,
        'FLOAT':pd.api.types.is_float_dtype,
        'TIMESTAMP':pd.api.types.is_datetime64_any_dtype,
        'OBJECT':pd.api.types.is_dict_like,
        'ARRAY':pd.api.types.is_list_like,
    }
    for column in data: # Iterate through all the columns in the dataframe
        last_column = list(data.columns)[-1] # Get the name of the last column
        for type_ in types_checker: 
            mapped = False
            if types_checker[type_](data[column]): # Check each column against data types in the type_checker dictionary
                mapped = True # A variable to store True of False if there's type is found. Will be used to raise an exception if type not found
                if column != last_column: # Check if the column we're checking its type is the last comlumn
                    column_type_query += f'{column} {type_},\n' # 
                else:
                    column_type_query += f'{column} {type_}\n'
                break
        if not mapped:
            raise ('Type not found')
    column_type_query += ');'
    output_query = create_table_statement + column_type_query
    return output_query
data = read_local_csv('data/transformed_data.csv')
query =generate_schema(data)
# print(query) # this generate the schema


def create_database_conn():
    try:
        # Create an S3 client
        s3 = boto3.client(
            's3',
            aws_access_key_id=config.get('ACCESS_KEY'),
            aws_secret_access_key=config.get('SECRET_KEY'),
            region_name=config.get('REGION')
        )
        return s3, config.get('BUCKET_NAME'), config.get('REGION')
    except Exception as e:
        print(f"Error creating S3 client: {e}")
        return None, None, None

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

def create_transformed_bucket():
    transform_bucket_name ='transjobdata'
    s3, bucket_name, region = create_database_conn()
    try:
        # Check if the bucket already exists
        response = s3.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        
        if transform_bucket_name not in buckets:
            # Create an S3 bucket if it doesn't exist
            s3.create_bucket(Bucket=transform_bucket_name, 
                            CreateBucketConfiguration={'LocationConstraint': region}
            )
            print(f"Bucket '{transform_bucket_name}' created successfully.")
        else:
            print(f"Bucket '{transform_bucket_name}' already exists.")

    except NoCredentialsError:
        print("Credentials not available or incorrect.")
    except Exception as e:
        print(f"Error: {e}")

def write_to_s3(data):
    transform_bucket_name='transjobdata'
    try:
    
        s3, bucket_name, region = create_database_conn()

        # Specify the local path to your JSON file
        local_file_path = 'data/transformed_data.csv'

        # Specify the S3 key (file name in the S3 bucket)
        s3_key = f'{transform_bucket_name}/{local_file_path}'

        # Upload the JSON file to S3
        s3.upload_file(local_file_path, transform_bucket_name, s3_key)
        print(f"JSON file '{local_file_path}' uploaded to S3 bucket '{transform_bucket_name}' as '{s3_key}'.")
    except NoCredentialsError:
        print("Credentials not available or incorrect.")
    except Exception as e:
        print(f"Error: {e}")