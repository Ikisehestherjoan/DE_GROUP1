# Import libraries
from time import sleep
from datetime import datetime
from util import generate_schema, execute_sql,write_to_s3
from etl import get_api_data, load_to_redshift, transformed_data_fnx

# Main method to run the pipeline
def main():
    bucket_name = 'transjobdata'
    tablename = 'job_data'
    folder_name ='data'
    file_name='transformed_data'
    counter = 0
    # # A while loop to send 5 requests to the API
    # while counter < 3:
    #     # data =get_api_data() # Extract data from API
    #     transformed_data_1 = transformed_data_fnx() # Transform the data
    #     write_to_s3(transformed_data_1)
    #     counter+= 1
    #     sleep(10) # Wait 30 seconds before sending another request to the API
    # print('API data pulled and written written to s3 bucket')

    # # create_table_query = generate_schema(data, tablename) # generate ddl of target table
    # # Create a taget table for in Redshift
    load_to_redshift(bucket_name,file_name, folder_name, tablename)
    
    
main()