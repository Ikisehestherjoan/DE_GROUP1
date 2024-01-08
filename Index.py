import pandas as pd
import boto3
import psycopg2
import redshift_connector as rds
from botocore.exceptions import NoCredentialsError
import requests
from configparser import ConfigParser

config = ConfigParser()
config.read('.env')

def extract_data():
    try:
        url = "https://jsearch.p.rapidapi.com/search"

        querystring = {
            "query": "Data Analyst, Data Engineer, Data Scientist, Machine Learning",
              "date_posted":"today",
              "remote_jobs_only":"fa