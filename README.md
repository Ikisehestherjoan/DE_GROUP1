# Project Title
10ALYTICS_JOB_BOARD
## Introduction

This project aims to build a data pipeline for extracting daily job posting data from the (https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch/). The extracted data is filtered for Data Engineer and Data Analyst jobs posted in the UK, Canada, or the US. The pipeline then stages the raw data in an Amazon S3 bucket and transforms it before loading it into another S3 bucket and eventually into an Amazon Redshift data warehouse.

## Project Structure

The project is organized into the following files:

1. **etl.py**: This file handles the extraction, transformation, and loading processes.
2. **util.py**: Contains utility functions, including database connections.
3. **main.py**: The main program where the entire pipeline can be run from.

## Pipeline Steps

### 1. Design Pipeline Architecture

Include a pipeline architecture diagram illustrating the flow of data from the source to the destination.

### 2. Extract Raw Data

The pipeline extracts raw data for Data Engineer and Data Analyst jobs posted in the UK, Canada, or the US from the LetScrape JSearch API.

### 3. Stage Raw Data

Raw data is staged in an Amazon S3 bucket named "raw_jobs_data" and saved in JSON format.

### 4. Transform and Load Data

The pipeline transforms the raw data and loads it into another S3 bucket named "transformed_data" in CSV format. The transformed data includes specific columns: employer_website, job_id, job_employment_type, job_title, job_apply_link, job_city, job_country, job_posted_at_timestamp, employer_company_type.

### 5. Load into Amazon Redshift

Data from the "transformed_jobs_data" bucket is pulled and loaded into an Amazon Redshift data warehouse.

### 6. Schedule Pipeline with Apache Airflow

The pipeline will be scheduled to run once a day using Apache Airflow for automation.

## Project Requirements

- An account on the Rapid API website is required to access the LetScrape JSearch API.
- Credentials and configurations for Amazon S3 and Amazon Redshift are necessary. Ensure that these are kept secure.

## Running the Code

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/your-repository.git
   cd your-repository

