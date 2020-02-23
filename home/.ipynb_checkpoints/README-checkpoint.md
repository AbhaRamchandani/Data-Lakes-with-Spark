Completed: 4-Aug-2019

# Project: Build an ETL pipeline for a database hosted on Redshift
Load data from S3, process the data into analytics tables using Spark, and load them back into S3.

# Available information
#1 - Dataset: 2 datasets that reside in S3:
     - Song data: s3a://udacity-dend/song-data
     - Log data: s3a://udacity-dend/log-data

#2 - Template files
     The project template includes 4 files:
     - etl.py is where you'll load data from S3 into parquet files on S3 using Spark.
     - dl.cfg contains your AWS credentials
     - README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.

# Setup Instructions
For this project, I used the Udacity workspace.
 . Create an IAM role with full access to S3
 . Download and save the credentials.csv generated while creating the IAM role.
 . Add the Access and Secret keys to dl.cfg file (without quotes).
 . Create an S3 bucket with public access. I created project4-spark. This bucket can be used, however, if the person executing this program creates his own bucket, he must update the output data path in etl.py and/or etl.ipynb.
 
# Program execution
 Note: I created an etl.ipynb file to execute and test each step of the process. So, there are 2 ways to execute this program.
 ## Alternative 1
 (1) Open the terminal window and execute: python create_tables.py
 (2) To test the execution, you can run a few queries in the etl-ipynb notebook. I, for example,  ran the following query to check the ETL:
     - parquetFile = spark.read.parquet("s3a://project4-spark/songplays/*/*/*.parquet")
       parquetFile.show(5)
 ## Alternative 2
 (1) Open etl.ipynb and execute each line of the Notebook. There are 22 steps.
 (2) To test the execution, you can run a few queries in the etl-ipynb notebook. I, for example,  ran the following query to check the ETL:
     - parquetFile = spark.read.parquet("s3a://project4-spark/songplays/*/*/*.parquet")
       parquetFile.show(5)
    
# Testing the program
To test the execution, I, for example,  ran the following query to check the ETL:
     - parquetFile = spark.read.parquet("s3a://project4-spark/songplays/*/*/*.parquet")
       parquetFile.show(5)
       
### NOTE: You might want to delete the data folders created under output_data. Otherwise you'll get the error - 'AnalysisException: 'path *** already exists.;''. This error just means that the destination path for data is already there in the target location. This error is acceptable and can be ignored if we have executed the program once and the data is loaded successfully. However, if the data failed to load successfully in the previous attempts, it is recommented to delete these folders.

# Reference
(1) Udacity lessons
(2) Udacity Slack channels and mentor
(3) https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
(4) http://pep8online.com/