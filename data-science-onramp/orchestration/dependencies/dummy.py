#!/usr/bin/env python
# coding: utf-8

# # Feature engineering with pandas and scikit-learn
# 
# This notebook demonstrates how to use AI Platform notebooks to perform feature engineering on a dataset using Pandas.
# 
# For each dataset, you will load the data into a Pandas DataFrame, clean up the columns into a usable format, and then restructure the data into feature and target data columns.
# 
# Before you jump in, let's cover some of the different tools you'll be using:
# 
# + [AI Platform](https://cloud.google.com/ai-platform) consists of tools that allow machine learning developers and data scientists to run their ML projects quickly and cost-effectively.
# 
# + [Cloud Storage](https://cloud.google.com/storage/) is a unified object storage for developers and enterprises, from live data serving to data analytics/ML to data archiving.
# 
# + [Pandas](https://pandas.pydata.org/) is a data analysis and manipulation tool built on top of the Python programming language.
# 
# + [scikit-learn](https://scikit-learn.org/stable/) is a machine learning and data analysis tool for the Python programming language that provides simple and efficient tools to analyze or predict data.

# # Citibike Dataset
# 
# First, perform feature engineering on the [Citibike dataset](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=new_york_citibike&t=citibike_trips). This includes cleaning the data, extracting the necessary features, and transforming the data into feature columns.
# 
# ## Load the data
# 
# ### Import libraries
# Running the following cell will import the libraries needed to preprocess the Citibike dataset.
# 
# + **Pandas**: to store and manipulate the dataset
# + **Google Cloud Storage**: to retrieve the dataset from the GCS bucket where the dataset is stored
# + **os**: to retrieve environment variables

# In[94]:


import os, shutil
import pandas as pd

from google.cloud import storage


# ### Define constants
# Define the name of your Google Cloud Storage bucket where the cleaned data is stored.
# 
# + `PROJECT_ID`: unique identifier for your project
# + `BUCKET_NAME`: name of the bucket where the cleaned dataset is stored
# + `BLOB_PREFIX`: folder where the files are stored
# + `DIR_NAME`: name of the local folder where the files will be downloaded to

# In[115]:


PROJECT_ID = os.getenv('PROJECT_ID', 'data-science-onramp') # 'your-project-id')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'citibikevd') #'your-bucket-name')

BLOB_PREFIX = 'clean_data/'    
DIR_NAME = 'citibike_data'


# ### List the files
# 
# Run the following command to create a local folder where the dataset files will be stored.

# In[108]:

if os.path.exists(DIR_NAME):
    shutil.rmtree(DIR_NAME)
os.mkdir(DIR_NAME)  #!mkdir $DIR_NAME


# Since the data cleaning job outputted multiple partioned files into the GCS bucket, you will need to loop through each file to access its contents. The following cell will retrieve all of the files with the `BLOB_PREFIX` defined above and download them. It will also create a list of the file names so they can be referenced later when loading the data into a dataframe.

# In[117]:


# Create storage client
storage_client = storage.Client()

# List files in the bucket with the specified prefix
print(str(dir(storage_client)))
blobs = storage_client.list_blobs(BUCKET_NAME, prefix=BLOB_PREFIX)

# Go through the files and save them into the local folder
filenames = []
for i, blob in enumerate(blobs):
    filename = f'{DIR_NAME}/citibike{i}.csv.gz'
    blob.download_to_filename(filename)
    filenames.append(filename)
    print('Downloaded file: ' + str(blob.name))


# ### Load the files into a dataframe
# 
# Now, you can load the files into a dataframe.
# 
# First, define the schema. From this dataset, you will need 4 columns:
# 
# + **starttime**: to extract the day of the week and date of when the trip starts
# + **stoptime**: to extract the day of the week and date of when the trip has ended
# + **start_station_id**: to find out how many trips started at a station
# + **end_station_id**: to find out how many trips ended at a station

# In[110]:


COLUMNS = (
    'starttime',
    'stoptime',
    'start_station_id',
    'end_station_id',
)


# Next, run the following cell to loop through the files downloaded to the local folder, create a Pandas DataFrame, and view the first ten rows. The columns needed are the 1st, 2nd, 3rd, and 7th columns from left to right (starting with 0) when looking at the table in BigQuery.
# ![image.png](attachment:image.png)

# In[111]:


# Create empty dataframe
citibike_data = pd.DataFrame()

# For each file: load the contents into a dataframe
# and concatenate the new dataframe with the existing
for file in filenames:
    print('Processing file: ' + file)
    new_df = pd.read_csv(file, compression='gzip', usecols=[1, 2, 3, 7], header=None,
                         names=COLUMNS, low_memory=True)
    citibike_data = pd.concat([citibike_data, new_df])

citibike_data.head(10)
