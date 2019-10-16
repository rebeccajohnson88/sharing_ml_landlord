

###################################### Import packages ##################################
import os
import yaml
import logging
import requests
import pandas as pd
import json
import re
import sodapy
from sodapy import Socrata
import retrying
from retrying import retry
from multiprocessing import Pool
import psycopg2
from sqlalchemy import *
import sys


###################################### Import functions ##################################
sys.path.append('/home/johnsonr/nyc_peu_inspections/utils/') 
from load_creds_file import *
from postgres_functions import *
from etl_functions import *

## define separately because
## retry causes issues with importation
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
def getdf_fromapi_onechunk(offset_start):
    one_get = client_data.get(jsonid_forget, limit = 10000,
                             offset = offset_start) # call
    one_df = pd.DataFrame.from_records(one_get)
    return(one_df)


################################ Specify dataname and start log
dataname = 'housingcourt_litigation'
creds = load_creds_file('/home/johnsonr/credentials_rj.yaml')
log_filename = dataname + "_logfile_run.log"
logging.basicConfig(level=logging.INFO, filename = log_filename)
logger = logging.getLogger(__name__)


################################ Loading and cleaning NYC open data links (all links) #################
sheet_string = 'https://docs.google.com/spreadsheets/d/19HRsZ4iDgndHFDc8PYSq_dIsQnVYSQjdpsozlE0wNnc/export?format=csv'
opendata_links = pd.read_csv(sheet_string)

## subset to ones where apilink != 0
## making copy to avoid copy error
## warning when we add the new col below
opendata_links_valid = opendata_links[(opendata_links['apilink'] != 'no') &
                                (opendata_links['include'] == "Yes")].copy()


opendata_links_valid['json_id'] = opendata_links_valid.apilink.apply(lambda x:
                                    re.search("(resource\\/)(.*)(\\.json)", x).group(2))

logger.info("generated json id")



######### Choosing data to pull (onedata),  cleaning inputs to .get call, and call ##################

## for now, didnt use credentials but later change
client_data = Socrata("data.cityofnewyork.us",
                    creds['nyc_api_getdata']['apikey'],
                    username = creds['nyc_api_getdata']['username'],
                    password = creds['nyc_api_getdata']['password'])
client_metadata = Socrata("data.cityofnewyork.us",
                    creds['nyc_api_getmetadata']['apikey'],
                    username = creds['nyc_api_getmetadata']['username'],
                    password = creds['nyc_api_getmetadata']['password'])


### cleaned version of json ID
jsonid_forget = clean_json(data = opendata_links_valid,
                           dataname = dataname)

### limit parameter - either feeds it
### approximate size if data has unique ID
### or uses the approximate size to get row limit
if int(opendata_links_valid[opendata_links_valid.dataname == dataname]['unique_id']) == 1:
    limit_parameter = generate_limit_parameter(data = opendata_links_valid,
            dataname = dataname, client_metadata = client_metadata)
else:
    limit_parameter = int(opendata_links_valid[opendata_links_valid.dataname == dataname]['size_approx']) + 10000


logger.info("generated lim parameter: " + str(limit_parameter) + " for: " + dataname)


## generate chunks of data to pool and pull from API in parallel
offset_range = list(range(0, limit_parameter, 10000))
print(len(offset_range))
pool = Pool() # Create a multiprocessing Pool; could test with 1 to test sequential version and put time buffer
print('started pool')
df_list = pool.map(getdf_fromapi_onechunk, offset_range)


## create dataframe version of the df
all_chunks = pd.concat(df_list)
logger.info("concatenated data into one df")


alchemy_connection = startengine_alchemy(creds)


### write to data
all_chunks.to_sql('housingcourt_litigation', 
                  alchemy_connection, 
                  schema=creds['schema_raw'], 
                  if_exists='replace', 
                  index=False)

logger.info("wrote data to postgres")

