import re 
import pandas as pd
import yaml
import boto3
import numpy as np
import sqlalchemy
import datetime 
from datetime import datetime
from dateutil.relativedelta import relativedelta


## function to load features dictionary 
## from amazon s3
def load_feature_lists(creds, name_features_list = 'features_dictionary.yaml'):
    
    ## establish client and download file from s3
    ## first establish an s3 client
    s3_client = boto3.client('s3',
        aws_access_key_id=creds['aws_s3']['access_key_id'],
        aws_secret_access_key=creds['aws_s3']['secret_access_key'])

    ## then, download file
    s3_client.download_file('dsapp-economic-development', 
                    'nyc_peu_inspections/data_backups/{filename}'.format(filename = name_features_list), 
                    'downloaded_features_dict.yaml')
    
    
    ## load yaml file
    with open('downloaded_features_dict.yaml','r') as stream: 
        feature_lists = yaml.load(stream)
        
    print("loaded features dictionary")    
        
    return(feature_lists) 

### function to combine individual feature lists into a 
### larger feature list
def create_featurelist_touse(features_dictionary, list_keys):
    
    ## subset dictionary to those keys
    feature_list = []
    for key in list_keys:
        one_list = features_dictionary[key]
        feature_list.extend(one_list)
    return(feature_list)

    

## function to read in training 
## and test set
## simplified from 
## previous code
## by automatically selecting
## training obs with obs label
## and test obs in target zip (rather than allowing
## buildings outside etc; can expand if we decide to)
## function to read in training 
## and test set
## simplified from 
## previous code
## by automatically selecting
## training obs with obs label
## and test obs in target zip (rather than allowing
## buildings outside etc; can expand if we decide to)
def select_features_and_labels(label_type,
                    label_name,
                    feature_list,
                    creds,
                    features_dictionary,
                    split_date,
                    alchemy_connection,
                    readquery_todf_postgres):
    
    # get names of tables
    ## name of test table: split date + 1 month
    test_end_date = datetime.strptime(split_date, "%Y-%m-%d")  + relativedelta(months = 1)
    test_end_date_str = test_end_date.strftime('%Y-%m-%d')
    
    ## create variable for label
    if label_type == 'binary':
        labels = features_dictionary['internal_binary_labels']

    elif label_type == 'continuous':
        labels = features_dictionary['internal_continuous_labels']
        
    elif label_type == 'ratio':
        labels = features_dictionary['internal_ratio_labels']

    else:
        print('tell function whether to choose binary or continuous labels')
    
    ## if not selecting feature cols according to a pattern,
    ## use feature_list for list of features
    sql_additional_features_list = ['month_start','internal_borough_factor_static'] + labels + feature_list 
    
    ## join list of features to select
    sql_additional_features_string = ', '.join(sql_additional_features_list)
    
    ## create base of query regardless of whether subsetting rows
    train_query_observed = """
                    {sql_base_query} {sql_additional_features_string} 
                    from dssg_staging.staging_monthly_split_20160301_20180201 
                    where {label_name} is not null
                    and month_start <= '{split_date}'
                    """.format(sql_base_query = 'select address_id, internal_units_static, ',
                               sql_additional_features_string = sql_additional_features_string,
                              label_name = label_name,
                              split_date = split_date)
    
    df_train = readquery_todf_postgres(sqlalchemy.text(train_query_observed),
                                      alchemy_connection)
    
    test_query_targetzip = """
                    {sql_base_query} {sql_additional_features_string} 
                    from dssg_staging.staging_monthly_split_20160301_20180201 
                    where internal_peu_target_zip = 1 
                    and month_start > '{split_date}'
                    and month_start <= '{test_set_date}'
                    """.format(sql_base_query = 'select address_id, internal_units_static, ',
                               sql_additional_features_string = sql_additional_features_string,
                            split_date = split_date,
                              test_set_date = test_end_date_str)
    
    df_test = readquery_todf_postgres(sqlalchemy.text(test_query_targetzip),
                                      alchemy_connection)
    
    
            
    return(df_train, df_test)
