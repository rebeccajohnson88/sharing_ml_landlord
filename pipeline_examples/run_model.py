
#!/usr/bin/env python
# coding: utf-8

### Import packages
import random
import warnings
import pandas as pd
import numpy as np
import sys
import logging
import json

from datetime import datetime
from dateutil.relativedelta import relativedelta


# import for database connection
import sqlalchemy
from sqlalchemy.types import *
import sqlalchemy.types as sql_types
 
# import for train
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression

## import functions 
sys.path.append('/home/yet/nyc_peu_inspections/utils/') 
from load_creds_file import *
from postgres_functions import *
from savebackups_S3 import *
from choosingfeatures_functions import *
from preprocessing_functions import *
from get_feature_var_types import *
from update_master_config import *


## log file
dataname = 'test_runmodels_100ish'
log_filename = dataname + "_logfile_run.log"
logging.basicConfig(level=logging.INFO, filename = log_filename)
log = logging.getLogger(__name__)

# load creds
creds = load_creds_file('/home/yet/credentials_ty.yaml')
warnings.filterwarnings("ignore")



random.seed(20190307)
with open('/home/yet/nyc_peu_inspections/pipeline/models/experiments/parameters_0307.yaml','r') as stream:
        parameters = yaml.load(stream)

locals().update(parameters)
parameter_names = [key[0] for key in parameters.items()]
update_master_config_new(creds = creds,
                     parameter_names = parameter_names,
                     parameters = parameters,
                     update_config = param_update_config,
                     return_all_config = param_return_all_config,
                    simpler_loading = True)


log.info('updated master config')


models_dictionary = download_backups_s3('models_dict.yaml', creds,
                                      filetype_string = 'yaml')
model_list = models_dictionary[param_model_list]
## initiate cursor and alchemy connection
cursor, psy_connect, alchemy_connection = startengine_alchemy(creds = creds, 
                                                              return_raw_connection = True)


## get model_group_table and models_table columns
modelgroup_table = 'model_group'
model_group_columns_to_insert = get_colnames_fromtable_postgres(schema_name = 'dssg_results',
                                                               table_name = modelgroup_table,
                                                               cursor = cursor)
# model_group_columns_to_insert.remove('model_group_id')
model_group_columns_to_insert = [col for col in model_group_columns_to_insert if col != 'model_group_id']
model_group_columns_to_insert = ', '.join(model_group_columns_to_insert)


models_table = 'models'
models_columns_to_insert = get_colnames_fromtable_postgres(schema_name = 'dssg_results',
                                                               table_name = models_table,
                                                               cursor = cursor)
# models_columns_to_insert.remove('model_id')
models_columns_to_insert = [col for col in models_columns_to_insert if col != 'model_id']
models_columns_to_insert = ', '.join(models_columns_to_insert)


tables_available = all_staging_tables(creds, cursor, param_primary_label, param_label_quantile_threshold)
schema_suffix = convert_labels_toschema(param_primary_label, param_label_quantile_threshold)
split_dates = return_split_dates(tables_available)


for one_model in model_list:
    log.info("estimating model:" + str(one_model))
    ### insert into model group table
    (model_group_values_to_insert,model_group_values_to_check) = create_modelgroup_row(
                                                model_call = one_model,
                                                param_features_to_pull = param_features_to_pull,
                                                label_name = param_primary_label,
                                                borough_fit_list = param_borough_fit_list)
     ### pass value to schematable_insert for sql_insert_function
    model_group_id = sql_insert_modelgroup(schematable_insert='dssg_results.model_group',
                                           columns_to_insert= model_group_columns_to_insert,
                                           values_to_insert= model_group_values_to_insert,
                                           alchemy_connection = alchemy_connection)
    for split_date in split_dates:
        log.info('running on split date: {split_date}'.format(split_date=split_date))

        train_filename = "{trainortest}_split_{splitdate}_features_{featurelist}".format(trainortest = 'train',
                                                                                             splitdate = split_date
                                                                                        ,featurelist=param_features_to_pull)
        test_filename = "{trainortest}_split_{splitdate}_features_{featurelist}".format(trainortest = 'test',
                                                                                              splitdate = split_date
                                                                                       ,featurelist=param_features_to_pull)   
        ## read in training and test data
        training_features_df = readtable_postgres(tablename = train_filename,
                                 schemaname = 'dssg_staging_' + schema_suffix,
                                 alchemy_connection = alchemy_connection)
        # training_features_df.head()
        testing_features_df = readtable_postgres(tablename = test_filename,
                                 schemaname = 'dssg_staging_' + schema_suffix,
                                 alchemy_connection = alchemy_connection)



        # testing_features_df.head()
        feature_intersect = list(set(training_features_df.columns).difference(set(['address_id',
                                               'month_start', param_primary_label])))
        ## create matrix of training features (removed reshape command)
        x_train_features = training_features_df[feature_intersect]
        ## create matrix of test features
        x_test_features = testing_features_df[feature_intersect]
        ## create training and test labels
        label_train = training_features_df[param_primary_label]
        label_test = testing_features_df[param_primary_label]

        ### top k
        top_k_list = readtable_postgres(tablename = 'top_k_by_month',
                                 schemaname = 'dssg_staging',
                                 alchemy_connection = alchemy_connection)
        top_k_splitdate = top_k_list[top_k_list.month_start==split_date].units_canvassed.values[0] //2


        run_one_model(one_model,x_train_features,label_train,x_test_features,
                     param_primary_label,param_borough_fit_list,alchemy_connection,
                     feature_intersect,train_filename,param_label_imputation_method,
                     top_k_splitdate, param_borough_predict_list,test_filename
                    ,model_group_id,models_columns_to_insert
                        ,testing_features_df,training_features_df)
    
    
log.info("Script finished" )    
    





