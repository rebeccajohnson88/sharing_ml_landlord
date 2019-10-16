import pip
import os
import requests
import yaml
import logging
import random
import pip
import time
import json
import requests
import warnings
import datetime
import dateutil.relativedelta
from dateutil.relativedelta import *

import pandas as pd
import numpy as np
from sklearn.metrics import confusion_matrix, fbeta_score, precision_score, recall_score, accuracy_score, roc_auc_score, average_precision_score, make_scorer
import civis
import hashlib
import boto3

pip.main(['install', 'sodapy'])
pip.main(['install', 'tables'])
pip.main(['install', 'retrying'])
import tables
from retrying import retry
from sodapy import Socrata

from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.feature_selection import SelectFromModel
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier, GradientBoostingClassifier, AdaBoostClassifier
from sklearn.linear_model import RidgeClassifier
from sklearn.linear_model import Lasso
from sklearn.linear_model import LassoCV
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LogisticRegressionCV
from sklearn.linear_model import RidgeClassifierCV
from sklearn.linear_model import PassiveAggressiveClassifier
from sklearn.linear_model import SGDClassifier

## function to get credentials
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/load_creds_file.py?token=ALo-oa4sOccEKbT0AXnQR67ubRS1hIqgks5bkwh0wA%3D%3D').text)

## functions for ETL
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/etl_functions.py?token=ALo-oengPNI9KTDH2EnjGBeazTkSLCWLks5bltPhwA%3D%3D').text)

## function to split data into training and testing sets
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/split_train_test.py?token=ALo-oahWBcVZR_unxs9rS_Q36obcNWNVks5bkwibwA%3D%3D').text)

## function to read metadata from redshift columns
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/get_cols_intable_redshift.py?token=ALo-oVbFPL1Mm0yQPTUKlGdd5k3fjkvrks5bkwjqwA%3D%3D').text)

## functions related to choosing features
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/choosingfeatures_functions.py?token=ALo-oRedLj_kHgCYhQqjpTLV2nM7fhClks5bkwk-wA%3D%3D').text)

## function to load and update config file
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/update_master_config.py?token=ALo-oU-CK34mrdpys8K1OrUzqycb0jhuks5bltN3wA%3D%3D').text)

## pre-processing functions: label imputation, feature imputation, and normalization
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/preprocessing_functions.py?token=ALo-ocsH41ZC7GcRneJz7blqhyU1AMAVks5bkwmCwA%3D%3D').text)

## get feature var_types
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/get_feature_var_types.py?token=ALo-oexfYLxbzDKqqgsjydcU7FJMcBCtks5bkwmnwA%3D%3D').text)

## function to generate uuid/store hdf backup
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/store_civisS3_logID.py?token=ALo-oVhje78rOwtgmUpcLy_aZmq2zLwZks5bkwnBwA%3D%3D').text)

## functions to create and check existence of rows
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/create_andcheck_rows.py?token=ALo-oRWSY0m0tYyhb7G9it1hWSftIJAxks5bkwnhwA%3D%3D').text)

## functions for sql operations
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/sql_create_insert_retrieve_functions.py?token=ALo-oYP8F69v5pBu7fPRjJ0q7b80dcBSks5bkwn_wA%3D%3D').text)

## function for top k
#exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/top_k_unit_prediction_generation.py?token=ATPULS4GiijqJBqCgV6vJZHlP4qWR4TOks5bgCHSwA%3D%3D').text)

## functions to run models
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/runmodels_returnresults.py?token=ALo-oR8MfSpxBPxdabBan8gV5mekgmaDks5bkwojwA%3D%3D').text)

## functions to generate graphs from the results schema
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/results_graphing_functions.py?token=ALo-oVM1poYaOMs0fFm2ostpBHXdx5ofks5bkwo9wA%3D%3D').text)

## functions to query the results schema
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/results_querying_functions.py?token=ALo-oYr389-O7uenAmzMo8nnYPy35qOoks5bkwpXwA%3D%3D').text)

## functions to summarize model performance
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/results_findbest_functions.py?token=ALo-oe_fuYEFeCe5iMucKsOGBghHJS_jks5bkwpzwA%3D%3D').text)

