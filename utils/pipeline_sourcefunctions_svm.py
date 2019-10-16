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
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/load_creds_file.py?token=ATPULU_lX8TPLlwd3u3XxV5lUbgJ7t2vks5bda79wA%3D%3D').text)

## function to split data into training and testing sets
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/split_train_test.py?token=ATPULW8J1Ih80ZIG23nO6PB-HoYG1ZPiks5bda8OwA%3D%3D').text)

## function to read metadata from redshift columns
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/get_cols_intable_redshift.py?token=ATPULWmC-F_34zhHSQI_teGqoc8knQYgks5bda8awA%3D%3D').text)

## functions related to choosing features
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/choosingfeatures_functions.py?token=ATPULRKzubhhCr97jNchvK492kOEW01Cks5bda8swA%3D%3D').text)

## function to load and update config file
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/update_master_config.py?token=ATPULXs_t2guEjjdKvFYw5-5q8zUQmauks5bda87wA%3D%3D').text)

## pre-processing functions: label imputation, feature imputation, and normalization
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/preprocessing_functions.py?token=ATPULR3GMd4C4D48pc_sBjG0q_jrVDASks5bda9UwA%3D%3D').text)

## get feature var_types
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/get_feature_var_types.py?token=ATPULawGaEAveZsbrlA1dt9K8CDRvVFYks5bda9vwA%3D%3D').text)

## function to generate uuid/store hdf backup
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/store_civisS3_logID.py?token=ATPULfFnLW98sV0vjQQL35z1ZM-HgVT4ks5bdbCWwA%3D%3D').text)

## functions to create and check existence of rows
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/create_andcheck_rows.py?token=ATPULdoIQd7w4w53PYMhwaUegwYWnRRjks5bda-MwA%3D%3D').text)

## functions for sql operations
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/sql_create_insert_retrieve_functions.py?token=ATPULWq8zzlqUJNv2cZUXY7GREXcs3w5ks5bda-bwA%3D%3D').text)

## function for top k
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/top_k_unit_prediction_generation.py?token=ATPULevdawNRv9rSwNgBgXNvyWWYs_Vhks5bda-zwA%3D%3D').text)

## functions to run models
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/runmodels_returnresults_for_svc.py?token=ALo-oWs0J_S-j5wn0ng4zbGseu1WDAh_ks5bdmWewA%3D%3D').text)

## functions to generate graphs from the results schema
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/results_graphing_functions.py?token=ATPULc5c7pFLqPCibxWWnrii71cDpM0Nks5bda_EwA%3D%3D').text)


## functions to query the results schema
exec(requests.get('https://raw.githubusercontent.com/dssg/nyc_peu_inspections/master/utils/results_querying_functions.py?token=AUYR5EME26Rc_cu9nJo830PvXFdQq6UTks5bdb2GwA%3D%3D').text)
