

###### Packages

import yaml
import boto3
import pandas as pd
import re
import numpy as np
import sys
import logging 
import random


###################################### Import functions ##################################
sys.path.append('/home/johnsonr/nyc_peu_inspections/utils/') 
from load_creds_file import *
from postgres_functions import *
from savebackups_S3 import *
from choosingfeatures_functions import *



################################ Specify dataname and start log
dataname = 'feature_lists'
creds = load_creds_file('/home/johnsonr/credentials_rj.yaml')
log_filename = dataname + "_logfile_run.log"
logging.basicConfig(level=logging.INFO, filename = log_filename)
logger = logging.getLogger(__name__)



################################ Start connection and read in entities data for now (eventually replace w/ staging)
cursor, psy_connect, alchemy_connection = startengine_alchemy(creds = creds, return_raw_connection = True)
feature_col_df = pd.DataFrame(get_colnames_fromtable_postgres(schema_name = creds['schema_staging'], 
                                table_name = 'staging_monthly_split_20160301_20160501', 
                                cursor = cursor))
feature_col_df.columns = ['column']



################################ Read in names of non-features and exclude

columns_nonfeatures_sheet = 'https://docs.google.com/spreadsheets/d/1h_KYcnJcxGoPDH2Loci2sx76lZda4Kife3BJJA1CLXk/export?format=csv'

columns_nonfeatures = pd.read_csv(columns_nonfeatures_sheet)
columns_nonfeatures['feature_toshield'] = columns_nonfeatures.prefix + "_" + columns_nonfeatures.middle + "_" + columns_nonfeatures.suffix
features_toshield_list = list(columns_nonfeatures['feature_toshield']) + ['month_year_tomerge_exact', 'year_tomerge_rounded', 'address_id']
feature_col_df['feature_toshield'] = np.where(feature_col_df.column.isin(features_toshield_list), 1, 0)
                                              
print('shielded features')

features_ready = feature_col_df.column[feature_col_df.feature_toshield != 1]

## iterate over patterns and choose the cols
pattern_list = ['^internal.*', '^internal.*static',
                '^internal.*count_next.*',
                '^internal.*any_next.*',
                '.*cases.*this.*|.*knocks.*this.*|.*opens.*this.*',
               '^hpd.*', '^housinglitig.*',
               '^pluto.*', 
                '^acs_tract_.*',
                '^acs_tract_percent.*',
               '^subs|^rent.*']

names_list = ['internal_all', 'internal_static',
              'internal_continuous_labels',
              'internal_binary_labels',
              'internal_lagged_labels',
               'hpd', 'housing_litigation',
               'pluto', 'acs_all',
              'acs_percent',
               'misc']

## store in dictionary
feature_list_dict = {}
for i in range(0, len(pattern_list)):
    one_pattern = pattern_list[i]
    one_name = names_list[i]
    feature_list_dict[one_name] = choosecols_usingpattern(one_pattern,
                     col_list = features_ready)
    
    

### create different combinations

all_labels = feature_list_dict['internal_binary_labels'] + feature_list_dict['internal_continuous_labels'] + feature_list_dict['internal_lagged_labels']
non_features = all_labels + ['month_start',  'month_end', 'month','year']
internal_dynamic = set(feature_list_dict['internal_all']).difference(set(feature_list_dict['internal_static']))
internal_dynamic_nolabels = set(internal_dynamic).difference(set(all_labels))
allfeatures_nolabels = set(features_ready).difference(set(non_features))
# add to the dictionary
feature_list_dict['internal_dynamic_nolabels_orlaggedlabels'] = list(internal_dynamic_nolabels)
feature_list_dict['allfeatures_nolabels_orlaggedlabels'] = list(allfeatures_nolabels)
## lists to choose from
dict_tochoose = {key:val for key, val in feature_list_dict.items() if key != 'internal_all' and 
                 key != 'internal_continuous_labels' and
                                                            key != 'internal_binary_labels' and
                                                            key != 'internal_lagged_labels' and 
                                                            key != 'allfeatures_nolabels_orlaggedlabels'}
# rename to add new
features_dictionary = feature_list_dict

## NEW: add combinations of different feature sets
### external viols
external_violations_keys = ['hpd', 'housing_litigation']
external_violations_list = create_featurelist_touse(features_dictionary, 
                      external_violations_keys)

features_dictionary['external_violations'] = external_violations_list

## clean up internal static section
internal_static_list = features_dictionary['internal_static']
exclude_internalstatic_mid = ['bbl', 'bin', 'county', 'census_block_group', 'census_block', 'census_tract']
exclude_internalstatic_full = ["internal_" + s + "_static" for s in exclude_internalstatic_mid]

## internal static final is set difference
internal_static_final = list(set(internal_static_list).difference(set(exclude_internalstatic_mid)))
features_dictionary['internal_static'] = internal_static_final 

## removed acs subsetting


### internal only
internal_only_keys = ['internal_static', 'internal_lagged_labels']
internal_only_list = create_featurelist_touse(features_dictionary, internal_only_keys)
features_dictionary['internal_only'] = internal_only_list

## all external
### add acs that are neither counts nor percents (so the median rent ones)
all_external_keys = external_violations_keys + ['pluto',  'misc', 'acs_all'] 
all_external_list = create_featurelist_touse(features_dictionary, all_external_keys)
features_dictionary['all_external'] = all_external_list


## all features minus ACS
all_features_keys = ['allfeatures_nolabels_orlaggedlabels', 'internal_lagged_labels']
all_features_list = create_featurelist_touse(features_dictionary, all_features_keys)
all_features_minusACS_list = list(set(all_features_list).difference(set(features_dictionary['acs_all'])))


features_dictionary['all_features_minusACS'] = all_features_minusACS_list


## add acs selected
acs_selected = ['acs_tract_median_age_all',
'acs_tract_percent_white_alone',
 'acs_tract_percent_black_or_african_american_alone',
 'acs_tract_percent_american_indian_and_alaska_native_alone',
 'acs_tract_percent_asian_alone',
 'acs_tract_percent_1200_am_to_459_am',
 'acs_tract_percent_500_am_to_529_am',
 'acs_tract_percent_530_am_to_559_am',
 'acs_tract_percent_600_am_to_629_am',
 'acs_tract_percent_630_am_to_659_am',
 'acs_tract_percent_700_am_to_729_am',
 'acs_tract_percent_730_am_to_759_am',
 'acs_tract_percent_800_am_to_829_am',
 'acs_tract_percent_830_am_to_859_am',
 'acs_tract_percent_900_am_to_959_am',
 'acs_tract_percent_1000_am_to_1059_am',
 'acs_tract_percent_1100_am_to_1159_am',
 'acs_tract_percent_1200_pm_to_359_pm',
 'acs_tract_percent_400_pm_to_1159_pm',
 'acs_tract_percent_living_in_household_with_supplemental_security_income_ssi_cash_public_assistance_income_or_food_stampssn',
 'acs_tract_percent_less_than_10000',
 'acs_tract_percent_10000_to_14999',
 'acs_tract_percent_with_supplemental_security_income_ssi']

## add deep dive ones
## add deep dive ones
features_dictionary['for_deep_dive'] = features_dictionary['pluto'] + features_dictionary['external_violations'] + features_dictionary['internal_only'] + ['subsidized_housing_flag_static'] + ['internal_cases_opened_count_ever'] + acs_selected





features_dictionary['for_deep_dive']

## add updated labels
features_dictionary['internal_ratio_labels'] = ['internal_cases_opened_per_unit_next_month', 
                                                'internal_cases_opened_per_open_next_month',
                                                'internal_cases_opened_per_knock_next_month']




save_backup_to_amzS3(object_tostore = features_dictionary,
                    name_storefile = 'features_dictionary',
                    creds = creds)

