from sklearn import preprocessing
import yaml
import numpy as np
import pandas as pd
import boto3


## added find top_k for finding top k
def generate_topk_list(df_test, param_borough_predict_list, param_primary_label, param_percents_ofunits):

    print('generating top k')

    ## otherwise, subset to obs with observed labels in borough(s) predicting for
    df_test_borough = df_test.loc[df_test.internal_borough_factor_static.isin(pd.Series(param_borough_predict_list))]

    ## find count of addresses with observed labels
    count_address_bymonth = df_test_borough[['month_start', param_primary_label]].groupby('month_start',
                                                                as_index = False).agg('count')

    ## create list of months_no_observed_labels
    months_no_observed_labels = list(count_address_bymonth.month_start[count_address_bymonth[param_primary_label] == 0])


    ## if all are zero, specify that we can't evaluate predictions for any months
    if count_address_bymonth[param_primary_label].sum() == 0:

        print('no units to evaluate predictions on in any months')
        top_k = []
        return(top_k)

    else:

        ## apply the percentage to the minimum observed units across test set months
        ## subset to observed
        df_test_borough_observed = df_test_borough[df_test_borough[param_primary_label].notnull()]


        ## get count of units attached to observed labels by month
        countunits_obslabel_bymonth  = df_test_borough_observed[['month_start', 'internal_units_static']].groupby('month_start',
                            as_index = False).agg('sum')
        
        mean_units_observed = np.mean(countunits_obslabel_bymonth.internal_units_static)
        
        top_k_observed = [int(round(x * mean_units_observed)) for x in param_percents_ofunits]
        return(top_k_observed, months_no_observed_labels)




## functions to recode to top k categories in factor
def gen_topk_dummies_fromtrain(df_train, feature, threshold):

    ## get count of addresses in each level of factor
    ## recoding na to missing before counts
    df_train[feature] = np.where(df_train[feature].isnull(), 'missing', df_train[feature])
    count_addresses = pd.value_counts(df_train[feature])

    ## get an indicator for whether the category is above threshold of count of addresses
    categories_abovethreshold = count_addresses[count_addresses >= threshold].index
    categories_abovethres_logic = df_train[feature].isin(categories_abovethreshold)

    ## copy data, create category for missing and other
    df_train_dummies = df_train.copy()
    df_train_dummies[feature][~categories_abovethres_logic] = 'OTHER'
    dummy_df = pd.get_dummies(df_train_dummies[feature], prefix=df_train_dummies[feature].name)

    return(dummy_df, categories_abovethreshold)


def apply_topk_totest(df_test, feature, train_dummies, categories_abovethreshold):

    ## get count of addresses in each level of factor
    ## recoding na to missing before counts
    df_test[feature] = np.where(df_test[feature].isnull(), 'missing', df_test[feature])

    ## use categories above threshold from training set
    categories_abovethres_logic = df_test[feature].isin(categories_abovethreshold)


    ## copy data, create category for missing and other
    df_test_dummies = df_test.copy()
    df_test_dummies[feature][~categories_abovethres_logic] = 'OTHER'
    dummy_df_init = pd.get_dummies(df_test_dummies[feature], prefix=df_test_dummies[feature].name)

    ## add dummies for indicators that exist in training df but where value is not observed (all 0's)
    ## in any level of test set
    dummies_fromtrain_toadd = list(set(train_dummies.columns).difference(dummy_df_init.columns))
    dummies_fromtrain_df = pd.DataFrame(np.zeros((dummy_df_init.shape[0], len(dummies_fromtrain_toadd))),
                                        columns = dummies_fromtrain_toadd)
    dummy_df_init.reset_index(drop=True, inplace=True)
    dummies_fromtrain_df.reset_index(drop=True, inplace=True)
    dummy_df = pd.concat([dummies_fromtrain_df, dummy_df_init], axis = 1)


    return(dummy_df)


def recode_top_k(feature_type_df, df_train, df_test, creds, threshold, features_pulled, feature_categorical_all,
                categorical_dontrecode):

    ## get features to recode
    feature_categorical_eligible4recode = list(set(feature_categorical_all).difference(categorical_dontrecode))
    features_torecode = list(set(feature_categorical_eligible4recode).intersection(features_pulled))

    if(len(features_torecode) == 0):

        print('no features to generate dummies for')
        return(df_train, df_test, [], [], [])


    print("features to generate dummies for: " + ", ".join(features_torecode))


    ## create static version of train and test for value counts
    df_train_static  = df_train[['address_id'] + features_torecode].drop_duplicates().copy()
    df_test_static  = df_test[['address_id'] + features_torecode].drop_duplicates().copy()
    
    

    ## clean up owner name using string matching dictionary
    ## if the ownername is in the set of features to recode
    if 'pluto_ownername_static' in features_torecode:

        print('owners contained in feature list')

        ## load renaming dictionary
        pluto_rename_dict = load_rename_map(name_rename_map = 'pluto_ownermap.yaml', creds = creds)

        ## map values
        df_train_static['pluto_ownername_static'] = df_train_static['pluto_ownername_static'].map(pluto_rename_dict).fillna(df_train_static['pluto_ownername_static'])
        df_test_static['pluto_ownername_static'] = df_test_static['pluto_ownername_static'].map(pluto_rename_dict).fillna(df_test_static['pluto_ownername_static'])


    train_store_dummies = pd.DataFrame()
    test_store_dummies = pd.DataFrame()

    ## iterate over features to recode
    for feature in features_torecode:

        

        (train_recode, train_recode_categories) = gen_topk_dummies_fromtrain(df_train = df_train_static, feature = feature,
                              threshold = threshold)



        test_recode = apply_topk_totest(df_test = df_test_static, feature = feature,
                            train_dummies = train_recode,
                             categories_abovethreshold = train_recode_categories)


        train_store_dummies = pd.concat([train_store_dummies, train_recode], axis = 1)
        test_store_dummies =  pd.concat([test_store_dummies, test_recode], axis = 1)

    ## bind train_store_dummies and test_store_dummies with train and test

    ## first, concatenate with address ids
    train_store_dummies_withid = pd.concat([df_train_static['address_id'].reset_index(drop = True),
                                            train_store_dummies.reset_index(drop = True)], axis = 1)
    
    test_store_dummies_withid = pd.concat([df_test_static['address_id'].reset_index(drop = True),
                                            test_store_dummies.reset_index(drop = True)], axis = 1)
    

    ## then, merge so that it's added back to long format data
    train_toreturn = df_train.merge(train_store_dummies_withid, on = 'address_id', how = 'left')
    print(str('dimensions of training set are: ') + str(train_toreturn.shape))
    test_toreturn = df_test.merge(test_store_dummies_withid, on = 'address_id', how = 'left')
    print(str('dimensions of test set are: ') + str(test_toreturn.shape))


    return(train_toreturn, test_toreturn, list(train_store_dummies.columns), list(test_store_dummies.columns), features_torecode)

## function to load the dictionary mapping owner names generated using function below
def load_rename_map(name_rename_map, creds):

    ## pull from s3
    ## establish client and download file from s3
    ## first establish an s3 client
    s3_client = boto3.client('s3',
        aws_access_key_id=creds['aws_s3']['access_key_id'],
        aws_secret_access_key=creds['aws_s3']['secret_access_key'])

    ## then, download file
    s3_client.download_file('dsapp-economic-development', 
                    'nyc_peu_inspections/data_backups/{filename}'.format(filename = name_rename_map), 
                    'downloaded_pluto_renamed.yaml')
    

    ## load yaml file
    with open('downloaded_pluto_renamed.yaml','r') as stream:
        rename_map = yaml.load(stream)

    print("loaded file to clean up pluto owner names")

    return(rename_map)


## function to take in a list of partial
## string matches and create a dictionary of values to
## recode
def create_recode_dictionary(all_matches):

    storage_dict = {}

    for i in range(0, len(all_matches)):

        one_match = all_matches[i]

        ## create array with names of matched levels
        match_array = np.array(one_match)[:, 0]

        ## if the length of the array is greater than 1, add to recoding dictionary
        if len(match_array) > 1:

            ## create a dictionary that maps keys to values
            top_string = match_array[0]
            other_strings = match_array[1:]

            ## if top_string is already a key,
            ## skip (so that each partially matched value
            ## is just recoded to the most frequently occurring one)
            if len(set(other_strings.flatten()) & set(storage_dict.keys())) > 0:

                continue

            else:
                recode_dictionary = dict.fromkeys(other_strings, top_string)
                storage_dict.update(recode_dictionary)

    ## return the dictionary
    return(storage_dict)


## function to impute labels
## takes in a df, label name, label method, and probability for impute_wprob
## and returns a df containing new columns for each imputed var
## and flags for whether or not each obs is imputed


def impute_labels(df, label, method, p = None):

    # imports within function
    import numpy as np
    import pandas as pd

    # set seed
    np.random.seed(20171107)

    # set new var names
    label_imputed = str(label + '_imputed')
    label_imputed_flag = str(label + "_imputed" + "_flag")

    if method == 'impute_allzeros':

        df[label_imputed] = df[label].fillna(0)
        df[label_imputed_flag] = np.where(df[label].isnull(), 1, 0)

    elif method == 'impute_allones':

        df[label_imputed] = df[label].fillna(1)
        df[label_imputed_flag] = np.where(df[label].isnull(), 1, 0)

    elif method == 'impute_random':

        df[label_imputed] = df[label].apply(lambda x: np.randint(0, 2) if np.isnan(x) else x)
        df[label_imputed_flag] = np.where(df[label].isnull(), 1, 0)

    elif method == 'impute_wprob':

        df[label_imputed] = df[label].apply(lambda x: np.random.binomial(1, p, size=None) if np.isnan(x) else x)
        df[label_imputed_flag] = np.where(df[label].isnull(), 1, 0)

    else:

        df[label_imputed] = df[label]
        df[label_imputed_flag] = np.where(df[label].isnull(), 1, 0)

    return df


## function to define feature imputation methods
## for training data
## takes in a dataframe containing features
## one of 6 imputation methods
## and value of k for KNN
## and returns new columns in the df with imputed values

def impute_train(df, feature, impute_method, k):

    # imports
    # import pip
    # pip.main(['install', 'fancyimpute'])
    # from fancyimpute import KNN
    import pandas as pd
    import numpy as np

    if impute_method == 'impute_allzeros':
        return df[feature].fillna(0)

    elif impute_method == 'impute_allones':
        return df[feature].fillna(1)

    elif impute_method == 'impute_mean':
        return df.groupby('internal_borough_factor_static')[feature].transform(lambda x: x.fillna(0) if np.isnan(x.mean()) else x.fillna(x.mean()))

    elif impute_method == 'impute_median':
        return df.groupby('internal_borough_factor_static')[feature].transform(lambda x: x.fillna(0) if np.isnan(x.median()) else x.fillna(x.median()))

    elif impute_method == 'impute_mode':
        return df.groupby('internal_borough_factor_static')[feature].transform(lambda x: x.fillna(x.mode()[0]) if len(x.mode()) != 0 else x.fillna(0))

    elif impute_method == 'impute_knn':
        return pd.DataFrame(KNN(k=k).complete(pd.DataFrame(df[feature]).as_matrix()))

    else:
        return df[feature]

## function to define feature imputation methods
## for test data (based on values in training data)
## takes in training and test dfs
## and an imputation method
## and returns new columns in the df with imputed values

def impute_test(df_test, df_train, feature, impute_method):

    from scipy import stats

    if impute_method == 'impute_allzeros':
        return df_test[feature].fillna(0)

    elif impute_method == 'impute_allones':
        return df_test[feature].fillna(1)

    elif impute_method == 'impute_mean':
        df_train_mean = df_train.groupby('internal_borough_factor_static')[feature].mean().fillna(0).rename(feature + '_mean')
        df_test_merge = df_test.merge(df_train_mean.to_frame(), left_on = 'internal_borough_factor_static', right_index=True)
        df_test_merge[feature + '_' + impute_method] = df_test_merge[feature].where(df_test_merge[feature].notnull(), df_test_merge[feature + '_mean'])
        return df_test_merge[feature + '_' + impute_method]

    elif impute_method == 'impute_median':
        df_train_median = df_train.groupby('internal_borough_factor_static')[feature].median().fillna(0).rename(feature + '_median')
        df_test_merge = df_test.merge(df_train_median.to_frame(), left_on = 'internal_borough_factor_static', right_index=True)
        df_test_merge[feature + '_' + impute_method] = df_test_merge[feature].where(df_test_merge[feature].notnull(), df_test_merge[feature + '_median'])
        return df_test_merge[feature + '_' + impute_method]

    elif impute_method == 'impute_mode':
        df_train_mode = df_train.groupby('internal_borough_factor_static')[feature].agg(lambda x: stats.mode(x)[0][0]).rename(feature + '_mode')
        df_test_merge = df_test.merge(df_train_mode.to_frame(), left_on = 'internal_borough_factor_static', right_index=True)
        df_test_merge[feature + '_' + impute_method] = df_test_merge[feature].where(df_test_merge[feature].notnull(), df_test_merge[feature + '_mode'])
        return df_test_merge[feature + '_' + impute_method]

    else:
        return df_test[feature]


## function to impute values for features
## takes in a training or test dataframe,
## a df containing defining each column type,
## a list of features to be included in the model,
## a list of imputation methods for binary variables,
## and a list of imputation methods for continuous variables.
## returns new columns for each variable for each imputation method
## and a list of features to be included in the model
## including non-imputed features from the original feature list
## impute features function
## impute features function
def impute_features(df_type, df_train, df_test, feature_type_df, feature_list, binary_method, continuous_method):

    import pandas as pd

    # get lists of binary & continuous features
    binary_features = feature_type_df[feature_type_df['var_type']=='binary']['column_name'].tolist()
    continuous_features = feature_type_df[feature_type_df['var_type']=='continuous']['column_name'].tolist()

    if df_type == 'train':

        df_impute = df_train.copy()

        # get list of columns containing any nulls in the dataframe
        missing_features = df_train.columns[df_train.isna().any()].tolist()

        # get binary list of features to impute: those in the binary list + feature list + missing list
        binary_features_to_impute = list(set(binary_features) & set(feature_list) & set(missing_features))


        for feature in binary_features_to_impute:

            df_impute[feature] = impute_train(df_train, feature, binary_method, k=3)

        print("imputed " + str(binary_features_to_impute) + ' using ' + binary_method)

        # get continuous list of features to impute: those in the continuous list + feature list + missing list
        continuous_features_to_impute = list(set(continuous_features) & set(feature_list) & set(missing_features))

        for feature in continuous_features_to_impute:

            df_impute[feature] = impute_train(df_train, feature, continuous_method, k=3)

        print("imputed " + str(continuous_features_to_impute) + ' using ' + continuous_method)

        return df_impute

    elif df_type == 'test':

        df_impute = df_test.copy()

        # get list of columns containing any nulls in the dataframe
        missing_features = df_test.columns[df_test.isna().any()].tolist()

        # get binary list of features to impute: those in the binary list + feature list + missing list
        binary_features_to_impute = list(set(binary_features) & set(feature_list) & set(missing_features))

        continuous_features_to_impute = list(set(continuous_features) & set(feature_list) & set(missing_features))

        for feature in binary_features_to_impute:

            df_impute[feature] = impute_test(df_test, df_train, feature, binary_method)

        print("imputed " + str(binary_features_to_impute) + ' using ' + binary_method)

        # get continuous list of features to impute: those in the continuous list + feature list + missing list
        continuous_features_to_impute = list(set(continuous_features) & set(feature_list) & set(missing_features))

        for feature in continuous_features_to_impute:

            df_impute[feature] = impute_test(df_test, df_train, feature, continuous_method)

        return df_impute


## function to generate expand categorical variables into dummies
## takes in two dfs (training and test), a list of categorical vars to encode,
## and two feature lists (training and test)
## and returns two dfs with new dummy variables and two new feature lists
## including both imputed and encoded features

def one_hot_encoding(df_train, df_test, vars_to_encode, feature_list_imputed_train, feature_list_imputed_test):

    # encode categorical variables in list vars_to_encode
    df_train_dummies = pd.get_dummies(df_train[vars_to_encode])
    df_test_dummies = pd.get_dummies(df_test[vars_to_encode])

    dummy_intersect = list(set(df_train_dummies.columns) & set(df_test_dummies.columns))
    print(dummy_intersect)

    # get dfs with only dummies that are in both df_train and df_test
    df_train_dummies_intersect = df_train_dummies[dummy_intersect]
    df_test_dummies_intersect = df_test_dummies[dummy_intersect]

    # concat dummies df onto original
    df_train = pd.concat([df_train, df_train_dummies_intersect], axis=1)
    df_test = pd.concat([df_test, df_test_dummies_intersect], axis=1)

    # add new dummies to feature_list

    feature_list_imputed_encoded_train = feature_list_imputed_train + dummy_intersect
    feature_list_imputed_encoded_test = feature_list_imputed_test + dummy_intersect

    return df_train, df_test, feature_list_imputed_encoded_train, feature_list_imputed_encoded_test


## function to normalize non-binary variables in imputed dfs
## takes in the imputed dfs, list of features in the model, and list of binary features to shield
## and returns a df where all variables except id vars and binary vars are normalized

def normalize_imputed_df(df_imputed,
                  features_to_normalize,
                  all_no_normalize,
                  use_minmax_train = False,
                  train_scaler = None):

    ## fit the scaler
    if use_minmax_train == False:

        ## convert first to data and then array (need first for later step)
        df_use_fornorm = df_imputed[features_to_normalize]
        df_use_fornorm_np = df_use_fornorm.values

        ## initialize normalizer
        min_max_scaler = preprocessing.MinMaxScaler()
        train_fit_scaler = min_max_scaler.fit(df_use_fornorm_np)
        df_normalized_np = train_fit_scaler.transform(df_use_fornorm_np)
        ## recombine into data to return
        df_normalized_df = pd.concat([df_imputed[all_no_normalize].reset_index(drop = True),
                                 pd.DataFrame(df_normalized_np,
                                        columns = df_use_fornorm.columns)],
                             axis = 1)

        return(df_normalized_df, train_fit_scaler)

    else:

        df_use_fornorm = df_imputed[features_to_normalize]
        df_use_fornorm_np = df_use_fornorm.values

        df_normalized_np = train_scaler.transform(df_use_fornorm_np)
        df_normalized_df = pd.concat([df_imputed[all_no_normalize].reset_index(drop = True),
                                 pd.DataFrame(df_normalized_np,
                                        columns = df_use_fornorm.columns)],
                             axis = 1)

        return(df_normalized_df)

## function to generate
## list of intersecting features in train and test in final
## data used for model estimation
def clean_features_final_list(df_train_with_dummies,
                                df_train_dummy_names,
                             df_test_dummy_names,
                             categorical_features_norecode, 
                             features_recoded,
                             features_pulled):
    categorical_exclude = categorical_features_norecode + features_recoded

    features_touse_train = list(set(df_train_dummy_names).difference(categorical_exclude)) + list(set(features_pulled).difference(categorical_exclude))

    features_touse_test = list(set(df_test_dummy_names).difference(categorical_exclude)) + list(set(features_pulled).difference(categorical_exclude))

    feature_intersect_init = list(set(features_touse_train).intersection(features_touse_test))

    ## filter out features with commas (rare; sometimes in pluto owner with low enough threshold)
    ## because of errors they cause at str.split stage of reading in SQL tables with feature lists
    feature_intersect_init2 = [feature for feature in feature_intersect_init if "," not in feature]
    feature_intersect = [feature for feature in feature_intersect_init2 if 
                    feature not in 
                    list(set(feature_intersect_init2).difference(df_train_with_dummies.columns.tolist()))]
    return(feature_intersect)
