
## imports
import psycopg2
import io
import pandas as pd
import numpy as np
import json
from datetime import datetime
from sklearn.metrics import confusion_matrix

import sqlalchemy
from sqlalchemy import *
from sqlalchemy.types import *
import sqlalchemy.types as sql_types

## takes in creds dictionary
## (with appropriate keys)
## and returns a connection to
## database through psycopg2
def startengine_psy(creds):
    connection_string = """
    host = '{hostname}'
    user = '{username}'
    dbname = '{dbname}'
    password = '{password}'
    """.format(hostname = creds['database']['host'],
              username = creds['database']['user'],
              dbname = creds['database']['dbname'],
              password = creds['database']['password'])
    connection = psycopg2.connect(connection_string)
    return(connection)
  
## takes in creds dictionary
## and returns a connection
## to database through 
## sqlalchemy with either the write role
## adopted (default) or the read
## role adopted
def startengine_alchemy(creds, set_write_role = True, set_read_role = False, return_raw_connection = False):
    
    ## start general connection
    engine_string = "postgresql://{user}:{password}@{host}/{dbname}".format(user = creds['database']['user'],
                        password = creds['database']['password'],
                        host = creds['database']['host'],
                        dbname =  creds['database']['dbname'])
    engine = create_engine(engine_string)
    alchemy_connection = engine.connect()
    
    ## set role based on logic
    if set_write_role == True and return_raw_connection == False:
        role_statement  = text("""set role {role}""".format(role = creds['database']['write_role']))
        alchemy_connection.execute(role_statement)
        return(alchemy_connection)
    elif set_write_role == False and set_read_role == True and return_raw_connection == False:
        role_statement  = text("""set role {role}""".format(role = creds['database']['write_role']))
        connection.execute(role_statement)
        return(alchemy_connection)
    elif set_write_role == True and return_raw_connection == True:
        role_statement_psycog  = """set role {role}""".format(role = creds['database']['write_role'])
        role_statement_alchemy  = text("""set role {role}""".format(role = creds['database']['write_role']))
        alchemy_connection.execute(role_statement_alchemy)
        psycog_connection = engine.raw_connection()
        cursor = psycog_connection.cursor()
        cursor.execute(role_statement_psycog)
        return(cursor, psycog_connection, alchemy_connection)
    else:
        print('please choose either read or write role')
        return None


## package into a function
def create_col_dict(df_full, default_to_text = True):

    type_dict = df_full.dtypes.apply(lambda x: x.name).to_dict()
    if default_to_text == True:
        for key, value in type_dict.items():
            type_dict[key] = sql_types.TEXT
        return(type_dict)
    elif default_to_text == False:
        print('customize types')
        for key, value in type_dict.items():
            if value == 'int64' or value == 'uint8':
                type_dict[key] = sql_types.INTEGER
            elif value == 'float64':
                type_dict[key] = sql_types.NUMERIC 
            elif key == 'month_start':
                type_dict[key] = sql_types.TIMESTAMP
            elif value == 'object':
                type_dict[key] = sql_types.TEXT
        return(type_dict)

## package into a function
def create_table(cursor, psycog_connection, alchemy_connection, tablename, schemaname, df, create_col_dict,
                default_to_text = True):
    
    ## guess data types
    column_types_dict = create_col_dict(df, default_to_text = default_to_text)
    print(column_types_dict)

    ## create table with first row of data
    if df.shape[0] > 1000:

        df.iloc[0:1000].to_sql(tablename, alchemy_connection,
                            if_exists='replace',
                            schema=schemaname, 
                      index=False, 
                      dtype=column_types_dict) #truncates the table and defaults to nvarchar

        print('created table')
        
        ## then, use the copy_from command to insert the rest of the rows
        output = io.StringIO()
        df.iloc[1000:, ].to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        contents = output.getvalue()
        copytablename = str(schemaname + "." + tablename)
        cursor.copy_from(output, copytablename, null = '') # null values become ''
        psycog_connection.commit()
        return None
    elif df.shape[0] <= 1000:
        df.iloc[0:5].to_sql(tablename, alchemy_connection,
                            if_exists='replace',
                            schema=schemaname, 
                      index=False, 
                      dtype=column_types_dict) #truncates the table and defaults to nvarchar

        print('created table')
        
        ## then, use the copy_from command to insert the rest of the rows
        output = io.StringIO()
        df.iloc[5:, ].to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        contents = output.getvalue()
        copytablename = str(schemaname + "." + tablename)
        cursor.copy_from(output, copytablename, null = '') # null values become ''
        psycog_connection.commit()
        return None     



# takes in:
# name of schema (string)
# name of table (string)
# psycog cursor (generated using start_engine_alchemy)
# returns:
# list of column names
# takes in:
# name of schema (string)
# name of table (string)
# psycog cursor (generated using start_engine_alchemy)
# returns:
# list of column names
def get_colnames_fromtable_postgres(schema_name, table_name, cursor):
    
    ## rj note: tried with information schema
    ## query-- it worked in dbeaver but didnt yield
    ## what it was supposed to here:
    ## SELECT column_name, table_name FROM information_schema.columns where table_schema = 'schema' and 
    ## table_name = 'table'
    
    cols_query = """SELECT * from {schemaname}.{tablename} limit 1;""".format(schemaname= schema_name, 
                                                    tablename = table_name)
    cursor.execute(cols_query)
    
    colnames = [desc[0] for desc in cursor.description]

    return(colnames)


# takes in:
# name of table
# name of schema
# sqlalchemy connection to database
def readtable_postgres(tablename, schemaname, alchemy_connection):
    
    return(pd.read_sql_table(table_name = tablename,
                            con = alchemy_connection,
                            schema = schemaname))


## takes in query and connection and returns pandas dataframe
def readquery_todf_postgres(query, alchemy_connection):
    
    return(pd.read_sql_query(sql = query,
                            con = alchemy_connection))


## function to insert modelgroup row
def sql_insert_modelgroup(schematable_insert,columns_to_insert,values_to_insert,
                       alchemy_connection):
    ### insert query
    insert_table_query = """
                        INSERT INTO {schematable_insert} ({columns_to_insert}) values
                        ({values_to_insert}) returning model_group_id
                        """.format(schematable_insert = schematable_insert,
                                   columns_to_insert = columns_to_insert,
                                   values_to_insert = values_to_insert
                                  )
    
    ### execute query
    results_query = alchemy_connection.execute(sqlalchemy.text(insert_table_query))
    (model_group_id,) = results_query.fetchall()
    model_group_id_return = model_group_id[0]
    return model_group_id_return

def sql_insert_model(schematable_insert,columns_to_insert,values_to_insert,
                       alchemy_connection):
    ### insert query
    insert_table_query = """
                        INSERT INTO {schematable_insert} ({columns_to_insert}) values
                        ({values_to_insert}) returning model_id
                        """.format(schematable_insert = schematable_insert,
                                   columns_to_insert = columns_to_insert,
                                   values_to_insert = values_to_insert
                                  )
    
    ### execute query
    results_query = alchemy_connection.execute(sqlalchemy.text(insert_table_query))
    (model_id,) = results_query.fetchall()
    model_id_return = model_id[0]
    return model_id_return
def get_row_model_group_fromid(modelgroup_id,alchemy_connection):
    query_model_group = """
    select *
    from dssg_results.model_group
    where model_group_id = '{id_to_retrieve}'
    """.format(id_to_retrieve = modelgroup_id)

    query_table_exec = readquery_todf_postgres(query = query_model_group,
                    alchemy_connection = alchemy_connection)

    ### return the last row in pandas format
    return query_table_exec

def create_models_row(rows_modelgroup, 
                    feature_list,
                    train_table_name,
                    model_call,
                    label_imputation_method):

     
        ## store the coefficients in a dictionary
        print('rows_modelgroup.algorithm_type: ',rows_modelgroup.algorithm_type)
        if rows_modelgroup.algorithm_type.isin(['DecisionTreeClassifier', 'RandomForestClassifier',
                  'GradientBoostingClassifier', 'AdaBoostClassifier','SVC'])[0]:
            model_parameters = 'N/A'
        else:
            coef_dict = {}
            coef_list = list(model_call.coef_.tolist()[0])
            for j in range(0, len(coef_list)):
                coef_dict[feature_list[j]] = coef_list[j]

            model_parameters_json = json.dumps(coef_dict)
            model_parameters = str(model_parameters_json).replace('"','\\"')
        
       ## add insert time?
        
        
        
        ## combine in to a string
        model_row = ', '.join(map(lambda x: "'" + x + "'",[str(rows_modelgroup['model_group_id'][0]),
                                                           str(rows_modelgroup['algorithm_type'][0]),model_parameters,
                                                           str(train_table_name),
                                                           str(label_imputation_method),
                                                          datetime.now().strftime("%I:%M%p on %B %d, %Y")]))
        return (model_row)
    
def create_modelgroup_row(model_call, param_features_to_pull, label_name, borough_fit_list):

    algorithm_type = str(model_call).split("(")[0]
    if algorithm_type == 'AdaBoostClassifier':
        original_parameters = model_call.get_params()
        original_parameters['base_estimator'] = str(original_parameters['base_estimator'])
        hyperparameters = json.dumps(original_parameters)
        
        hyperparameters_set = set(model_call.get_params().items())
        hyperparameters_str = str(hyperparameters).replace('"','\\"')
    else:
        hyperparameters = json.dumps(model_call.get_params())
        hyperparameters_set = set(model_call.get_params().items())
        hyperparameters_str = str(hyperparameters).replace('"','\\"')
#     print(hyperparameters_str)

    borough_fit_str = '_'.join(borough_fit_list)
    borough_set = set(borough_fit_list)
    
    ## string version of row
    modelgroup_row = ', '.join(map(lambda x: "'" + x + "'",[algorithm_type, label_name,hyperparameters_str,
                                                            param_features_to_pull,
                                                           borough_fit_str]))
#     print(modelgroup_row)
    ## set version of row
    modelgroup_row_list = [algorithm_type, label_name, hyperparameters_str, param_features_to_pull,
                          borough_set]

    
    ## return a string of model group row
    return(modelgroup_row,modelgroup_row_list)

## function to return a row
## from models
def get_row_model_fromid(model_id,alchemy_connection):
    query_models = """
    select *
    from dssg_results.models
    where model_id = '{id_to_retrieve}'
    """.format(id_to_retrieve = model_id)

    query_table_exec = readquery_todf_postgres(query = query_models,
                    alchemy_connection = alchemy_connection)

    ### return the last row in pandas format
    return query_table_exec

def create_predictions_rows(testing_features_df, models_row, label_prob, unit_id, time_id,
                           top_k, borough_predict_list, param_primary_label):  
    as_of_date = testing_features_df[time_id]
    entity_id = testing_features_df[unit_id]
    training_table_name = models_row['training_table_name'][0]
    score = label_prob[:, 1]
    label_observed = np.where(testing_features_df[param_primary_label].notnull(), 1, 0)
    boroughs_predicted_for_str = '_'.join(borough_predict_list)
    predictions_df = pd.DataFrame(data = {'model_id': models_row['model_id'][0],
                                          'as_of_date': as_of_date,
                                          'borough_predicted_for': boroughs_predicted_for_str,
                                          'entity_id': entity_id,
                                          'training_table_name': training_table_name,
                                          'score': score,
                                          'top_k': top_k,
                                         'label_observed': label_observed})
    predictions_df['rank_abs'] = predictions_df.score.rank(ascending = False).astype(int) 
    predictions_df['rank_pct'] = predictions_df.rank_abs/predictions_df.shape[0]
    return(predictions_df[['model_id','as_of_date',
                           'borough_predicted_for',
                           'entity_id',
                           'training_table_name',
                           'score',
                           'rank_abs',
                           'rank_pct',
                          'top_k',
                          'label_observed']])

def create_featureimportances_rows(importance_df, models_row): 
        coef_dict_df = importance_df.copy()
        coef_dict_df['rank_abs'] = coef_dict_df.feature_importance.rank(ascending =False).astype(int) 
        coef_dict_df['model_id'] = models_row['model_id'][0]
        coef_dict_df['rank_pct'] = coef_dict_df.rank_abs/coef_dict_df.shape[0]
        
        return(coef_dict_df[['model_id', 'feature', 'feature_importance',
                   'rank_abs', 'rank_pct']]) #reorder and return dataframe
    
 # functions to facilitate evaluation generation
 # 1
def pull_count_units(alchemy_connection):
    pull_units = """
    select address_id,internal_units_static
    FROM dssg_staging.entities_address_table_rs_clean
    WHERE   internal_peu_target_zip_static=1 and internal_units_static::int >= 6
    """
    units_table = readquery_todf_postgres(query = pull_units,
                                     alchemy_connection = alchemy_connection)
    return(units_table)
# 2
def not_imputed_dataframe_for_metrics(testing_features_df,label_name):
    print(label_name)
#     label_imputed_flag = label_name + '_flag'
    testing_features_df_not_imputed = testing_features_df.loc[testing_features_df[label_name].notnull()].copy()
    #### generate a dataframe that contains 'address_id','internal_units_static','prediction_prob'
    address_unit_proba = testing_features_df_not_imputed[['address_id','month_start','internal_units_static','prediction_prob',label_name]]
    address_unit_proba = address_unit_proba.sort_values(by = ['prediction_prob','internal_units_static'], ascending=False).copy()

    address_unit_proba['row_number'] = [i for i in range(1, len(address_unit_proba)+1)]
    address_unit_proba['k_proportion'] = address_unit_proba.row_number.astype(float)/len(address_unit_proba)

    address_unit_proba[label_name+'_cumsum'] = address_unit_proba[label_name].cumsum()
    address_unit_proba['precision_not_imputed'] = address_unit_proba[label_name+'_cumsum'].astype(float)/address_unit_proba['row_number']
    label_sum = sum(address_unit_proba[label_name])
    address_unit_proba['recall_not_imputed'] = address_unit_proba[label_name+'_cumsum'].astype(float)/label_sum

    address_unit_proba['num_units_so_far'] = address_unit_proba['internal_units_static'].cumsum()
    return address_unit_proba
# 3
def get_last_k_row_number(dataframe,k):
    if list(dataframe.num_units_so_far)[-1] >= k:
        last_row = list(dataframe.loc[dataframe.num_units_so_far >= k].row_number)[0]
    ### if the sum of the units in this dataframe is less than k, return the last row number
    else:
        last_row = list(dataframe.row_number)[-1]
    return last_row
# 4
def confusion_matrix_atk(dataframe,last_row,label_suffix,label_name,confusion_results,confusion_results_list):
    ### label_suffix could be ['_no_impute','_imputed_1', '_imputed_0','_upper_bound']
    ### generate the prediction label
    dataframe['prediction_label_at_k'] = [1 for i in range(last_row-1)] + [0 for i in range(len(dataframe)-last_row+1)]
    y_pred = dataframe['prediction_label_at_k']

    if label_suffix == '_no_impute':
        y_true = dataframe[label_name]
    elif label_suffix == '_imputed_1' or label_suffix == '_imputed_0':
        label_use = label_name + label_suffix
        y_true = dataframe[label_use]
    elif label_suffix == '_upper_bound': # label_suffix = '_lower_bound'
        y_true = list(dataframe.loc[dataframe.row_number<=last_row, label_name].fillna(1)) + list(dataframe.loc[dataframe.row_number>last_row, label_name].fillna(0))
    elif label_suffix == '_lower_bound': # label_suffix = '_lower_bound'
        y_true = list(dataframe.loc[dataframe.row_number<=last_row, label_name].fillna(0)) + list(dataframe.loc[dataframe.row_number>last_row, label_name].fillna(1))

#     print('len(y_pred):', len(y_pred))

    ### calculate confusion_matrix_array
    confusion_matrix_array = confusion_matrix(y_true,y_pred,labels =[0,1])
#     print('confusion_matrix_array: ', confusion_matrix_array)
    confusion_tn = confusion_matrix_array[0][0]
    confusion_fp = confusion_matrix_array[0][1]
    confusion_fn = confusion_matrix_array[1][0]
    confusion_tp = confusion_matrix_array[1][1]

    confusion_results.extend([confusion_tn,confusion_fp,confusion_fn,confusion_tp])
    confusion_results_list.extend(['confusion_true_negative%s'%label_suffix,'confusion_false_positive%s'%label_suffix,'confusion_false_negative%s'%label_suffix,'confusion_true_positive%s'%label_suffix])

    return(confusion_results,confusion_results_list)
def generate_evaluations_row_insert(k,testing_features_df,label_prob,
                                    label_name,
                                    confusion_matrix_atk,
                                    not_imputed_dataframe_for_metrics, 
                                    get_last_k_row_number, 
                                    models_row):


    ### parameter definition
    ### k: number of units to target in next month
    ### testing_features_df: df that has label,feature and everything for testing
    ### label_prob: predicted proba generated by model
    print('top_k starts')
    print(label_name)
    model_id=models_row['model_id'][0]
    model_type=models_row['model_type'][0]

    ### add probability to the df
    testing_features_df['prediction_prob'] = np.array(label_prob)[:,1]
    ### filter the testing_features_df to df with no missing label
#     label_imputed_flag = label_name + '_imputed_flag'
    print('testing_features_df[prediction_prob] finished')

    
    ###-----------------------------------------------initial result lists-----------------------------------------------
    precision_results = []
    precision_results_list = []

    recall_results = []
    recall_results_list = []

    confusion_results = []
    confusion_results_list = []

    ### get the not imputation dataframe
    dataframe_not_imputed = not_imputed_dataframe_for_metrics(testing_features_df=testing_features_df,label_name=label_name)
    ### get the precision baseline score
    precision_baseline = list(dataframe_not_imputed.precision_not_imputed)[-1]
    ### add precision baseline score here in the list
    precision_results.append(precision_baseline)
    precision_results_list.append('precision_baseline')

    ### get the row number of last row in k
    last_row_not_imputed = get_last_k_row_number(dataframe=dataframe_not_imputed,k=k)
    ### retrieve the precision and recall score in this row and append to the results list
    precision_no_imputed = dataframe_not_imputed.iloc[last_row_not_imputed-1]['precision_not_imputed']
    precision_results.append(precision_no_imputed)
    precision_results_list.append('precision_no_imputed')

    recall_not_imputed = dataframe_not_imputed.iloc[last_row_not_imputed-1]['recall_not_imputed']
    recall_results.append(recall_not_imputed)
    recall_results_list.append('recall_not_imputed')
    


    ###-----------------------------------------------confusion matrix-----------------------------------------------


    ## then, take the observed versus predicted labels
    ## and generate a confusion matrix

    # confusion_matrix_not_imputed
    (confusion_results,confusion_results_list) = confusion_matrix_atk(dataframe = dataframe_not_imputed,
                                                                      last_row = last_row_not_imputed,
                                                                      label_suffix='_no_impute',
                                                                      label_name = label_name,
                                                                      confusion_results=confusion_results,
                                                                      confusion_results_list=confusion_results_list)


    ###-----------------------------------------------add all result metrics lists together-------------------------------------------------


    evaluation_results_all = precision_results + recall_results + confusion_results
    evaluation_list_all = precision_results_list + recall_results_list + confusion_results_list

    
    return (evaluation_results_all, evaluation_list_all)

## function to create evaluations rows
def create_evaluations_rows(evaluations_list_values,
                            evaluations_list_names,
                           models_row,
                            predictions_df,
                           test_filename,
                           top_k): 
    
  
    evaluations_df = pd.DataFrame({'model_id': models_row['model_id'][0],
                         'as_of_date': predictions_df.as_of_date.iloc[0],
                         'metric': evaluations_list_names,
                         'parameters' : 'NotApplicable',
                         'value' : evaluations_list_values,
                         'test_matrix_name': test_filename,
                        'top_k': str(top_k)})
    return(evaluations_df[['model_id','as_of_date','metric','parameters',
                           'value','test_matrix_name', 'top_k']])  
### function used for feature importance table
def create_importance_df(models_row,one_model,training_df):
    feature_list = list(training_df.columns)
    
    if models_row.model_type.isin(['DecisionTreeClassifier','RandomForestClassifier','AdaBoostClassifier',
                                     'GradientBoostingClassifier'])[0]:
        importance_array = one_model.feature_importances_.copy()
    elif models_row.model_type.isin(['LogisticRegression','LogisticRegressionCV','Lasso','LassoCV','RidgeClassifier','RidgeClassifierCV'])[0]:
       ### this alternative line is for linear SVC that has a .coef_ function
       # elif models_row.model_type.isin(['LogisticRegression','LogisticRegressionCV','Lasso','LassoCV','RidgeClassifier','RidgeClassifierCV','SVC'])[0]:
        importance_array = list(one_model.coef_.tolist()[0])
    
    importance_df = pd.DataFrame(zip(feature_list,importance_array), columns=['feature','feature_importance'])
    
    return importance_df

## functions to create the training and test data
def convert_labels_toschema(label,
                           param_label_quantile_threshold):
    if label == 'internal_cases_opened_any_next_month':
        return('anycase')
    elif label == 'internal_opens_any_next_month':
        return('anyopen')
    elif label == 'internal_knocks_any_next_month':
        return('anyknock')
    elif label == 'internal_cases_opened_per_unit_next_month_binary' and param_label_quantile_threshold == 0.9:
        return('casethresholdhigher')
    elif label == 'internal_cases_opened_per_unit_next_month_binary' and param_label_quantile_threshold == 0.75:
        return('casethresholdlower')

def all_staging_tables(creds, cursor, param_primary_label,
                      param_label_quantile_threshold):
    
    schema_suffix = convert_labels_toschema(param_primary_label,
                                           param_label_quantile_threshold)
    
    ## read in staging tables from tabledef
    schemaname = "dssg_staging_" + str(schema_suffix)
    cursor.execute("select * from pg_tables where schemaname = '{schemaname}';".format(schemaname = schemaname))
    tables = [x[1] for x in cursor.fetchall()]
    split_df_allstaging = [table for table in tables if "train_split" in table
                                                        or "test_split" in table]
    
    return(split_df_allstaging)

def return_split_dates(tables_available):
   split_tables = [table.split("_") for table in tables_available]
   all_split_dates = []
   for item in split_tables:
       for subitem in item:
           if subitem.isdigit() and subitem not in all_split_dates:
               all_split_dates.append(subitem)
           else:
               pass
   all_split_dates.sort()
   return(all_split_dates)
   
 ## function to run a single model
def run_one_model(one_model,
                 x_train_features,
                 label_train,
                 x_test_features,
                 param_primary_label,
                 param_borough_fit_list,
                 alchemy_connection,
                 feature_intersect,
                 train_filename,
                 param_label_imputation_method,
                 top_k_splitdate,
                 param_borough_predict_list,
                 test_filename,
                 model_group_id,
                 models_columns_to_insert,
                 testing_features_df,training_features_df):
    one_model.fit(x_train_features.values, label_train.values)
    ### make predictions

    label_prob = one_model.predict_proba(x_test_features)

   
    ### insert into models table
    modelgroup_row = get_row_model_group_fromid(modelgroup_id=model_group_id,
                                            alchemy_connection=alchemy_connection)
    model_values_to_insert = create_models_row(rows_modelgroup=modelgroup_row, 
                    feature_list=feature_intersect,
                    train_table_name=train_filename,
                    model_call=one_model,
                    label_imputation_method=param_label_imputation_method)
     ### pass value to schematable_insert for sql_insert_function
    model_id=sql_insert_model(schematable_insert='dssg_results.models',
                    columns_to_insert= models_columns_to_insert,
                    values_to_insert= model_values_to_insert,
                   alchemy_connection = alchemy_connection)
    models_row = get_row_model_fromid(model_id=model_id,alchemy_connection=alchemy_connection)
    
    ### prediction table
    predictions_df = create_predictions_rows(testing_features_df = testing_features_df,
                             models_row = models_row,
                             label_prob = label_prob,
                             unit_id = 'address_id',
                             time_id = 'month_start',
                             top_k = top_k_splitdate,
                             borough_predict_list = param_borough_predict_list,
                             param_primary_label = param_primary_label)
    predictions_df.to_sql('predictions',alchemy_connection,if_exists='append',schema='dssg_results',
                   index=False)
    
    ### feature importance table
    model_type = models_row['model_type'][0]
    model_id = models_row['model_id'][0]
    
    importance_df = create_importance_df(models_row=models_row,one_model=one_model,
                                         training_df=x_train_features)

    featureimportance_df = create_featureimportances_rows(importance_df=importance_df, models_row = models_row)

    featureimportance_df.to_sql('feature_importances',alchemy_connection, if_exists='append',
                   schema='dssg_results',index=False)
    
    ### evaluation table
    units_df = pull_count_units(alchemy_connection=alchemy_connection)
    testing_features_df_units = testing_features_df.merge(units_df,how='left',
                                                left_on='address_id',right_on='address_id')
    (eval_list_values, eval_list_names) = generate_evaluations_row_insert(
                                            k=top_k_splitdate,
                                            testing_features_df=testing_features_df_units,
                                            label_prob=label_prob,
                                            label_name= param_primary_label,
                                            confusion_matrix_atk=confusion_matrix_atk,
                                            not_imputed_dataframe_for_metrics=not_imputed_dataframe_for_metrics,
                                            get_last_k_row_number=get_last_k_row_number,
                                            models_row=models_row )
    evaluations_df = create_evaluations_rows(evaluations_list_values=eval_list_values,
                            evaluations_list_names=eval_list_names,
                           models_row=models_row,
                            predictions_df=predictions_df,
                           test_filename=test_filename,
                           top_k=top_k_splitdate)
    evaluations_df.to_sql('evaluations',alchemy_connection,
                   if_exists='append', schema='dssg_results',
                   index=False)

    return 'one_model_finished'
    
    
    
