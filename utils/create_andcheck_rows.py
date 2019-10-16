### imports
import civis 
import yaml
import pandas as pd
import numpy as np
import hashlib 
import json 


### create rows functions

def create_modelgroup_row(model_call, training_df, label_name, borough_fit_list):

    algorithm_type = str(model_call).split("(")[0]
    if algorithm_type == 'AdaBoostClassifier':
        original_parameters = model_call.get_params()
        original_parameters['base_estimator'] = str(original_parameters['base_estimator'])
        hyperparameters = json.dumps(original_parameters)
        
        hyperparameters_set = set(model_call.get_params().items())
        hyperparameters_str = str(hyperparameters).replace('\'','"').replace('"','\\"')
    else:
        hyperparameters = json.dumps(model_call.get_params())
        hyperparameters_set = set(model_call.get_params().items())
        hyperparameters_str = str(hyperparameters).replace('\'','"').replace('"','\\"')
#     print(hyperparameters_str)
    model_group_feature_list = ','.join(list(training_df.columns))
    features_set = set(list(training_df.columns))
    borough_fit_str = '_'.join(borough_fit_list)
    borough_set = set(borough_fit_list)
    
    ## string version of row
    modelgroup_row = ', '.join(map(lambda x: "'" + x + "'",[algorithm_type, label_name,hyperparameters_str,
                                                            model_group_feature_list,
                                                           borough_fit_str]))
#     print(modelgroup_row)
    ## set version of row
    modelgroup_row_list = [algorithm_type, label_name, hyperparameters_str, features_set,
                          borough_set]

    
    ## return a string of model group row
    return(modelgroup_row,modelgroup_row_list)

def create_models_row(rows_modelgroup, 
                    feature_list,
                    train_uuid,
                    creds,
                    model_call,
                    label_imputation_method,
                    model_comment_toadd):

     
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

            model_parameters = json.dumps(coef_dict)
        
        ## add optional model comment
        if model_comment_toadd == 'None':
            model_comment = 'None'
        else:
            model_comment = model_comment_toadd
        
        
        ## combine in to a string
        model_row = ', '.join(map(lambda x: "'" + x + "'",[str(rows_modelgroup['model_group_id'][0]),
                                                           str(rows_modelgroup['algorithm_type'][0]),model_parameters,
                                                           str(train_uuid),
                                                           str(label_imputation_method),
                                                           model_comment]))
        return (model_row)



def create_featureimportances_rows(importance_array, modelgroup_row, models_row): 
        coef_dict_df = pd.DataFrame()
        coef_dict_df['feature_importance'] = importance_array #to do: check the order of feature-importance
        coef_dict_df['feature'] = modelgroup_row['feature_list'][0].split(',')
        coef_dict_df['rank_abs'] = coef_dict_df.feature_importance.rank(ascending =False).astype(int) 
        coef_dict_df['model_id'] = models_row['model_id'][0]
        coef_dict_df['rank_percent'] = coef_dict_df.rank_abs/coef_dict_df.shape[0]
        
        return(coef_dict_df[['model_id', 'feature', 'feature_importance',
                   'rank_abs', 'rank_percent']]) #reorder and return dataframe


def create_predictions_rows(testing_features_df, models_row, label_prob, unit_id, time_id,
                           top_k, borough_predict_list, label_imputed_flag):  
    as_of_date = testing_features_df[time_id]
    entity_id = testing_features_df[unit_id]
    matrix_id = models_row['training_matrix_uuid'][0]
    score = label_prob[:, 1]
    impute_label = np.where(testing_features_df[label_imputed_flag] == 1, 1, 0) 
    boroughs_predicted_for_str = '_'.join(borough_predict_list)
    predictions_df = pd.DataFrame(data = {'model_id': models_row['model_id'][0],
                                          'as_of_date': as_of_date,
                                          'borough_predicted_for': boroughs_predicted_for_str,
                                          'entity_id': entity_id,'matrix_id': matrix_id,
                                          'score': score,
                                          'top_k': top_k,
                                         'impute_label': impute_label})
    predictions_df['rank_abs'] = predictions_df.score.rank(ascending = False).astype(int) 
    predictions_df['rank_pct'] = predictions_df.rank_abs/predictions_df.shape[0]
    return(predictions_df[['model_id','as_of_date',
                           'borough_predicted_for',
                           'entity_id',
                           'matrix_id',
                           'score',
                           'rank_abs',
                           'rank_pct',
                          'top_k',
                          'impute_label']])




def create_evaluations_rows(evaluations_list_values,
                            evaluations_list_names,
                           models_row,
                            predictions_df,
                           test_uuid,
                           top_k): 
    
  
    evaluations_df = pd.DataFrame({'model_id': models_row['model_id'][0],
                         'as_of_date': predictions_df.as_of_date.iloc[0],
                         'metric': evaluations_list_names,
                         'parameters' : 'NotApplicable',
                         'value' : evaluations_list_values,
                         'matrix_id': str(test_uuid),
                        'top_k': str(top_k)})
    return(evaluations_df[['model_id','as_of_date','metric','parameters',
                           'value','matrix_id', 'top_k']])        


## check model existence functions
def checkif_modelgroup_exists(model_group_values_to_check, creds):
    
    ## select all rows/cols from the model group table 
    ## returns a list of lists (by not instructing it to read as pandas df)
    query_model_group_all = """
    select *
    from {table_name}
    order by model_group_id desc
    """.format(table_name = creds['schema_results'] + ".model_group")

    mg_allcols = civis.io.read_civis_sql(
        query_model_group_all, database=creds['database'],
        credential_id = creds['civis_superuser']['civis_id'])
        
    print('read in model group table')
    ## remove the first column (model group id) from the 
    ## each sublist to check
    
    mg_cols_tocheck = [item[1:6] for item in mg_allcols]

    
    ## iterate through all but the first sublist (which contains col headers)
    ## and if the row to check matches all columns, return the index
    
    for i in range(1, len(mg_cols_tocheck)):
    
        one_row = mg_cols_tocheck[i]
        
    
        if ((one_row[0] == model_group_values_to_check[0]) and 
        (one_row[1] == model_group_values_to_check[1]) and
        (set(json.loads(one_row[2]).items()) == model_group_values_to_check[2]) and 
        (set(one_row[3].split(',')) == model_group_values_to_check[3]) and
        (set(one_row[4].split('_')) == model_group_values_to_check[4])):
        
            print('found match; returning model group ID to use')
            
            mg_id_toreturn = mg_allcols[i][0]
            return(mg_id_toreturn)
            
            break
    
    return('mg_doesnt_exist')



def checkif_modelexists_forthatMG(modelgroup_id, train_uuid, test_uuid, label_imputation_method, creds):
    
    
    ## pull models and test id rows associated with that model id
    models_rows_query = """
    select dssg_results.models.model_id, training_matrix_uuid, label_imputation_method, 
    test_matrix_id
    from dssg_results.models 
    left join dssg_results.evaluations on dssg_results.models.model_id = dssg_results.evaluations.model_id
    where model_group_id = '{modelgroup_id}'
    """.format(modelgroup_id = modelgroup_id)
    
    ## read in models rows in df format 
    models_forthatID = civis.io.read_civis_sql(
        models_rows_query, database=creds['database'],
        credential_id = creds['civis_superuser']['civis_id'],use_pandas = True)
    
    ## check train data AND test data AND label imputation method (need to be all combined in same model)
    
    model_tocheck = train_uuid + "_" + test_uuid + "_" + label_imputation_method
    models_forthatID['training_matrix_uuid_str'] = models_forthatID['training_matrix_uuid'].astype(str)
    models_forthatID['test_matrix_id_str'] = models_forthatID['test_matrix_id'].astype(str)
    models_forthatID['label_imputation_method_str'] = models_forthatID['label_imputation_method'].astype(str)
    models_tocheck_in = models_forthatID[['training_matrix_uuid_str', 'test_matrix_id_str', 
                               'label_imputation_method_str']].apply(lambda x: '_'.join(x), axis=1)
    
    if models_tocheck_in.str.contains(model_tocheck).any() == True:
        
        print('model group already fit with that training data, test data, and label imputation method; skipping to next model')
        
        
        return('skip_fitting')
    
    else:
        
        print('model group not yet fit with this combination of training data, test data, or label imputation method')
        
        return('continue_fitting') 


def evaluation_methods(metrics, label_observed, label_predicted):
    evaluation_result = metrics(label_observed, label_predicted)
    return evaluation_result


print(str("functions and packages are: " + ", ".join([f for f in dir() if f[0] is not '_'])))
