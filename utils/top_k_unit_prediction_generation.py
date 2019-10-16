### define generation of precision and recall score for a building list that covers k units

# imports within function
import numpy as np
import pandas as pd
import random
from sklearn.metrics import confusion_matrix
from postgres_functions import *

### for main model

### function to return count of units
def pull_count_units(alchemy_connection):
    pull_units = """
    select address_id,internal_units_static
    FROM dssg_staging.entities_address_table_rs_clean
    WHERE   internal_peu_target_zip_static=1 and internal_units_static::int >= 6
    """
    units_table = readquery_todf_postgres(query = pull_units,
                                     alchemy_connection = alchemy_connection)
    return(units_table)


# ### function to generate confusion_matrix
# def confusion_matrix_atk(dataframe_at_k,label_suffix,confusion_results,confusion_results_list):

#     ### to do
#     dataframe_at_k['prediction_label_at_k'] = [1 for i in range(k+1)] + [0 for i in range(len(dataframe_for_matrix)-k+1)]
#     ### subset to not_missing
#     dataframe_not_missing = dataframe_at_k.loc[dataframe_at_k[label_name].notnull()].copy()
#     y_true = dataframe_not_missing[label_name]
#     y_pred = dataframe_not_missing['prediction_label_at_k']

#     confusion_matrix_array = confusion_matrix(y_true,y_pred,labels =[0,1])

#     confusion_tn = confusion_matrix_array[0][0]
#     confusion_fp = confusion_matrix_array[0][1]
#     confusion_fn = confusion_matrix_array[1][0]
#     confusion_tp = confusion_matrix_array[1][1]

#     confusion_results.extend([confusion_tn,confusion_fp,confusion_fn,confusion_tp])
#     confusion_results_list.extend(['confusion_true_negative_%s'%label_suffix,'confusion_false_positive_%s'%label_suffix,'confusion_false_negative_%s'%label_suffix,'confusion_true_positive_%s'%label_suffix])

#     return(confusion_results,confusion_results_list)


### function to generate precision-recall dataframe for non-imputed labels
def not_imputed_dataframe_for_metrics(testing_features_df,label_name):
    print(label_name)
    label_imputed_flag = label_name + '_flag'
    testing_features_df_not_imputed = testing_features_df.loc[testing_features_df[label_imputed_flag]==0].copy()
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


def imputed_dataframe_for_metrics(testing_features_df,label_name):

    label_imputed_flag = label_name + '_flag'

    ###-----------------------------------------------imputation dataframe-----------------------------------------------
    ### subset columns
    address_unit_proba_impute = testing_features_df[['address_id','month_start','internal_units_static','prediction_prob',label_name,label_imputed_flag]]

    ### sort values by prediction_prob and get the running sum for units
    address_unit_proba_impute = address_unit_proba_impute.sort_values(by = ['prediction_prob','internal_units_static'], ascending=False).copy()
    address_unit_proba_impute['num_units_so_far'] = address_unit_proba_impute['internal_units_static'].cumsum()

    ### label imputation
    address_unit_proba_impute[label_name+'_imputed_1'] = address_unit_proba_impute[label_name].fillna(1)
    address_unit_proba_impute[label_name+'_imputed_0'] = address_unit_proba_impute[label_name].fillna(0)

    ### label flip
    address_unit_proba_impute[label_name+'_imputed_1'+'_flip'] = 1 - address_unit_proba_impute[label_name+'_imputed_1']

    ### cumulative sum
    address_unit_proba_impute[label_name+'_cumsum'] = address_unit_proba_impute[label_name+'_imputed_0'].cumsum()
    address_unit_proba_impute[label_name+'_imputed_1'+'_cumsum'] = address_unit_proba_impute[label_name+'_imputed_1'].cumsum()
    address_unit_proba_impute[label_name+'_imputed_0'+'_cumsum'] = address_unit_proba_impute[label_name+'_imputed_0'].cumsum()
    address_unit_proba_impute[label_name+'_imputed_1'+'_flip'+'_cumsum'] = address_unit_proba_impute[label_name+'_imputed_1'+'_flip'].cumsum()
    address_unit_proba_impute[label_imputed_flag+'_cumsum'] = address_unit_proba_impute[label_imputed_flag].cumsum()

    ### add row number to get K
    address_unit_proba_impute['row_number'] = [i for i in range(1,len(address_unit_proba_impute)+1)]

    address_unit_proba_impute['k_proportion'] = address_unit_proba_impute.row_number.astype(float)/len(address_unit_proba_impute)
    ###-----------------------------------------------precisions-----------------------------------------------
    ### precision scores with all data points including missing values when selecting top K, numerator: labeled data, denominator: labeled data
    address_unit_proba_impute['precision_all'] = list(address_unit_proba_impute[label_name+'_cumsum'
                                                                               ].astype(float)/(address_unit_proba_impute['row_number']-address_unit_proba_impute[label_imputed_flag+'_cumsum']))
    address_unit_proba_impute['precision_all'] = address_unit_proba_impute['precision_all'].fillna(0)

    ### precision scores with all data points including missing values when selecting top K, numerator: labeled data, denominator: all data
    address_unit_proba_impute['precision_all_all'] = list(address_unit_proba_impute[label_name+'_cumsum'
                                                                               ].astype(float)/address_unit_proba_impute['row_number'])
    address_unit_proba_impute['precision_all_all'] = address_unit_proba_impute['precision_all_all'].fillna(0)

    ### precision score with label imputed as filling with 1 before K and filling with 0 after K
    address_unit_proba_impute['precision_upper_bound'] = list(address_unit_proba_impute[label_name+'_imputed_1'+'_cumsum'
                                                                                       ].astype(float)/address_unit_proba_impute['row_number'])

    ### precision score with label imputed as filling with 0 before K and filling with 1 after K
    address_unit_proba_impute['precision_lower_bound'] = list(address_unit_proba_impute[label_name+'_imputed_0'+'_cumsum'
                                                                                       ].astype(float)/address_unit_proba_impute['row_number'])

    ###-----------------------------------------------recalls-----------------------------------------------

    ### recall score for label 1 with all data points including missing values when determine K (K including missing values, drop missing values after determine K)
    address_unit_proba_impute['recall_all'] = address_unit_proba_impute[label_name+'_cumsum'].astype(float)/sum(address_unit_proba_impute[label_name+'_imputed_0'])


    ### recall score for label 0 with all data points including missing values when determine K (K including missing values, drop missing values after determine K)
    address_unit_proba_impute['recall_of_0_all'] = address_unit_proba_impute[label_name+'_imputed_1'+'_flip'+'_cumsum'
                                                                            ].astype(float)/sum(address_unit_proba_impute[label_name+'_imputed_1'+'_flip'])

    ### recall score for missing labels imputed as 1
    address_unit_proba_impute['recall_imputed_1'] = address_unit_proba_impute[label_name+'_imputed_1'+'_cumsum'
                                                                             ].astype(float)/sum(address_unit_proba_impute[label_name+'_imputed_1'])

    ### recall score for missing lables imputed as 1 when <=K and imputed as 0 when > K
    address_unit_proba_impute['recall_upper_bound'] = address_unit_proba_impute[label_name+'_imputed_1'+'_cumsum'
                                                                             ].astype(float)/(
                                                      address_unit_proba_impute[label_name+'_imputed_1'+'_cumsum'
                                                      ] + sum(address_unit_proba_impute[label_name+'_imputed_0']) - address_unit_proba_impute[label_name+'_imputed_0'+'_cumsum'])

    ### recall score for missing lables imputed as 0 when <=K and imputed as 1 when > K
    address_unit_proba_impute['recall_lower_bound'] = address_unit_proba_impute[label_name+'_imputed_0'+'_cumsum'
                                                                             ].astype(float)/(
                                                      address_unit_proba_impute[label_name+'_imputed_0'+'_cumsum'
                                                      ] + sum(address_unit_proba_impute[label_name+'_imputed_1']) - address_unit_proba_impute[label_name+'_imputed_1'+'_cumsum'])


    return address_unit_proba_impute

def get_last_k_row_number(dataframe,k):
    if list(dataframe.num_units_so_far)[-1] >= k:
        last_row = list(dataframe.loc[dataframe.num_units_so_far >= k].row_number)[0]
    ### if the sum of the units in this dataframe is less than k, return the last row number
    else:
        last_row = list(dataframe.row_number)[-1]
    return last_row

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


def top_k_unit_prediction_generation(k,testing_features_df,label_prob,
                                    label_name,
                                    confusion_matrix_atk,
                                    not_imputed_dataframe_for_metrics, 
                                    imputed_dataframe_for_metrics, 
                                    get_last_k_row_number, 
                                    plot_precision_recall_proportion_plot,
                                    plot_i,
                                    save_figure_to_amzS3,
                                    model_id,
                                    model_type,
                                    creds):


    ### parameter definition
    ### k: number of units to target in next month
    ### testing_features_df: df that has label,feature and everything for testing
    ### label_prob: predicted proba generated by model
    print('top_k starts')
    print(label_name)


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

    ###-----------------------------------------------impute=None-----------------------------------------------

    ### subset to non-imputed

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

    ###-----------------------------------------------imputation dataframe-----------------------------------------------

    ### all metrics here: ['precision_all'],['precision_upper_bound'],['precision_lower_bound'],['recall_all'],['recall_of_0_all']
    # ['recall_imputed_1'],['recall_upper_bound'],['recall_lower_bound']


    ### get the dataframe with imputation
    dataframe_imputed = imputed_dataframe_for_metrics(testing_features_df=testing_features_df,label_name=label_name)
    ### get the row number of last row in k
    last_row_imputed = get_last_k_row_number(dataframe=dataframe_imputed,k=k)
    ### retrieve the precision and recall score in this row and append to the results list

    precision_all = dataframe_imputed.iloc[last_row_imputed-1]['precision_all']
    precision_upper_bound = dataframe_imputed.iloc[last_row_imputed-1]['precision_upper_bound']
    precision_lower_bound = dataframe_imputed.iloc[last_row_imputed-1]['precision_lower_bound']
    recall_all = dataframe_imputed.iloc[last_row_imputed-1]['recall_all']
    recall_of_0_all = dataframe_imputed.iloc[last_row_imputed-1]['recall_of_0_all']
    recall_imputed_1 = dataframe_imputed.iloc[last_row_imputed-1]['recall_imputed_1']
    recall_upper_bound = dataframe_imputed.iloc[last_row_imputed-1]['recall_upper_bound']
    recall_lower_bound = dataframe_imputed.iloc[last_row_imputed-1]['recall_lower_bound']

    ### append to the results list
    precision_results.extend([precision_all,precision_upper_bound,precision_lower_bound])
    precision_results_list.extend(['precision_all','precision_upper_bound','precision_lower_bound'])
    recall_results.extend([recall_all,recall_of_0_all,recall_imputed_1,recall_upper_bound,recall_lower_bound])
    recall_results_list.extend(['recall_all','recall_of_0_all','recall_imputed_1','recall_upper_bound','recall_lower_bound'])


    ###-----------------------------------------------confusion matrix-----------------------------------------------


    ## then, take the observed versus predicted labels
    ## and generate a confusion matrix

    ### to do: 1. need to modify the input of confusion_matrix; 2. calculate confusion_matrix for none, all_zero, all_one, upper_bound, lower_bound


    # confusion_matrix_not_imputed
    (confusion_results,confusion_results_list) = confusion_matrix_atk(dataframe = dataframe_not_imputed,
                                                                      last_row = last_row_not_imputed,
                                                                      label_suffix='_no_impute',
                                                                      label_name = label_name,
                                                                      confusion_results=confusion_results,
                                                                      confusion_results_list=confusion_results_list)

    # confusion_matrix_imputed_1
    (confusion_results,confusion_results_list) = confusion_matrix_atk(dataframe = dataframe_imputed,
                                                                      last_row = last_row_imputed,
                                                                      label_suffix='_imputed_1',
                                                                      label_name = label_name,
                                                                      confusion_results=confusion_results,
                                                                      confusion_results_list=confusion_results_list)

    # confusion_matrix_imputed_0
    (confusion_results,confusion_results_list) = confusion_matrix_atk(dataframe = dataframe_imputed,
                                                                      last_row = last_row_imputed,
                                                                      label_suffix='_imputed_0',
                                                                      label_name = label_name,
                                                                      confusion_results=confusion_results,
                                                                      confusion_results_list=confusion_results_list)

    # confusion_matrix_upper_bound
    (confusion_results,confusion_results_list) = confusion_matrix_atk(dataframe = dataframe_imputed,
                                                                      last_row = last_row_imputed,
                                                                      label_suffix='_upper_bound',
                                                                      label_name = label_name,
                                                                      confusion_results=confusion_results,
                                                                      confusion_results_list=confusion_results_list)

    # confusion_matrix_lower_bound
    (confusion_results,confusion_results_list) = confusion_matrix_atk(dataframe = dataframe_imputed,
                                                                      last_row = last_row_imputed,
                                                                      label_suffix='_lower_bound',
                                                                      label_name = label_name,
                                                                      confusion_results=confusion_results,
                                                                      confusion_results_list=confusion_results_list)


    ###-----------------------------------------------add all result metrics lists together-------------------------------------------------


    evaluation_results_all = precision_results + recall_results + confusion_results
    evaluation_list_all = precision_results_list + recall_results_list + confusion_results_list

    ###-----------------------------------------------plotting-------------------------------------------------



    ### precision_all
    plt.figure(plot_i)
    plt.style.use('ggplot')
    sns.set_style("whitegrid", {'axes.grid' : False})
    plot_precision_recall_proportion_plot(evals_df=dataframe_imputed,
           figure_type='precision_all', model_type=model_type, save_figure_to_amzS3=save_figure_to_amzS3, model_id=model_id, creds=creds)

    plt.show()
    plot_i = plot_i + 1

    ### precision_all
    plt.figure(plot_i)
    plt.style.use('ggplot')
    sns.set_style("whitegrid", {'axes.grid' : False})
    plot_precision_recall_proportion_plot(evals_df=dataframe_imputed,
           figure_type='precision_all_all', model_type=model_type, save_figure_to_amzS3=save_figure_to_amzS3, model_id=model_id, creds=creds)

    plt.show()
    plot_i = plot_i + 1


    ### precision_precision
    plt.figure(plot_i)
    plt.style.use('ggplot')
    sns.set_style("whitegrid", {'axes.grid' : False})
    plot_precision_recall_proportion_plot(evals_df=dataframe_imputed,
                           figure_type='precision_precision', model_type=model_type, save_figure_to_amzS3=save_figure_to_amzS3, model_id=model_id, creds=creds)
    plt.show()
    plot_i = plot_i + 1


    ### precision_recall
    plt.figure(plot_i)
    plt.style.use('ggplot')
    sns.set_style("whitegrid", {'axes.grid' : False})
    plot_precision_recall_proportion_plot(evals_df=dataframe_not_imputed,
                           figure_type='precision_recall', model_type=model_type, save_figure_to_amzS3=save_figure_to_amzS3, model_id=model_id, creds=creds)
    plt.show()
    plot_i = plot_i + 1

    ### recall_recall
    plt.figure(plot_i)
    plt.style.use('ggplot')
    sns.set_style("whitegrid", {'axes.grid' : False})
    plot_precision_recall_proportion_plot(evals_df=dataframe_imputed,
                           figure_type='recall_recall', model_type=model_type, save_figure_to_amzS3=save_figure_to_amzS3, model_id=model_id, creds=creds)
    plt.show()
    plot_i = plot_i + 1


    return (evaluation_results_all, evaluation_list_all, plot_i)


### get top k list after rn the model such as for calculating jaccard index

def get_top_k_address_list(k, df_model):
    address_unit_proba = df_model[['entity_id','internal_units_static','score']]
    ### sort values by prediction_prob and get the running sum for units
    address_unit_proba = address_unit_proba.sort_values('score', ascending=False).copy()
    address_unit_proba['num_units_so_far'] = address_unit_proba['internal_units_static'].cumsum()

    ### average number of unit knocks per month: k=5645 for now, cut till last number that is no smaller than 5645
    address_unit_proba.reset_index(inplace=True)
    last_index = address_unit_proba.loc[address_unit_proba.num_units_so_far >= k].index[0]
    test_pred_positive = address_unit_proba[:last_index+1]
    print('unit_sum: ',test_pred_positive.internal_units_static.sum())
    address_list = list(test_pred_positive.entity_id)
    return address_list

### get top k dataframe for drawing map

def get_top_k_df(k, df_model):
    address_unit_proba = df_model[['entity_id','internal_units_static','score',
                                   'internal_longitude_static', 'internal_latitude_static']]
    ### sort values by prediction_prob and get the running sum for units
    address_unit_proba = address_unit_proba.sort_values('score', ascending=False).copy()
    address_unit_proba['num_units_so_far'] = address_unit_proba['internal_units_static'].cumsum()

    ### average number of unit knocks per month: k=5645 for now, cut till last number that is no smaller than 5645
    address_unit_proba.reset_index(inplace=True)
    last_index = address_unit_proba.loc[address_unit_proba.num_units_so_far >= k].index[0]
    test_pred_positive = address_unit_proba[:last_index+1]
    print('unit_sum: ',test_pred_positive.internal_units_static.sum())
    top_k_with_la_long_df = test_pred_positive[['entity_id','score','internal_longitude_static', 'internal_latitude_static']]
    return top_k_with_la_long_df






