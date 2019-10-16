import sqlalchemy
from postgres_functions import *


def pull_from_eval(alchemy_connection, id_lower = 0, id_upper=900000000, subset_list = False, id_list = '(0)', 
                label_topull = 'internal_cases_opened_any_next_month', 
                boroughs_fit_topull = 'Bronx_Queens_Staten Island_Brooklyn_Manhattan', 
                maxgaptraintest_topull = 40):
    
    if subset_list == True:
        
        print('pulling ID list')
            
        pull_results_info = """
                select * from dssg_results.eval_meta 
                where model_id in {id_list}
                and label = '{label_topull}'
                and borough_fit_on = '{boroughs_fit_topull}'
                order by model_id desc;
                """.format(id_list = id_list,
                          label_topull = label_topull,
                          boroughs_fit_topull = boroughs_fit_topull)

                ## pull from database
        df_eval = readquery_todf_postgres(sqlalchemy.text(pull_results_info), 
                                          alchemy_connection)

        return(df_eval)
            
    else: 
        
        print('pulling ids in range')
            
        pull_results_info = """
                select * from dssg_results.eval_meta 
                where model_id >= {id_lower}
                and model_id <= {id_upper}
                and label = '{label_topull}'
                and borough_fit_on = '{boroughs_fit_topull}'
                order by model_id desc;
                """.format(id_lower = id_lower, id_upper = id_upper,
                          label_topull = label_topull,
                          boroughs_fit_topull = boroughs_fit_topull)

                    ## pull from database
        df_eval = readquery_todf_postgres(sqlalchemy.text(pull_results_info), 
                                          alchemy_connection)

        return(df_eval)
      
## function to clean results
def clean_results_df(data, label_tosubset = 'internal_cases_opened_any_next_month'):

  
    columns_nonduplicated = ['model_group_id', 'test_set_month', 'metric', 'top_k']
    data_uniquerows = data.drop_duplicates(subset = columns_nonduplicated).copy()
#     return_results_df_uniquerows.metric.value_counts()
## for now, subset to any case label
    data_touse =  data_uniquerows[data_uniquerows.label == label_tosubset].copy()
    data_dedup = data_touse[['model_group_id', 'model_id',
                                    'algorithm_type', 'metric', 'value', 'top_k', 
                                      'hyperparameters', 'test_set_month', 'feature_list']]
    return data_dedup

## function to reshape the metrics  
def reshape_wide_genmetrics(results_dedup, date_cutoff_foreval):
    
    ## create model and hyp column
    results_dedup['model_and_hyp'] = results_dedup.algorithm_type.astype(str) + "_" + results_dedup.hyperparameters.astype(str)
    
    ## subset to complete results and make sure
    ## value column is float type
    results_dedup_complete = results_dedup[results_dedup.metric.notnull()].copy()
    results_dedup_complete['value'] = results_dedup_complete.value.astype(float)
    
    ## reshape to wide
    results_wide = pd.pivot_table(results_dedup_complete, index = ['algorithm_type', 'hyperparameters', 'model_and_hyp', 
                                                'top_k', 'model_id', 'model_group_id',
                                                'test_set_month'],
                                   columns = ['metric'],
                                values = ['value']).reset_index()
    
    ## rename columns
    results_wide.columns = ['algorithm_type', 'hyperparameters', 'model_and_hyp', 
                        'top_k', 'model_id', 'model_group_id',
                        'test_set_month',
                        'confusion_fn', 'confusion_fp',
                        'confusion_tn', 'confusion_tp',
                        'precision_baseline', 'precision_at_k',
                        'recall_of_1']
    
    ## clean date
    results_wide['test_set_month_dateversion'] =  pd.to_datetime(results_wide.test_set_month,
                                                            format = '%Y-%m-%d')

    ## generate ratios
    results_wide['precision_ratio'] = results_wide.precision_at_k/results_wide.precision_baseline
    
    ## return to objects
    results_wide_validation = results_wide[results_wide.test_set_month < date_cutoff_foreval].copy()
    results_wide_test = results_wide[results_wide.test_set_month >= date_cutoff_foreval].copy()
    
    return(results_wide_validation, results_wide_test)
    
