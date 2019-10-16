def clean_results_df(data, featurelist_length_threshold, metrics_tocompare):

  
    columns_nonduplicated = ['model_group_id', 'borough_predicted_for', 'borough_fit_on', 'test_set_month', 'metric', 'top_k']
    data_uniquerows = data.drop_duplicates(subset = columns_nonduplicated).copy()
    
    data_uniquerows['feature_list_length'] = data_uniquerows['feature_list'].apply(lambda x: len(x))
#     return_results_df_uniquerows.metric.value_counts()
## for now, subset to any case label
    data_touse =  data_uniquerows[data_uniquerows.feature_list_length > featurelist_length_threshold].copy()

    data_vars = data_touse[['model_group_id', 'model_id',
                                    'algorithm_type', 'metric', 'value', 'top_k', 'training_end_date',
                                      'hyperparameters', 'test_set_month', 'borough_predicted_for','borough_fit_on', 'feature_list',
                           'feature_list_length', 'pred_score_range']]
    
    data_vars['model_and_hyp'] = data_vars.algorithm_type + data_vars.hyperparameters
    
    data_vars_subsetmetrics = data_vars.loc[data_vars.metric.isin(metrics_tocompare),
                     ['algorithm_type', 'hyperparameters', 'model_and_hyp', 'metric', 'value',
                      'top_k', 'training_end_date', 'model_id',
                        'test_set_month', 'borough_predicted_for', 'borough_fit_on']]
    
    return data_vars_subsetmetrics


def reshape_wide_genmetrics(data):
    
    ## reshape to wide
    results_wide = pd.pivot_table(data, 
                                        index = ['algorithm_type', 'hyperparameters', 'model_and_hyp', 'borough_fit_on',
                                                 'borough_predicted_for', 
                                                 'training_end_date', 'top_k', 'model_id'],
                                    columns = ['metric'],
                                      values = ['value']).reset_index()

    results_wide.columns = ['algorithm_type', 'hyperparameters', 'model_and_hyp', 'borough_fit_on',
                                'borough_predicted_for',
                                'split_date', 'top_k', 'model_id', 'precision_baseline', 'precision_no_imputed', 
                                'recall_of_0', 'recall_of_1']
    results_wide['split_date_dateversion'] = pd.to_datetime(results_wide.split_date, format = '%Y-%m-%d')
    results_wide['test_set_month'] = results_wide.split_date_dateversion + pd.DateOffset(months=1)
    
    ## convert top k to percentages (change if we change top k parameter)
    results_wide['top_k_rank'] = results_wide.groupby(['algorithm_type', 'hyperparameters', 'model_and_hyp', 'split_date', 'model_id',
                                                                  'borough_predicted_for', 'borough_fit_on'])['top_k'].rank(ascending = True)

    top_k_percent_list = [0.01, 0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5]
    top_k_rank_list = [i for i in range(1,9)]
    top_k_dictionary = dict(zip(top_k_rank_list, top_k_percent_list))

    results_wide = results_wide.replace({'top_k_rank': top_k_dictionary})      
    
    ## generate metrics
    results_wide['precision_ratio'] = results_wide.precision_no_imputed/results_wide.precision_baseline
    results_wide['recall_ratio'] = np.where((results_wide.recall_of_0 == 0) & (results_wide.recall_of_1 !=0), 
                                                100,
                                                np.where((results_wide.recall_of_0 == 0) & (results_wide.recall_of_1 ==0),
                                                0, results_wide.recall_of_1/
                                               results_wide.recall_of_0))
    
    ## add a pd time column for easier subsetting of months
    results_wide['test_set_month_pdtime'] = results_wide.test_set_month.astype(datetime.datetime)
    
    return(results_wide)


def return_all_split_dates(data):
    results_summary = data[['model_and_hyp', 'test_set_month_pdtime']].drop_duplicates().sort_values(by = 'model_and_hyp')
    grouped_models = results_summary.groupby(['model_and_hyp'])
    all_summary = []
    for group, data in grouped_models:
        unique_split_dates = sorted(data.test_set_month_pdtime.astype(str).str.replace(' 00:00:00', '').unique())
        joined_split_dates = '; '.join(unique_split_dates)
        model_and_hyp = [data.model_and_hyp.iloc[0]]
        summary_data = pd.DataFrame({'model_and_hyp': model_and_hyp,
                                'test_set_months_all': joined_split_dates})
        all_summary.append(summary_data)
    all_summary_df = pd.concat(all_summary)
    return(all_summary_df)

def return_all_modelIDs(data):
    
    modID_summary = data[['model_and_hyp', 'model_id']].drop_duplicates().sort_values(by = 'model_and_hyp')
    grouped_models = modID_summary.groupby(['model_and_hyp'])
    all_summary = []
    for group, data in grouped_models:
        unique_modIDs = data.model_id.astype(str).unique()
        joined_split_dates = '; '.join(unique_modIDs)
        model_and_hyp = [data.model_and_hyp.iloc[0]]
        summary_data = pd.DataFrame({'model_and_hyp': model_and_hyp,
                                'model_id_all': joined_split_dates}).drop_duplicates()
        all_summary.append(summary_data)
    all_summary_df = pd.concat(all_summary)
    return(all_summary_df)

def summarize_evals_all(data, topk_include, testmonths_include, which_metric):
    
    data_to_evaluate_initial = data[(data.top_k_rank.isin(topk_include)) &
                           (data.test_set_month_pdtime.isin(testmonths_include))].copy()
  
    
    models_alldates = list(data_to_evaluate_initial.model_and_hyp.value_counts()[data_to_evaluate_initial.model_and_hyp.value_counts() >= 
                    len(testmonths_include)].index)

    
    data_to_evaluate = data_to_evaluate_initial[data_to_evaluate_initial.model_and_hyp.isin(models_alldates)].copy()
    
    if which_metric == 'precision_ratio':
        
        metric_summary = data_to_evaluate.groupby(['model_and_hyp']).agg({'precision_ratio':['mean', 'min', 'max']}).reset_index()
       
    
        metric_summary.columns = ['model_and_hyp', 'precision_ratio_mean', 'precision_ratio_min', 'precision_ratio_max']
    
        metric_summary_sorted = metric_summary.sort_values(by = 'precision_ratio_mean', ascending = False)
    
        ## summary of model IDs and split dates attached to each rank
        test_dates_summary = return_all_split_dates(data_to_evaluate)
        ids_summary = return_all_modelIDs(data_to_evaluate)
    
        ## model ID and split date of max precision ratio
        
        max_ID_split = metric_summary_sorted[['model_and_hyp', 'precision_ratio_max']].merge(data_to_evaluate[['model_and_hyp',
                                                    'test_set_month_pdtime',
                                                    'model_id',
                                                    'precision_ratio']], 
                                      left_on = ['model_and_hyp', 'precision_ratio_max'],
                                      right_on = ['model_and_hyp', 'precision_ratio']).drop_duplicates()
        
        max_ID_toreturn = max_ID_split[['model_and_hyp', 'test_set_month_pdtime', 'model_id']]
        max_ID_toreturn.columns = ['model_and_hyp', 'testsetmonth_ofmax', 'modelid_ofmax']
        max_ID_toreturn['testmonth_ofmax'] = max_ID_toreturn.testsetmonth_ofmax.astype(str).str.replace(' 00:00:00', '')
        max_ID_toreturn_final = max_ID_toreturn[['model_and_hyp', 'testmonth_ofmax', 'modelid_ofmax']]
        
        
        min_ID_split = metric_summary_sorted[['model_and_hyp', 'precision_ratio_min']].merge(data_to_evaluate[['model_and_hyp',
                                                    'test_set_month_pdtime',
                                                    'model_id',
                                                    'precision_ratio']], 
                                      left_on = ['model_and_hyp', 'precision_ratio_min'],
                                      right_on = ['model_and_hyp', 'precision_ratio']).drop_duplicates()
        
        min_ID_toreturn = min_ID_split[['model_and_hyp', 'test_set_month_pdtime', 'model_id']]
        min_ID_toreturn.columns = ['model_and_hyp', 'testsetmonth_ofmin', 'modelid_ofmin']
        min_ID_toreturn['testmonth_ofmin'] = min_ID_toreturn.testsetmonth_ofmin.astype(str).str.replace(' 00:00:00', '')
        min_ID_toreturn_final = min_ID_toreturn[['model_and_hyp', 'testmonth_ofmin', 'modelid_ofmin']]
        
        ## bind and return
        metric_summary_merged = metric_summary_sorted.merge(test_dates_summary, on = 'model_and_hyp', how = 'inner').merge(ids_summary, 
                                on = 'model_and_hyp', how = 'inner').merge(max_ID_toreturn_final, on = 'model_and_hyp',
                                how = 'inner').merge(min_ID_toreturn_final, on = 'model_and_hyp', how = 'inner')
        

        return(metric_summary_merged)
    
    elif which_metric == 'recall_ratio':
        
        metric_summary = data_to_evaluate.groupby(['model_and_hyp']).agg({'recall_ratio': ['mean', 'min', 'max']}).reset_index()
    

    
        metric_summary.columns = ['model_and_hyp', 'recall_ratio_mean', 'recall_ratio_min', 'recall_ratio_max']
    
        metric_summary_sorted = metric_summary.sort_values(by = 'recall_ratio_mean', ascending = False)
    
        ## summary of model IDs and split dates attached to each rank
        test_dates_summary = return_all_split_dates(data_to_evaluate)
        ids_summary = return_all_modelIDs(data_to_evaluate)
    
        
        ## model ID and split date of max precision ratio
        
        max_ID_split = metric_summary_sorted[['model_and_hyp', 'recall_ratio_max']].merge(data_to_evaluate[['model_and_hyp',
                                                    'test_set_month_pdtime',
                                                    'model_id',
                                                    'recall_ratio']], 
                                      left_on = ['model_and_hyp', 'recall_ratio_max'],
                                      right_on = ['model_and_hyp', 'recall_ratio']).drop_duplicates()
        
        max_ID_toreturn = max_ID_split[['model_and_hyp', 'test_set_month_pdtime', 'model_id']]
        max_ID_toreturn.columns = ['model_and_hyp', 'testsetmonth_ofmax', 'modelid_ofmax']
        max_ID_toreturn['testmonth_ofmax'] = max_ID_toreturn.testsetmonth_ofmax.astype(str).str.replace(' 00:00:00', '')
        max_ID_toreturn_final = max_ID_toreturn[['model_and_hyp', 'testmonth_ofmax', 'modelid_ofmax']]
        
        
        min_ID_split = metric_summary_sorted[['model_and_hyp', 'recall_ratio_min']].merge(data_to_evaluate[['model_and_hyp',
                                                    'test_set_month_pdtime',
                                                    'model_id',
                                                    'recall_ratio']], 
                                      left_on = ['model_and_hyp', 'recall_ratio_min'],
                                      right_on = ['model_and_hyp', 'recall_ratio']).drop_duplicates()
        
        min_ID_toreturn = min_ID_split[['model_and_hyp', 'test_set_month_pdtime', 'model_id']]
        min_ID_toreturn.columns = ['model_and_hyp', 'testsetmonth_ofmin', 'modelid_ofmin']
        min_ID_toreturn['testmonth_ofmin'] = min_ID_toreturn.testsetmonth_ofmin.astype(str).str.replace(' 00:00:00', '')
        min_ID_toreturn_final = min_ID_toreturn[['model_and_hyp', 'testmonth_ofmin', 'modelid_ofmin']]
        
        ## bind and return
        metric_summary_merged = metric_summary_sorted.merge(test_dates_summary, on = 'model_and_hyp', how = 'inner').merge(ids_summary, 
                                on = 'model_and_hyp', how = 'inner').merge(max_ID_toreturn_final, on = 'model_and_hyp',
                                how = 'inner').merge(min_ID_toreturn_final, on = 'model_and_hyp', how = 'inner')
        
        
        
    

        return(metric_summary_merged)
        
    