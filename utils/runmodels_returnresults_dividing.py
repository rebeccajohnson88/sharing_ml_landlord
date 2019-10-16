pip.main(['install', 'retrying'])
from retrying import retry

def runmodels_returnresults(label_name,
                           label_imputation_method,
                            feature_list,
                    training_features_df,
                    testing_features_df,
                    unit_id,
                    time_id,
                    borough_fit_list,
                    borough_predict_list,
                    model_list,
                    top_k_list,
                    months_nolabels,
                    creds,
                    model_comment_toadd = 'None',
                    fileid_table_comment_toadd = 'None'):


    ## note: for subsequent steps, it's useful to have the
    ## data preserved in addition to an np.array because data
    ## is needed for things like feature names



    ## create matrix of training features (removed reshape command)
    training_features_df = training_features_df.loc[training_features_df.internal_borough_factor_static.isin(pd.Series(borough_fit_list))].copy()
    x_train_features = training_features_df[feature_list]
    x_train_mat = x_train_features.values


    ## create matrix of test features
    testing_features_df = testing_features_df.loc[testing_features_df.internal_borough_factor_static.isin(pd.Series(borough_predict_list))].copy()
    x_test_features = testing_features_df[feature_list]
    #x_test_mat = x_test_features.values


    ## get correct label name for subsetting
    label_name_tosubset = label_name + "_imputed"
    label_imputed_flag = label_name_tosubset + "_flag"


    ## create training and test labels
    label_train = training_features_df[label_name_tosubset]
    label_test = testing_features_df[label_name_tosubset]
    label_train_mat = label_train.values
    label_test_mat = label_test.values

    ## create training and test matrix to return uuids for
    x_train_features_foruuid = pd.concat([training_features_df[unit_id], training_features_df[time_id],
          x_train_features, label_train], axis = 1)

    x_test_features_foruuid = pd.concat([testing_features_df[unit_id], testing_features_df[time_id],
          x_test_features, label_test], axis = 1)

    ## SKIP BECAUSE HAVE TABLE
    (train_uuid, train_uuid_exists) = store_civisS3_logID(name_storefile = 'training_mat',  schema_storefile = 'NotApplicable',
                object_tostore = x_train_features_foruuid,
                datatype_formeta_table = 'NotApplicable',
                type_query = 'insert_new',
                where_store = 'file_ids', # where store parameter to store in fileids or s5
                storeas_type = 'hdf',
                 nycopen_api = False, creds = creds,
                     return_fileID = False,
                     return_uuid = True,
                     return_uuid_exists = True,
                     training_or_test_matrix = True,
                     time_id = time_id,
                    optional_comment = fileid_table_comment_toadd,
                    optional_borough_predict_list = borough_predict_list)



    (test_uuid, test_uuid_exists) = store_civisS3_logID(name_storefile = 'training_mat', schema_storefile = 'NotApplicable',
                                                        object_tostore = x_test_features_foruuid,
                 datatype_formeta_table = 'NotApplicable',
                 type_query = 'insert_new',
                 where_store = 'file_ids', # where store parameter to store in fileids or s5
                 storeas_type = 'hdf',
                 nycopen_api = False, creds = creds,
                     return_fileID = False,
                     return_uuid = True,
                     return_uuid_exists = True,
                     training_or_test_matrix = True,
                     time_id = time_id,
                    optional_comment = fileid_table_comment_toadd)




    modelgroup_table = 'model_group'
    model_group_columns_to_insert = get_table_col(creds = creds,table = modelgroup_table,get_cols_intable_redshift=get_cols_intable_redshift)
    print("extracted columns for model_group table")

    ### pass value to table (models) for get_table_col function
    models_table = 'models'
    models_columns_to_insert = get_table_col(creds = creds,table = models_table,get_cols_intable_redshift=get_cols_intable_redshift)
    print("extracted columns for models")

    print(model_group_columns_to_insert)
    print(models_columns_to_insert)


    plot_i_out = 1

    for i in range(0, len(model_list)):

        ## pull out model
        one_model = model_list[i]

        (model_group_values_to_insert,
         model_group_values_to_check) = create_modelgroup_row(model_call = one_model,
                                                training_df = x_train_features,
                                                label_name = label_name,
                                                borough_fit_list = borough_fit_list)

        print("created row for model_group table")

        ## SKIP: before inserting, check if model group values already exists
        modelgroup_id = checkif_modelgroup_exists(model_group_values_to_check= model_group_values_to_check,
                             creds = creds)




        print(modelgroup_id)



        if modelgroup_id == 'mg_doesnt_exist':

#             ### pass value to schematable_insert for sql_insert_function
            model_group_schematable_insert = 'dssg_results.model_group'
            sql_insert_function(model_group_schematable_insert,
                             model_group_columns_to_insert,
                             model_group_values_to_insert,
                             creds=creds)

            ## check recent inserts and return ID
            modelgroup_id = checkif_modelgroup_exists_recent(model_group_values_to_check=
                            model_group_values_to_check,
                             creds = creds)

            modelgroup_row = get_row_model_group_fromid(modelgroup_id,creds=creds)

            # old code: got last row
            # modelgroup_row = get_last_row_model_group(creds=creds)

            print("model group DID NOT exist: updated row for model_group table and retrieving most recent row")

        else:

            modelgroup_row = get_row_model_group_fromid(modelgroup_id,creds=creds)

            print('model group DID exist: retrieving row associated with that model group')


             ## check if that model group has been combined with same training, test, and label impute
            modelexists_result = checkif_modelexists_forthatMG(modelgroup_id = modelgroup_id,
                                        train_uuid = train_uuid,
                                        test_uuid = test_uuid,
                                        label_imputation_method = label_imputation_method,creds=creds)


            if modelexists_result == 'skip_fitting':

                 continue


        ## fit model
        one_model.fit(x_train_features, label_train_mat)


        print("fit model: " + modelgroup_row['algorithm_type'])

         ## after fitting model, create row for
        ## models table




        model_values_to_insert = create_models_row(rows_modelgroup = modelgroup_row,
                                                 feature_list = feature_list,
                                                    train_uuid = train_uuid,
                                                    creds = creds,
                                                    model_call = one_model,
                                                    label_imputation_method = label_imputation_method,
                                                    model_comment_toadd = model_comment_toadd)

        print('created row for models table')

#         ### pass value to schematable_insert for sql_insert_function
        models_schematable_insert = 'dssg_results.models'
        sql_insert_function(models_schematable_insert,models_columns_to_insert,
                                 model_values_to_insert,creds)

        print("updated row for models table")

       ## query to get models_row
       ## ADD - get most recent rows and return match
        models_row = return_matching_models_row(modelgroup_id = modelgroup_id, train_uuid = train_uuid, creds = creds)

        #models_row = get_last_row_models(creds=creds)
        #print("retrieved last row of models table")

        model_type = models_row['model_type'][0]
        model_id = models_row['model_id'][0]




        if models_row.model_type.isin(['DecisionTreeClassifier','RandomForestClassifier','AdaBoostClassifier',
                                      'GradientBoostingClassifier'])[0]:
             importance_array = one_model.feature_importances_.copy()



        elif models_row.model_type.isin(['LogisticRegression','LogisticRegressionCV','Lasso','LassoCV','RidgeClassifier','RidgeClassifierCV'])[0]:
        ### this alternative line is for linear SVC that has a .coef_ function
        # elif models_row.model_type.isin(['LogisticRegression','LogisticRegressionCV','Lasso','LassoCV','RidgeClassifier','RidgeClassifierCV','SVC'])[0]:
             importance_array = list(one_model.coef_.tolist()[0])

        ## generate rows for feature importances
        featureimportances_df = create_featureimportances_rows(importance_array = importance_array, modelgroup_row = modelgroup_row,
                                                            models_row = models_row)

        print('created rows for feature importances table')
        start_insert_feature_importance = time.time()

        insert_feature_importance_table_exec = civis.io.dataframe_to_civis(featureimportances_df,database=creds['database'],
                                                        table='dssg_results.feature_importances',
                                                        credential_id = creds['civis_superuser']['civis_id'],
                                                        existing_table_rows='append')
        insert_feature_importance_table_exec.result()
        print('insert featureimportances_df into database')
        print('feature importance inserted in: ', time.time()-start_insert_feature_importance)


        ## plot the feature importances and return graph
        plt.figure(plot_i_out)
        featureimportance_graph = plot_feature_imp(feature_importance_data = featureimportances_df,
                              mg_row = modelgroup_row, theme = theme(panel_grid = element_blank(),
                              panel_background = element_rect(fill = "white", colour = "black"),
                              axis_text_x = element_text(color = "black", angle = 90, hjust = 1),
                              axis_text_y = element_text(color = "black"),
                            legend_background = element_blank()))
        plt.show()
        plot_i_out = plot_i_out + 1
        featureimportance_graph.save('featimport.png', dpi = 250, width = 12, height = 8)
    
        ### save file to Amazon S3
        #@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
        save_figure_to_amzS3(filename='featimport.png',
               destination='nyc_peu_inspections/output/figures_modelruns/feature_importances/featureimportance_{model_type}_{model_id}.png'.format(model_type=model_type,                model_id=model_id),
                creds = creds)


        ## generate rows for predictions

        ## loop over months in the test set

        ### step one: group the data version of the test set
        df_test_grouped = testing_features_df.groupby([time_id])

        ## iterate over the groups
        for data, group in df_test_grouped:

            ## add print statement
            print('running test set month: ' + data)

            ## turn grouped data into array
            x_test_features_group = group[feature_list]
            x_test_mat = x_test_features_group.values

            ## generate predictions for all

            ### first get test labels (binary prediction + continuous prob)
            label_prob = one_model.predict_proba(x_test_mat)

            ## add loop over top k
            for top_k in top_k_list:

                predictions_df = create_predictions_rows(testing_features_df = group, #changed to be group
                            models_row = models_row,
                            label_prob = label_prob, unit_id = unit_id, time_id = time_id,
                            top_k = top_k,label_imputed_flag=label_imputed_flag,
                            borough_predict_list = borough_predict_list)

                print('created rows for predictions table')
                # start_insert = time.time()
                insert_table_exec = civis.io.dataframe_to_civis(predictions_df,database=creds['database'],
                                                        table='dssg_results.predictions',
                                                        credential_id = creds['civis_superuser']['civis_id'],
                                                        existing_table_rows='append')
                insert_table_exec.result()
                # time_used = time.clock() - start_insert
                print('len(predictions_df): ', len(predictions_df))
                print('model_id: ',predictions_df.reset_index().model_id[0])
                # print('prediction insert finished, time.time: ',time.time()-start_insert)
                print("updated row for predictions table")


                ### draw the distribution histogram for this model
                model_type = models_row['model_type'][0]
                model_id = models_row['model_id'][0]

                ## INSERT PREDICTIONS HISTOGRAM CODE
                plt.figure(plot_i_out)
                plt.style.use('ggplot')
                sns.set_style("whitegrid", {'axes.grid' : False})
                predict_score = pd.Series(np.array(label_prob)[:,1], name="Prediction score")
                ax = sns.distplot(predict_score,hist_kws=dict(edgecolor="w", linewidth=2),kde=False)
                ax.set_title('{model_type}; model_id = {model_id},{month_start} '.format(model_type=model_type, model_id=model_id,
                  month_start=list(group['month_start'])[0]))
                ax.set_xlim(0,1)
                plt.savefig('score_distribution.png',dpi = 250)

                ### save file to Amazon S3
                #@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
                save_figure_to_amzS3(filename='score_distribution.png',
                                     destination='nyc_peu_inspections/output/figures_modelruns/predictions_distribution/prediction_score_distribution_{model_type}_{model_id}_{month_start}.png'.format(
                                    model_type=model_type,model_id=model_id, month_start=list(group['month_start'])[0]),creds = creds)
                print('prediction score distribution figure saved to Amazon S3')

                plt.show()
                plot_i_out = plot_i_out + 1





                ## conditional for whether to generate evaluations

                if data in months_nolabels:

                    print('test set has no observed labels; not inserting into evaluations')


                else:

                    print('test set has some observed labels; writing real row to evaluations')
                    print(label_name_tosubset)

                    (evaluation_results_all, evaluation_list_all,plot_i)=top_k_unit_prediction_generation(k=top_k,testing_features_df=group,
                                                                                label_prob=label_prob,
                                                                               label_name= label_name_tosubset,
                                                                                confusion_matrix_atk=confusion_matrix_atk,
                                                                                not_imputed_dataframe_for_metrics=not_imputed_dataframe_for_metrics,
                                                                                imputed_dataframe_for_metrics=imputed_dataframe_for_metrics,
                                                                                get_last_k_row_number=get_last_k_row_number,
                                                                                plot_precision_recall_proportion_plot=plot_precision_recall_proportion_plot,
                                                                                plot_i=plot_i_out,
                                                                                save_figure_to_amzS3=save_figure_to_amzS3,
                                                                                model_id=model_id,
                                                                                model_type=model_type,
                                                                                creds=creds)

                    plot_i_out = plot_i + 1

                    evaluations_df = create_evaluations_rows(evaluation_results_all,
                            evaluation_list_all,
                           models_row,
                            predictions_df,
                           test_uuid,
                           top_k)


                    print('created rows for evaluations table')
                    insert_evaluation_table_exec = civis.io.dataframe_to_civis(evaluations_df,database=creds['database'],
                                                        table='dssg_results.evaluations',
                                                        credential_id = creds['civis_superuser']['civis_id'],
                                                        existing_table_rows='append')
                    insert_evaluation_table_exec.result()


                  


    return(None)
