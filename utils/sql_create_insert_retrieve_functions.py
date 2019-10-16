import civis
import yaml
import pandas as pd


## function to get the model group row
### when the model group row is new and just inserted (to do: consider to combine with search)
def get_last_row_model_group(creds):
    query_model_group = """
    select *
    from dssg_results.model_group
    order by model_group_id desc
    limit 1
    """
    query_table_exec = civis.io.read_civis_sql(
        query_model_group, database=creds['database'],use_pandas=True,
        credential_id = creds['civis_superuser']['civis_id'])
    ### return the last row in pandas format
    return query_table_exec

def get_row_model_group_fromid(modelgroup_id,creds):
    query_model_group = """
    select *
    from dssg_results.model_group
    where model_group_id = '{id_to_retrieve}'
    """.format(id_to_retrieve = modelgroup_id)

    query_table_exec = civis.io.read_civis_sql(
        query_model_group, database=creds['database'],use_pandas=True,
        credential_id = creds['civis_superuser']['civis_id'])

    ### return the last row in pandas format
    return query_table_exec

## function to get the models row
### when the model group row is new and just inserted (to do: consider to combine with search)
def get_last_row_models(creds):
    query_models = """
    select *
    from dssg_results.models
    order by model_id desc
    limit 1
    """
    query_table_exec = civis.io.read_civis_sql(
        query_models, database=creds['database'],use_pandas=True,
        credential_id = creds['civis_superuser']['civis_id'])
    ### return the last row in pandas format
    return query_table_exec

### functions to get the predictions table row for drawing a map
def get_predictions_for_map(model_id,creds):
    sql = """
    select p.model_id,p.entity_id,p.score,p.rank_abs,p.rank_pct,
    s.internal_units_static, s.internal_longitude_static,s.internal_latitude_static
    from dssg_results.predictions p
    left join (
    select address_id,internal_units_static,internal_longitude_static, internal_latitude_static
    from dssg_staging.entities_address_level_rentstab) s
    on p.entity_id=s.address_id
    where p.model_id = {model_id}
    """.format(model_id=model_id)
    ## read data into dataframe
    df_model = civis.io.read_civis_sql(sql,database=creds['database'],
                                       credential_id = creds['civis_superuser']['civis_id'],use_pandas=True)
    return df_model

### functions to get the predictions table for comparison, such as jaccard index
def get_predictions_of_two_models(model_id_0, model_id_1,creds):
    sql = """
    select p.model_id,p.entity_id,p.score,p.rank_abs,p.rank_pct,s.internal_units_static
    from dssg_results.predictions p
    left join (
    select distinct (address_id),internal_units_static
    from dssg_staging.staging_train_monthly) s
    on p.entity_id=s.address_id
    where p.model_id = {model_id_0} or p.model_id = {model_id_1}
    """.format(model_id_0=model_id_0,model_id_1=model_id_1)
    ## read data into dataframe
    df_two_model = civis.io.read_civis_sql(sql,database=creds['database'],credential_id = creds['civis_superuser']['civis_id'],use_pandas=True)
    df_model0= df_two_model.loc[df_two_model.model_id == model_id_0]
    df_model1= df_two_model.loc[df_two_model.model_id == model_id_1]
    return (df_model0,df_model1)


# get columns of a table for insert query,  if the table has an idnetity/serial column, remove the identity/serial column


def get_table_col(creds,table,get_cols_intable_redshift): #table should be either 'models' or 'model_group'
    ## get all columns in the target table
    table_info = get_cols_intable_redshift(schema_name = creds['schema_results'],
                                 table_name = table,
                                 creds = creds)
    ## select columns to insert and join into string format
    if table == 'models' or table =='model_group':
        columns_to_insert=','.join(table_info['column'][1:])
    else:
        print(str('entered else condition for' + str(table)))
        columns_to_insert=','.join(table_info['column'])
        print(str('created columns to insert for' + str(table)))

    return columns_to_insert


## define insert query function to insert new results in results_schema
def sql_insert_function(schematable_insert,columns_to_insert,values_to_insert,creds):
    ### insert query
    if schematable_insert == 'dssg_results.model_group' or schematable_insert == 'dssg_results.models':
        insert_table_query = """
                        INSERT INTO {schematable_insert} ({columns_to_insert}) values
                        ({values_to_insert})
                        """.format(schematable_insert = schematable_insert,
                                   columns_to_insert = columns_to_insert,
                                   values_to_insert = values_to_insert
                                  )
    else:
        insert_table_query = """
                INSERT INTO {schematable_insert} values
                ({values_to_insert})
                """.format(schematable_insert = schematable_insert,
                           values_to_insert = values_to_insert
                          )
    ### execute query
    insert_table_exec = civis.io.query_civis(
        insert_table_query, database=creds['database'],
        credential_id = creds['civis_superuser']['civis_id'])
    insert_table_exec.result()

def return_matching_models_row(modelgroup_id, train_uuid, creds):


    ## pull models and test id rows associated with that model id
    models_rows_query = """
    select *
    from dssg_results.models
    where model_group_id = '{modelgroup_id}'
    order by model_id desc
    limit 5
    """.format(modelgroup_id = modelgroup_id)

    ## read in models rows in df format
    models_forthatID = civis.io.read_civis_sql(
        models_rows_query, database=creds['database'],
        credential_id = creds['civis_superuser']['civis_id'],use_pandas = True)

    ## check train data AND test data AND label imputation method (need to be all combined in same model)

    train_uuid_tocheck = train_uuid

    for i in range(0, models_forthatID.shape[0]):

        one_models_row = models_forthatID.iloc[i]

        train_uuid_tomatch = str(one_models_row['training_matrix_uuid'])

        if train_uuid_tomatch == train_uuid_tocheck:

            print('found model id')
            return(one_models_row.to_frame().T)

            break

        else:

            print('error; didnt find matching model with that model group id and uuid; abort script')


def checkif_modelgroup_exists_recent(model_group_values_to_check, creds):

    ## select all rows/cols from the model group table
    ## returns a ist of lists (by not instructing it to read as pandas df)
    query_model_group_all = """
    select *
    from {table_name}
    order by model_group_id desc
    limit 5
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
        (len(set(one_row[3].split(',')).intersection(model_group_values_to_check[3])) > 0.8*len(model_group_values_to_check[3])) and
        (set(one_row[4].split('_')) == model_group_values_to_check[4])):

            print('found match')

            mg_id_toreturn = mg_allcols[i][0]

            return(mg_id_toreturn)

            break



    return('error; mg doesnt exist')