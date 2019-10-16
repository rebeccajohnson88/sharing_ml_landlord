import pip
pip.main(['install', 'sodapy'])
pip.main(['install', 'tables'])

import civis 
import requests 
from sodapy import Socrata
import logging
import civis 
import datetime
import yaml
import pandas as pd
import time
import hashlib

def store_civisS3_logID(name_storefile, 
                  schema_storefile,
                  object_tostore,
                datatype_formeta_table,
                type_query,
                where_store, # where store parameter to store in fileids or meta
                storeas_type,
                nycopen_api, creds, 
                    client_metadata = None,
                    get_updatefreq = None, 
                    opendata_data = None,
                    logging = False,
                    log_file_name = None,
                    return_fileID = False,
                    return_uuid = False,
                    return_uuid_exists = False,
                    training_or_test_matrix = False,
                    time_id = None,
                    optional_comment = None,
                    optional_borough_predict_list = None):

    # generate name of schema to write to
    if where_store == 'file_ids':
        schematable_insert =  creds['schema_results'] + "." + where_store
    elif where_store == 'meta':
        schematable_insert = creds['schema_clean'] + "." + where_store

    # get date and time of storage
    now = datetime.datetime.now()
    time_for_table = now.strftime("%Y-%m-%d %H:%M:%S")

    # if from NYC open, get update frequency,
    # else, store as null
    if nycopen_api == True:
        update_freq = get_updatefreq(data = opendata_data, dataname = name_storefile,
                                    client_metadata = client_metadata)
    else:
        update_freq = 'NotApplicable' #also tried np.nan and null but just wrote as strings since 
        # needs to be wrapped in quotes in the insert table call for non-null update frequency

    ## options for storing file
    object_type = str(type(object_tostore))

    
    ## in below, if-else, add a step to hash the object_tostore 
    ## the uuid is the hashed thing? 
    ## md5 
    
    if storeas_type=='same':
        
        if logging == True:
            
            filename = log_file_name
            uuid = 'NotApplicable'
            
            
        
        elif object_type == "<class 'dict'>":
            
            filename = name_storefile + ".yaml"
            uuid = 'NotApplicable'
            with open(filename, 'w') as outfile:
                yaml.dump(object_tostore, outfile)
                
            print('wrote yaml file of dictionary')
            
            
    else:
        
        if(object_type == "<class 'pandas.core.frame.DataFrame'>" and storeas_type == 'hdf' and
        training_or_test_matrix == False):

                filename = name_storefile + ".h5"
                object_tostore.to_hdf(filename, key = name_storefile)
                uuid = 'NotApplicable'
                
        elif(object_type == "<class 'pandas.core.frame.DataFrame'>" and storeas_type == 'hdf' and
        training_or_test_matrix == True):

            
                print('generate matrix rows')
                
                
                filename = name_storefile + ".h5"
                object_tostore.to_hdf(filename, key = name_storefile) 
                start_date = pd.to_datetime(object_tostore[time_id]).min() #check if works
                end_date = pd.to_datetime(object_tostore[time_id]).max() #check if works
                n_rows = object_tostore.shape[0]
                if optional_comment == None:
                    optional_comment_tostore = 'NotApplicable'
                else:
                    optional_comment_tostore = optional_comment
                    
                ## generate uuid from string of metadata
                feature_list_string = ", ".join(object_tostore.columns)
                if optional_borough_predict_list == None:
                    data_id = str(feature_list_string + "_" + str(start_date) + "_" + str(end_date) + "_" + str(n_rows))
                else:
                    boroughs_predicted_for_str = '_'.join(optional_borough_predict_list)
                    data_id = str(feature_list_string + "_" + str(start_date) + "_" + str(end_date) + "_" + str(n_rows)) + "_" + str(boroughs_predicted_for_str)
                uuid = hashlib.md5(str.encode(data_id)).hexdigest()
                print(uuid)
                print(type(uuid))

                
                      
        elif(object_type == "<class 'pandas.core.frame.DataFrame'>" and storeas_type == 'csv' and
            training_or_test_matrix == False):

                filename = name_storefile + ".csv"
                object_tostore.to_csv(filename, index = False)
                uuid = 'NotApplicable'
                
        

    ## across all methods, store file ID
    ## to write into meta table
    file_id = civis.io.file_to_civis(filename, filename,  expires_at=None)

    # write query
    if((type_query == 'insert_new') & (where_store == 'meta')):

        insert_table_query = """
                    INSERT INTO {schematable_insert} values 
                    ('{schema_storefile}', '{dataname}', '{time_for_table}','{file_id}', '{datatype}', '{updatefreq}')
                    """.format(schematable_insert = schematable_insert,
                              schema_storefile = schema_storefile,
                              dataname = filename,
                              time_for_table = time_for_table,
                              file_id = file_id,
                              datatype = datatype_formeta_table,
                              updatefreq = update_freq)
        
        print(insert_table_query)

        insert_table_execute = civis.io.query_civis(insert_table_query, creds['database'],
                                                   credential_id = creds['civis_superuser']['civis_id'])
        insert_table_execute.result()        

    elif((type_query == 'update_existing') & (where_store == 'meta')):

        update_table_query = """
                    UPDATE {schematable_insert} 
                    set civis_file_id = {file_id} 
                    where tablename = '{filename}'
                    """.format(schematable_insert = schematable_insert,
                              file_id = file_id,
                              filename = filename)

        update_table_execute = civis.io.query_civis(update_table_query, creds['database'],
                                                   credential_id = creds['civis_superuser']['civis_id'])
        update_table_execute.result()  
        
    elif((type_query == 'insert_new') & (where_store == 'file_ids')):
        
        
        if(training_or_test_matrix == True):
            

            
            ## check if uuid exists in file ids and don't insert if so
            file_table = creds['schema_results'] + ".file_ids"
            select_uuid = """select uuid
            from {table_name}
            """.format(table_name = file_table)

            all_uuid = civis.io.read_civis_sql(select_uuid, database = creds['database'],
                                    use_pandas=True, 
                                    credential_id = creds['civis_superuser']['civis_id'])
            
            
            if all_uuid['uuid'].isnull().all():
                
                print('training/test matrix does not already exist (no uuids exist); inserting')
                
                uuid_existence = 'uuid_doesnt_exist'
                
                
                insert_table_query = """
                    INSERT INTO {schematable_insert} values 
                    ('{schema_storefile}', 
                    '{dataname}', 
                    '{file_id}', 
                     '{description}', 
                    '{uuid}', 
                    '{last_updated}', 
                    '{start_date}', 
                    '{end_date}',
                    '{n_rows}',
                    '{optional_comment}')
                    """.format(schematable_insert = schematable_insert,
                              schema_storefile = schema_storefile,
                              dataname = filename,
                              file_id = file_id, 
                            description = filename,
                              uuid = uuid,
                              last_updated = time_for_table,
                              start_date = start_date,
                              end_date = end_date, 
                              n_rows = n_rows,
                              optional_comment = optional_comment)
            

                insert_table_execute = civis.io.query_civis(insert_table_query, creds['database'],
                                                           credential_id = creds['civis_superuser']['civis_id'])
                insert_table_execute.result() 
            
            else:
                
                if all_uuid['uuid'].str.contains(str(uuid)).any():

                    print('training/test matrix already exists; not inserting')
                
                    uuid_existence = 'uuid_already_exists'
                
                else:
                
                    print('training/test matrix does not already exist; inserting')
                
                    uuid_existence = 'uuid_doesnt_exist'
                
                
                    insert_table_query = """
                    INSERT INTO {schematable_insert} values 
                    ('{schema_storefile}', 
                    '{dataname}', 
                    '{file_id}', 
                     '{description}', 
                    '{uuid}', 
                    '{last_updated}', 
                    '{start_date}', 
                    '{end_date}',
                    '{n_rows}',
                    '{optional_comment}')
                    """.format(schematable_insert = schematable_insert,
                              schema_storefile = schema_storefile,
                              dataname = filename,
                              file_id = file_id, 
                            description = filename,
                              uuid = uuid,
                              last_updated = time_for_table,
                              start_date = start_date,
                              end_date = end_date, 
                              n_rows = n_rows,
                              optional_comment = optional_comment)
            

                    insert_table_execute = civis.io.query_civis(insert_table_query, creds['database'],
                                                           credential_id = creds['civis_superuser']['civis_id'])
                    insert_table_execute.result() 
        
        else:
            
            print('inserting new row not about training/test matrix into file ids')
            
            insert_table_query = """
                    INSERT INTO {schematable_insert} 
                    (schema_name, file_name, civis_file_id, description)
                    values 
                    ('{schema_storefile}', '{dataname}', '{file_id}', '{description}')
                    """.format(schematable_insert = schematable_insert,
                               schema_storefile = schema_storefile,
                              dataname = filename,
                              file_id = file_id,
                              description = filename)

            insert_table_execute = civis.io.query_civis(insert_table_query, creds['database'],
                                                       credential_id = creds['civis_superuser']['civis_id'])
            insert_table_execute.result()         

    elif((type_query == 'update_existing') & (where_store == 'file_ids')):

        update_table_query = """
                    UPDATE {schematable_insert} 
                    set civis_file_id = {file_id} 
                    where file_name = '{filename}'
                    """.format(schematable_insert = schematable_insert,
                              file_id = file_id,
                              filename = filename)

        update_table_execute = civis.io.query_civis(update_table_query, creds['database'],
                                                   credential_id = creds['civis_superuser']['civis_id'])
        update_table_execute.result()  


    # return or don't return file ID
    if return_fileID == True and return_uuid == True:
        
        return(file_id, uuid)
    
    elif return_fileID == True and return_uuid == False:
        
        return(file_id)
    
    elif (return_fileID == False) and (return_uuid == True) and (return_uuid_exists == False):
        
        return(uuid)
    
    elif (return_fileID == False) and (return_uuid == True) and (return_uuid_exists == True):
        
        return(uuid,  uuid_existence)

    else:
        return None
    
