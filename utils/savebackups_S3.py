import yaml
import pandas as pd
import boto3
from time import gmtime, strftime

def save_backup_to_amzS3(object_tostore,
                    name_storefile,
                    creds, storeas_type = 'hdf', # only change if data.frame and .csv
                    return_uuid = False,
                    return_uuid_exists = False,
                    training_or_test_matrix = False):
    
    ## get type of object
    object_type = str(type(object_tostore))
    
    ## conditions for storing dataframe as hdf or csv
    if(object_type == "<class 'pandas.core.frame.DataFrame'>" and storeas_type == 'hdf' and 
       training_or_test_matrix == False):
        
        ## generate local file
        filename = name_storefile + ".h5"
        object_tostore.to_hdf(filename, key = name_storefile)
        
        ## store in s3
        s3_client = boto3.client('s3',
        aws_access_key_id=creds['aws_s3']['access_key_id'],
        aws_secret_access_key=creds['aws_s3']['secret_access_key'])
        
        # upload local file to s3
        s3_client.upload_file(filename, 'dsapp-economic-development', 
                             'nyc_peu_inspections/data_backups/{filename}'.format(filename = filename))
        print('uploaded')
        
    elif(object_type == "<class 'pandas.core.frame.DataFrame'>" and storeas_type == 'csv' and
            training_or_test_matrix == False):
        filename = name_storefile + ".csv"
        object_tostore.to_csv(filename, index = False)
        
        ## store in s3
        s3_client = boto3.client('s3',
        aws_access_key_id=creds['aws_s3']['access_key_id'],
        aws_secret_access_key=creds['aws_s3']['secret_access_key'])
        
        # upload local file to s3
        s3_client.upload_file(filename, 'dsapp-economic-development', 
                             'nyc_peu_inspections/data_backups/{filename}'.format(filename = filename))
        print('uploaded')
    
    elif object_type == "<class 'dict'>":
        filename = name_storefile + ".yaml"
        with open(filename, 'w') as outfile:
            yaml.dump(object_tostore, outfile)
                
        print('wrote yaml file of dictionary')
        
        ## store in s3
        s3_client = boto3.client('s3',
        aws_access_key_id=creds['aws_s3']['access_key_id'],
        aws_secret_access_key=creds['aws_s3']['secret_access_key'])
        
        # upload local file to s3
        s3_client.upload_file(filename, 'dsapp-economic-development', 
                             'nyc_peu_inspections/data_backups/{filename}'.format(filename = filename))
        print('uploaded')
        
    return None

def update_master_config(creds, parameter_names, parameters, update_config = True, return_all_config = False,
    save_backup_to_amzS3 = save_backup_to_amzS3):
    
    print('func4: ', parameter_names)
    print('parameters_update_master', parameters)
    

    ## read in config file from amazon s3
    ## establish client and download file from s3
    ## first establish an s3 client
    s3_client = boto3.client('s3',
        aws_access_key_id=creds['aws_s3']['access_key_id'],
        aws_secret_access_key=creds['aws_s3']['secret_access_key'])

    ## then, download file
    s3_client.download_file('dsapp-economic-development', 
                    'nyc_peu_inspections/data_backups/all_configs.yaml', 
                    'downloaded_configs.yaml')

    ## load yaml file
    with open('downloaded_configs.yaml','r') as stream:
        all_configs_dict = yaml.load(stream)

    ## create a dictionary that stores this run's configs based on
    ## names of local objects
    current_config = {}
    for i in range(0, len(parameter_names)):
        name = parameter_names[i]
        current_config[name] = parameters[i]

    print('current_config: ', current_config)

    ## create name of key for config file
    current_time = strftime("%Y_%m_%d_%H:%M:%S:%MS", gmtime())
    config_keyname = str('modelrun_' + current_time + "_label:" + current_config['param_primary_label'] + "_features:" +
                    current_config['param_features_to_pull'])


    ## check if key exists and return if so
    if config_keyname in all_configs_dict:

        print('key conflict; re-run')
        return(None)

    else:

        ## if key doesn't exist, append to dictionary
        all_configs_dict[config_keyname] = current_config

        ## return if want
        if return_all_config == True:

            return(all_configs_dict)

        if update_config == True:
            ## then store in S3 
            save_backup_to_amzS3(object_tostore = all_configs_dict,
                    name_storefile = 'all_configs',
                    creds = creds)


        ## unless returning all configs, return none
        return(None)

def download_backups_s3(name_storefile_withextension,
                       creds,
                      filetype_string = 'csv'): #yaml or csv at moment; hd5 seems to be deprecated
    
    ## initialize client
    ## establish client and download file from s3
    ## first establish an s3 client
    s3_client = boto3.client('s3',
        aws_access_key_id=creds['aws_s3']['access_key_id'],
        aws_secret_access_key=creds['aws_s3']['secret_access_key'])
    
    downloaded_name = str('downloaded_' + name_storefile_withextension)
    
    ## then, download file
    s3_client.download_file('dsapp-economic-development', 
                    'nyc_peu_inspections/data_backups/{filename}'.format(filename = name_storefile_withextension), 
                    downloaded_name)
    print("downloaded")
    
    if filetype_string == "yaml":
        
        ## open differently depending on extension
        with open(downloaded_name,'r') as stream: 
            file = yaml.load(stream)
        
        return(file)
        
    elif filetype_string == "csv":
        
        file = pd.read_csv(downloaded_name)
        return(file)
        
