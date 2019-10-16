## imports
import boto3
import yaml
from time import gmtime, strftime
from savebackups_S3 import *

## define a function to load and update configs
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
                    current_config['param_features_to_pull'] + "_startTrain:" + current_config['param_train_startdate'].strftime('%Y-%m-%d') +
                    "_endTest:" + current_config['param_test_enddate'].strftime('%Y-%m-%d'))


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

def update_master_config_new(creds, parameter_names, parameters, 
    update_config = True, return_all_config = False,
    save_backup_to_amzS3 = save_backup_to_amzS3,
    simpler_loading = False):
    
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
    if simpler_loading == False:
        current_config = {}
        for i in range(0, len(parameter_names)):
            name = parameter_names[i]
            current_config[name] = parameters[i]

        print('current_config: ', current_config)

        ## create name of key for config file
        current_time = strftime("%Y_%m_%d_%H:%M:%S:%MS", gmtime())
        config_keyname = str('modelrun_' + current_time + "_label:" + current_config['param_primary_label'] + "_features:" +
                        current_config['param_features_to_pull'] + "_startTrain:" + current_config['param_train_start_date'].strftime('%Y-%m-%d') +
                        "_endTest:" + current_config['param_test_end_date'].strftime('%Y-%m-%d'))


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
        
    elif simpler_loading == True:
        
        current_time = strftime("%Y_%m_%d_%H:%M:%S:%MS", gmtime())
        config_keyname = str('modelrun_' + current_time + "_label:" + parameters['param_primary_label'] + "_features:" +
                        parameters['param_features_to_pull'] + "_startTrain:" + parameters['param_split_start_date'] +
                        "_endTest:" + parameters['param_split_end_date'])
        
        if config_keyname in all_configs_dict:

            print('key conflict; re-run')
            return(None)

        else:

            ## if key doesn't exist, append to dictionary
            all_configs_dict[config_keyname] = parameters

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


## example of how applied
## parameter_names = [obj for obj in dir() if obj.startswith('param_')]
## update_master_config(creds = creds, parameter_names = parameter_names, update_config = True,
##                                 return_all_config= False)
