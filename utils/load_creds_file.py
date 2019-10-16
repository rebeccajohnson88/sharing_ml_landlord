
import yaml

## modify load_creds_file function
def load_creds_file(name_and_path_creds_file):
    
    with open(name_and_path_creds_file,'r') as stream: 
        creds_dict = yaml.load(stream)
        
    print("loaded yaml file to use")    
        
    return(creds_dict) 

print('load creds script run')

## join parameters
parameters_all = ", ".join(['name_creds_file', 'credential_id'])

print(str('parameters are: ' + parameters_all))