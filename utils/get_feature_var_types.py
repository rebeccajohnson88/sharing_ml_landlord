
import numpy as np 
import pandas as pd 
import re

def generate_var_types(feature_col_df): 
    
    # identify different types of variables

    binary_pattern = re.compile(r'.*_any.*|.*sp_.*|.*team.*|.*borough.*|internal_rent_stabilized_.*|.*heating_season.*|.*peu_target.*|.*bldg_wide.*|.*flag.*')
    factor_pattern = re.compile(r'.*factor.*')
    label_pattern = re.compile(r'.*next_month')
    character_pattern = re.compile(r'.*tract$|.*tract_static$')
    flagged_binary_init = [x for x in feature_col_df.column_name if re.match(binary_pattern,x)]
    flagged_factor = [x for x in feature_col_df.column_name if re.match(factor_pattern,x)]
    flagged_label = [x for x in feature_col_df.column_name if re.match(label_pattern,x)]
    flagged_character = [x for x in feature_col_df.column_name if re.match(character_pattern,x)]
    flagged_binary_final = list(set(flagged_binary_init).difference(set(flagged_factor + flagged_label + flagged_character))) # convert to list
    
    # put var types into a df

    feature_col_df['var_type'] = feature_col_df.apply(lambda row: get_var_types(row['data_type'], row['column_name'], flagged_label, flagged_binary_final,
                                                                               flagged_character), axis=1)
    
    return feature_col_df

def get_var_types(type, column_name, flagged_label, flagged_binary_final, flagged_character):
    
    if type == 'double precision':
        
        return('continuous')
    
    elif type == 'character varying' or type == 'text':
        
        return('character')
    
    elif type == 'date':
        
        return('date')
    
    elif type == 'integer' or type == 'bigint' or type == 'numeric':
        
        if column_name in flagged_label:
            
            return('label')
        
        elif column_name in flagged_binary_final: 
            
            return('binary')
        
        elif column_name in flagged_character:
            
            return('character')
        
        else:
        
            return('continuous')