## imports
import pip
import sodapy
import tables 
import sodapy
from sodapy import Socrata
import retrying 
from retrying import retry
from multiprocessing import Pool

## function to get frequency with which
## NYC open data is updated
## returns a string with update frequency
def get_updatefreq(data, dataname, client_metadata):

        onedata_json = data[data.dataname == dataname]['json_id'].values[0] # pulls json
        # id with dataname and turns into string (format for get command)
        one_json_metadata = client_metadata.get_metadata(onedata_json) #pulls all metadata

        ## pulls update freq
        updatefreq = one_json_metadata['metadata']['custom_fields']['Update']['Update Frequency']

        ## returns update freq
        return(updatefreq)



## function that
## takes in:
## 1. @data: the name of the data
## containing ALL api links
## 2. @dataname: the name
## of the specific dataset to
## pull; should be string format
## returns:
## 1. the json id
## in the format needed to
## feed it to the .get request
def clean_json(data, dataname):
        jsonid_forget = data[data.dataname == dataname]['json_id'].tolist()[0]
        return(jsonid_forget)


## function that
## takes in:
## 1. @data: the name of the data
## containing ALL api links
## 2. @dataname: the name
## of the specific dataset to
## pull; should be string format
## returns:
## limit of rows to pull based on column with
## max # of unique rows (all dfs have some id column
## so should be reasonably okay approx); adds
## 10,000 as a buffer (arbitrary)
def generate_limit_parameter(data, dataname, client_metadata):
        onedata_json = data[data.dataname == dataname]['json_id'].values[0] # pulls json
        # id with dataname and turns into string (format for get command)
        one_json_metadata = client_metadata.get_metadata(onedata_json) #pulls all metadata
        one_json_info_allcols = one_json_metadata['columns'] # pull column info from metadata

        # iterate through column info for all cols
        # and store max # of unique rows (cardinality)
        max_row_num = 0
        for i in range(len(one_json_info_allcols)):
            one_col = one_json_info_allcols[i]
            cardinality_onecol = int(one_col['cachedContents']['cardinality'])
            if cardinality_onecol > max_row_num:
                max_row_num = cardinality_onecol

        # return max # of unique rows for that json (should be id columns)
        # + 10000 as a buffer (it's ok if it tries to pull rows that don't
        # exist but want to avoid to extent possible)
        limit_buffer = max_row_num + 10000
        return(limit_buffer)



def df_column_uniquify(df):
    df_columns = map(str.lower, df.columns)
    new_columns = []
    for item in df_columns:
        counter = 0
        newitem = item
        while newitem in new_columns:
            counter += 1
            newitem = "{}_{}".format(item, counter)
            print(newitem)
        new_columns.append(newitem)
    df.columns = new_columns
    return df

def standardize_county_string(county_var):
    if len(county_var) == 2:
        return(str("0" + str(county_var)))
    elif len(county_var) == 1:
        return(str("00" + str(county_var)))
    else:
        return(county_var)
    
def standardize_tract_string(tract_var):
    if len(tract_var) == 5:
        return(str("0" + str(tract_var)))
    elif len(tract_var) == 4:
        return(str("00" + str(tract_var)))
    elif len(tract_var) == 3:
        return(str("000" + str(tract_var)))
    elif len(tract_var) == 2:
        return(str("0000" + str(tract_var)))
    elif len(tract_var) == 1:
        return(str("00000" + str(tract_var)))
    else:
        return(tract_var)

def entities_cols_rename(row):
    if row.rename_col == 1:
        return(row.column + "_static")
    else:
        return(row.column)
    