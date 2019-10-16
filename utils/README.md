# Utils 

This directory contains various functions used in our pipeline.

## Setup Functions
- `load_creds_file`: function to load a .yaml file containing database and other credentials. The files are stored in Civis' internal S3. The general structure of the file should be as follows in order to pull the relevant credentials in subsequent scripts: 
  ``` 
  nyc_api_getdata:
    apikey:
	username: 
	password 
    
  googlemap_api:
    apikey: 
	
  redshift:
    username: 
	password: 
	
  civis: 
    username: 
    password: 
    id: 
    
  aws_s3:
    access_key_id: 
    secret_access_key: 

  database: 
  schema_internal: name of schema containing internal data
  schema_raw: name of schema to dump raw external data
  schema_clean: name of schema to store cleaned tables
  schema_staging: name of schema to store staging tables
  schema_results: name of schema to store results
  ```
- `store_civisS3_logID`: function that stores a file in Civis' S3, and inserts the associated file id either in the meta table of the `dssg_clean` schema (if the file is an hdf backup of a SQL table in that schema) or in the `file_ids` table in the results schema otherwise. Users input a file in its native format (e.g., Pandas DataFrame, dictionary). The function then either contains options to store in different formats (e.g., hdf versus .csv for Pandas DataFrame) or defaults to a format (e.g., defaults to .yaml files) for dictionaries.

## ETL Functions

- `etl_functions`: contains the following functions related to different steps of the ETL process. 
	- **General purpose functions**: functions to 'uniquify' column names to allow writing to SQL tables.
	- **ACS cleaning functions**: functions to standardize the county and tract strings with the correct number of digits.
	- **NYC API functions**: functions for pulling from the NYC open data API related to establishing clients, reading metadata, reading data in chunks, etc.

## Pipeline Preprocessing Functions
- `get_cols_intable_redshift`: function to create a dataframe containing all the columns in a given Redshift table. This function is used in scripts that, for instance, read the columns of data in the staging table and generate feature sets from those column names. It can also be used to check the SQL data types of a column, with `get_feature_var_types` first using these SQL data types and then performing further classification.
- `get_feature_var_types`: function to define variable types of columns based on their names, SQL data types, and other attributes 
- `split_train_test`: function that generates training and test staging tables containing features and labels based on a list of split dates
- `choosingfeatures_functions`: functions that select sets of training and test tables and specific sets of features to load into dataframes. The main function relevant for estimating models is `select_features_and_labels`. It contains a few different options that allow users to fit a model with different sets of features (columns) or different sets of addresses (rows):
	- **Which date ranges to estimate the model on**: `[train/test]_[start/end]date`: these parameters control which training and test sets are read in. In general, training sets start in 2016 and end at varying points depending on the temporal split; test sets start the month following the last training set month. 
	- **Which addresses to estimate the model on**: two parameters control the sample the model is estimated on and/or predicted for. For the training set, since the rows are at the address-month level, the parameters mean that the same address may have one month of data in the sample but not a different month (e.g., an address may have February 2017 pulled because there was a knock so a non-missing label but not March 2017 pulled). At the staging table stage, addresses are restricted to those with at least one rent-stabilized unit.
		- `only_observed_labels_train`: this restricts the training set to observations with non-missing labels for a given month. The default for the parameter is set to True.
		- `test_targetzip`: the default for the test set is to read in all addresses regardless of whether they have a missing or observed label, since we can still generate predictions for the former. This parameter, which is set to a default of True, restricts the test set to addresses in TSU's target zip codes for a particular month (which varies across months due to expansions, etc.). The `add_testrows` parameter allows the user to add in addresses outside the target zips (drawn from a stored randomly generated order) if one wants to score predictiosn from the model on fewer than the full set of 45,000 rent-stabilized addresses but more than the target zip addresses (which range from 4000 to 6400 addresses depending on the month).
	- **Which features to read in**: the script quickly runs out of memory if we read in all staging table features, even from a limited set of rows. So the `feature_list` parameter accepts a list and subsets the read-in columns to these features to help with memory.

- `preprocessing_functions`: functions that take in above dataframes and perform imputation, normalization, and converting categorical variables into dummy indicators.

## Modeling Functions
- `update_master_config`: function that appends all the parameters for a given model to one (long!) .yaml file stored in Civis S3
- `runmodels_returnresults`: main function that runs a given list of models, stores the results in the results schema, and generates a variety of graphical outputs
	- The steps are as follows:
		- Subset the training and test set to the relevant rows and columns (the rows are restricted to the boroughs we've specified to fit the model on; columns to the features, label, and time + unit ids)
		- Generate a unique ID for each training and test sets stored in the `dssg_results.file_ids` table and other `dssg_results` schema tables. These uuids allow us to pull the exact training and test matrix used in a particular model in case we want to re-generate the results or extract new attributes from the model not stored in the initial run (e.g., for decision tree models, the object that allows us to graph the tree structure)
		- Iterate over each model in the list and store information about that model in the `model_group` table.
		- Estimate the model and store information about that estimation (e.g., hyperparamaters used; feature list) in the results schema
		- With the fit model object, iterate over test set months and generate and store predictions
		- With the predictions, evaluate model performance and store the performance metrics at different 'k' (k = observed TSU capacity in test set months)
		
	- The above steps call on the following families of functions:
		- `sql_create_insert_retrieve_functions`: functions related to retrieving from the results schema (for instance, for grabbing the serially-created model ID) and inserting into the schema
		- `results_graphing_functions`: functions for plotting feature importances, prediction distributions, and precision-recall curves stored in S3 and saved under the model ID and test set month
		- `top_k_unit_prediction_generation`: functions related to evaluating model performance at top k

- `pipeline_sourcefunctions`: this is a script that contains links to the functions above that are relevant to preprocessing and modeling stage of the pipeline, and is generally sourced at the beginning of a script.

## Results Interpretation Functions

- `results_graphing_functions`: this script also contains functions useful for plotting results outside the main model run

- `results_querying_functions`: this script contains functions for querying the results schema for different results of interest (e.g., predictions; feature importances); most focus on the user feeding a model ID or range of model IDs to pull.

- `results_findbest_functions`: contains functions for aggregating performance metrics (precision ratio; recall ratio) across split dates and generating a LaTeX table summarizing 1) the results of those evaluations, 2) all model IDs for that 'model class' (algorithm + hyperparameters), and 3) model IDs for months with max and min performance
