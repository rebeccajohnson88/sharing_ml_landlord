# sharing_ml_landlord
Example scripts from pipeline for: https://dl.acm.org/citation.cfm?id=3332484

*ETL*
The first step in our pipeline involves extracting data from various external sources, such as the American Community Survey and the NYC Open Data Portal, loading it in the database in the raw schema, and performing some preliminary cleaning to prepare the data for merging.

*Preprocessing*
The next step involves preprocessing the data and generating staging tables to be fed into model runs. This is the stage at which we generate our splits, features, and labels.

*Run Models*
We then fit a number of models using different sets of features, split dates, and other parameters. The results of these models are stored in the results schema in the database.

*Evaluating Results*
Finally, we evaluate the models using various metrics, including precision and recall at k, which are stored in the evaluations table in the results schema.

