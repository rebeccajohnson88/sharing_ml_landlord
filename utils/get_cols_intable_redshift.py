import civis
import pandas as pd

## function to get names of columns
## in a table in schema
def get_cols_intable_redshift(schema_name, table_name, creds):
    cols_query = """set search_path to '$user', 'public', '{schema_name}';
          SELECT *
          FROM pg_table_def
          WHERE tablename='{table_name}'""".format(schema_name= schema_name,
                                                   table_name = table_name)
    col_names_df = civis.io.read_civis_sql(cols_query,
                            database=creds['database'],
                            use_pandas=True,
                           credential_id = creds['civis_superuser']['civis_id'])
    return(col_names_df)


parameters = ", ".join(['schema_name',
                    'table_name',
                    'creds'])

print(str('parameters are: ' + parameters))
