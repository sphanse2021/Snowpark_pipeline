import sys
sys.path.append('/Users/pradeep/Downloads/Udemy_course_videos/course_2_assignments/Snowpark_pipeline/')
from generic_code import code_library
from schema import src_stg_schema
from snowflake.snowpark.context import get_active_session
import json

### Read from config file.
config_snow_copy = open('./config/copy_to_snowstg_avro.json', "r")
config_snow_copy = json.loads(config_snow_copy.read())

connection_parameter = open('./config/connection_details.json', "r")
connection_parameter = json.loads(connection_parameter.read())

session = code_library.snowconnection(connection_parameter)

df = session.read.avro("@my_s3_stage/Avro_folder/userdata1.avro")


def copy_to_table_semi_struct_data(session,config_file,schema='NA'):
    database_name = config_file.get("Database_name")
    Schema_name = config_file.get("Schema_name")
    Target_table = config_file.get("Target_table")
    target_columns = config_file.get("target_columns")
    on_error = config_file.get("on_error")
    Source_location = config_file.get("Source_location")
    transformations = config_file.get("transformations")

    if config_file.get("Source_file_type") == 'csv':
            return "Expecting semi structured data but got csv"
    elif config_file.get("Source_file_type") == 'avro':
        df = session.read.avro(Source_location)

    # Create temporary stage
    _ = session.sql("create or replace temp stage demo_db.public.mystage").collect()
    remote_file_path = '@demo_db.public.mystage/'+Target_table+'/'
    # Write df to temporary internal stage location
    df.write.copy_into_location(remote_file_path, file_format_type="csv", format_type_options={"FIELD_OPTIONALLY_ENCLOSED_BY":'"'}, header=False, overwrite=True)
    
    # Read the file from temp stage location
    df = session.read.schema(schema).csv("'"+remote_file_path+"'")
    with session.query_history() as query_history:
        copied_into_result = df.copy_into_table(database_name+"."+Schema_name+"."+Target_table, target_columns=target_columns,force=True,on_error=on_error,format_type_options={"FIELD_OPTIONALLY_ENCLOSED_BY":'"'})
    query = query_history.queries
    # Mention command to collect query id of copy command executed.
    for id in query:
        if "COPY" in id.sql_text:
            qid = id.query_id
    return copied_into_result, qid


copied_into_result, qid = copy_to_table_semi_struct_data(session,config_snow_copy,src_stg_schema.int_emp_details_avro)


print(copied_into_result)
print(qid)

copied_into_result_df = session.create_dataframe(copied_into_result)
copied_into_result_df.show()


f = open('/Users/pradeep/Downloads/Udemy_course_videos/course_2_assignments/Snowpark_etl/config/copy_to_snowflake.json', "r")
config = json.loads(f.read())




rejects.count()

get_active_session()

print(copied_into_result)

print(qid)

import pandas as pd
from snowflake.connector.options import installed_pandas, pandas

from snowflake.connector.options import installed_pandas

from snowflake.connector.options import installed_pandas

df = session.create_dataframe(pd.DataFrame([(1, 2, 3, 4)], columns=["a", "b", "c", "d"])).collect()

print(df)

df.show()

df = session.table("DEMO_DB.PUBLIC.SNOWPARK_TEMP_TABLE_GL8Z56B6A4")
#pip install "snowflake-connector-python[pandas]"

if  installed_pandas:
    print("hi")