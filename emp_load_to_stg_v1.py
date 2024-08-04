import sys
# Ignore the below command, if you are executing this code from Github codespaces
sys.path.append('/Users/pradeep/Downloads/Udemy_course_videos/course_2_assignments/Snowpark_pipeline/')

# Import generic code library.
from generic_code import code_library
from schema import src_stg_schema
from snowflake.snowpark.context import get_active_session
import json

### Read from config file.
config_snow_copy = open('./config/copy_to_snowstg.json', "r")
config_snow_copy = json.loads(config_snow_copy.read())

connection_parameter = open('./config/connection_details.json', "r")
connection_parameter = json.loads(connection_parameter.read())


session = code_library.snowconnection(connection_parameter)
copied_into_result, qid = code_library.copy_to_table(session,config_snow_copy,src_stg_schema.emp_stg_schema)
rejects = code_library.collect_rejects(session, qid, config_snow_copy)

employee_rejects






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