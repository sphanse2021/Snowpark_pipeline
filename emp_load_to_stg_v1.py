# This file is part of Lecture,
# Creating configuration file,
#https://www.udemy.com/course/snowpark-data-engineering-with-snowflake/learn/lecture/36039772#overview

#Collect rejects,
#https://www.udemy.com/course/snowpark-data-engineering-with-snowflake/learn/lecture/36039468#overview

import sys
# Ignore the below command, if you are executing this code from Github codespaces
#sys.path.append('/Users/pradeep/Downloads/Udemy_course_videos/course_2_assignments/Snowpark_pipeline/')

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

print(copied_into_result)
print(qid)

copied_into_result_df = session.create_dataframe(copied_into_result)
copied_into_result_df.show()

#Comment line