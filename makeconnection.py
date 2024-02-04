import sys
from snowflake.snowpark import Session
sys.path.append('/Users/pradeep/Downloads/Udemy_course_videos/course_2_assignments/Snowpark_pipeline/')
from generic_code import code_library

# Make connection and create Snowpark session
connection_parameters = {"account":"kp41433.ap-southeast-1", \
"user":"pavan", \
"password": "Abc123123", \
"role":"ACCOUNTADMIN", \
"warehouse":"COMPUTE_WH", \
"database":"DEMO_DB", \
"schema":"PUBLIC" \
}

session = code_library.snowconnection(connection_parameters)


session = snowconnection(connection_parameters)