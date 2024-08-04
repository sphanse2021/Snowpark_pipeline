-- This file belongs to ,
-- https://www.udemy.com/course/snowpark-data-engineering-with-snowflake/learn/lecture/36040078#overview

use database demo_db;
use schema public;

create table emp_avro
(
    emp variant
);

create table INT_EMP_DETAILS_AVRO
(
    registration_dttm varchar,
    id integer,
    first_name varchar,
    last_name varchar,
    email varchar,
    gender varchar,
    ip_address varchar,
    country varchar,
    birthdate date,
    salary varchar,
    title varchar,
    comments varchar,

);

copy into emp_avro
from '@my_s3_stage/Avro_folder/userdata1.avro'
file_format = (type = 'AVRO')
ON_ERROR = CONTINUE;

select * from emp_avro;

select $1:registration_dttm, $1:id, $1: first_name, $1:last_name, $1: email, $1: gender, $1:ip_address,
$1: country, $1: birthdate, $1: salary, $1:title, $1:comments from emp_avro;

create or replace temp stage demo_db.public.mystage;

list @demo_db.public.mystage;
rm @demo_db.public.my stage;

create or replace file format my_avro 
type='AVRO' ;

copy into @demo_db.public.mystage from
(select $1:registration_dttm, $1: id, $1: first_name, $1: Last_name, $1: email, $1: gender, $1: ip_address,
$1: cc, $1: country, $1: birthdate, $1: salary, $1: title, $1: comments from '@my_s3_stage/Avro_folder/userdata1.avro' (file_format => 'my_avro'))
file_format =(type = 'csv');

select from DEMO_DB.PUBLIC.INT_EMP_DETAILS_AVRO;

copy into DEMO_DB. PUBLIC.INT_EMP_DETAILS_AVRO from '@demo_db.public.mystage'
file_format = (type = 'CSV')
ON_ERROR =CONTINUE;