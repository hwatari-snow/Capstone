-------------------------------------------------------------------------------------------
-- Capstone File Stream Setup for Users
--
-- This worksheet enables SEs to setup their own file streaming into 
-- their bucket from their account.  This is a much more flexible arrangement than
-- the previous Capstone solution where the streaming was always on
-- and there was no way to control the streaming.  By putting the 
-- control in the SEs hands, it enables more flexible learning.
--
-- Prequisiites:  
--    * You must have your raw Kapa table populated in your Capstone Database
--    * Ensure that you have the following privs granted to the role that owns your capstone objects.
--         GRANT EXECUTE TASK on ACCOUNT to ROLE <your_role>;
--         GRANT EXECUTE MANAGED TASK on ACCOUNT to ROLE <your_role>;
-- 
-- Overview of the process
-- 1. Create the 2 stored procedures you will need.
-- 2. Setup variables for the rest of the worksheet
-- 3. Get the KAPA offset value to be used for replaying older files.
-- 4. Test that you can write to your bucket.
-- 5. Create and start the Task to stream files to your bucket.
-------------------------------------------------------------------------------------------

-- Set your context
use role sysadmin;  -- replace with your role
use warehouse compute_wh_xl ;  -- replace with your warehouse
use database capstone; -- replace with your database
use schema capstone.public;  -- replace with your schema

---------------------------------------------------------------------------
-- 1. Create the 2 stored procedures you will need for the stream
---------------------------------------------------------------------------
---------------------------------------------------------------------------
-- CHECK_S3_WRITE Stored Procedure
-- This stored proc checks to see if you can write a file to your bucket.
---------------------------------------------------------------------------
create or replace procedure check_s3_write 
(v_kapa_offset integer, v_raw_table varchar, v_stage varchar, v_json_ff varchar, v_json_data varchar)
returns varchar(2000)
language sql
as
$$
declare
  dynamic_query varchar(2000);
  error_message varchar(2000);
--   kapa_offset number;
begin
  dynamic_query :=   
  'COPY INTO @'||v_stage||'/kapa-stream/ '||
  'FROM ('||
  'SELECT distinct OBJECT_CONSTRUCT_KEEP_NULL('||
  '''gs'', '||v_json_data||':"gs"::STRING '||  
  ', ''heading'', '||v_json_data||':"heading"::STRING'||
  ', ''baro_alt'', '||v_json_data||':"baro_alt"::STRING'||
  ', ''squawk'', '||v_json_data||':"squawk"::STRING'||
  ', ''alt'', '||v_json_data||':"alt"::STRING'||
  ', ''lon'', '||v_json_data||':"lon"::STRING'||
  ', ''facility_name'', '||v_json_data||':"facility_name"::STRING'||
  ', ''gps_alt'', '||v_json_data||':"gps_alt"::STRING'||
  ', ''pitr'', '||v_json_data||':"pitr"::STRING'||
  ', ''id'', '||v_json_data||':"id"::STRING'||
  ', ''hexid'', '||v_json_data||':"hexid"::STRING'||
  ', ''facility_hash'', '||v_json_data||':"facility_hash"::STRING'||
  ', ''ident'', '||v_json_data||':"ident"::STRING'||
  ', ''lat'', '||v_json_data||':"lat"::STRING'||
  ', ''type'', '||v_json_data||':"type"::STRING'||
  ', ''updateType'', '||v_json_data||':"updateType"::STRING'||
  ', ''air_ground'', '||v_json_data||':"air_ground"::STRING'||
  ', ''clock'', '||v_json_data||':"clock"::number+'||to_char(v_kapa_offset+28500)||
  ') AS flight_obj '||
  'FROM '||v_raw_table||
  ' where '||v_json_data||':clock::timestamp between timestampadd(''SECONDS'', '||to_char(-v_kapa_offset)||', current_timestamp())'||
  'and timestampadd(''SECONDS'', '||to_char(-v_kapa_offset+300)||', current_timestamp()))'||
  'PARTITION BY ( ''year=''|| date_part(''YEAR'', flight_obj:clock::timestamp)||''/month=''|| lpad(date_part(''MONTH'',flight_obj:clock::timestamp), ''2'', ''0'')||''/day=''|| lpad(date_part(''DAY'',flight_obj:clock::timestamp), ''2'', ''0'')) FILE_FORMAT = '''||v_json_ff||''';';

  begin
    execute immediate :dynamic_query;
    return 'COPY command executed successfully. Check your bucket for the file.';
  exception
    WHEN OTHER THEN
      -- If an exception occurs, capture the error message
      error_message := SQLERRM;
      RETURN 'Error executing query: ' || error_message;
  end
  ;
  -- return dynamic_query;
end;
$$
;

---------------------------------------------------------------------------
-- CREATE_STREAM_TASK Stored Procedure
-- This stored proc creates the task for streaming a file into your 
-- bucket every 5-6 minutes.  
---------------------------------------------------------------------------
create or replace procedure create_stream_task 
(v_db_schema varchar, v_raw_table varchar, v_kapa_offset number, v_stage varchar, v_json_ff varchar, v_json_data varchar)
returns varchar(2000)
language sql
as
$$
declare
  dynamic_query varchar(2000);
  error_message varchar(2000);
begin
  dynamic_query := 
  'create or replace task '||v_db_schema||'.publish_kapa_adsb_data '||
  'SCHEDULE = ''6 MINUTES'' '||
  'USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = ''XSMALL'' '||
  'as '||
  'COPY INTO @'||v_stage||'/kapa-stream/ '||
  'FROM ('||
  'SELECT distinct OBJECT_CONSTRUCT_KEEP_NULL('||
  '''gs'', '||v_json_data||':"gs"::STRING '||  
  ', ''heading'', '||v_json_data||':"heading"::STRING'||
  ', ''baro_alt'', '||v_json_data||':"baro_alt"::STRING'||
  ', ''squawk'', '||v_json_data||':"squawk"::STRING'||
  ', ''alt'', '||v_json_data||':"alt"::STRING'||
  ', ''lon'', '||v_json_data||':"lon"::STRING'||
  ', ''facility_name'', '||v_json_data||':"facility_name"::STRING'||
  ', ''gps_alt'', '||v_json_data||':"gps_alt"::STRING'||
  ', ''pitr'', '||v_json_data||':"pitr"::STRING'||
  ', ''id'', '||v_json_data||':"id"::STRING'||
  ', ''hexid'', '||v_json_data||':"hexid"::STRING'||
  ', ''facility_hash'', '||v_json_data||':"facility_hash"::STRING'||
  ', ''ident'', '||v_json_data||':"ident"::STRING'||
  ', ''lat'', '||v_json_data||':"lat"::STRING'||
  ', ''type'', '||v_json_data||':"type"::STRING'||
  ', ''updateType'', '||v_json_data||':"updateType"::STRING'||
  ', ''air_ground'', '||v_json_data||':"air_ground"::STRING'||
  ', ''clock'', '||v_json_data||':"clock"::number+'||to_char(v_kapa_offset+28500)||
  ') AS flight_obj '||
  'FROM '||v_raw_table||
  ' where '||v_json_data||':clock::timestamp between timestampadd(''SECONDS'', '||to_char(-v_kapa_offset)||', current_timestamp())'||
  'and timestampadd(''SECONDS'', '||to_char(-v_kapa_offset+300)||', current_timestamp()))'||
  'PARTITION BY ( ''year=''|| date_part(''YEAR'', flight_obj:clock::timestamp)||''/month=''|| lpad(date_part(''MONTH'',flight_obj:clock::timestamp), ''2'', ''0'')||''/day=''|| lpad(date_part(''DAY'',flight_obj:clock::timestamp), ''2'', ''0'')) FILE_FORMAT = '''||v_json_ff||''';';

  begin
    execute immediate :dynamic_query;
    return 'CREATE TASK command executed successfully. Remember to Resume the Task and check status.';
  exception
    WHEN OTHER THEN
      -- If an exception occurs, capture the error message
      error_message := SQLERRM;
      RETURN 'Error executing query: ' || error_message;
  end;

  -- return :dynamic_query;

end;
$$
;


  
---------------------------------------------------------------------------
-- 2. Set sql variables for the worksheet
---------------------------------------------------------------------------
set my_db_schema = 'capstone.public';  -- your capstone database and schema
set my_raw_table = 'capstone.public.kapa_raw';  -- fully qualified name of the table where your raw kapa data resides
set my_stage = 'capstone.public.capstone_s3_stage';  -- fully qualified name of the external stage to your bucket
set my_json_data = 'RAW_DATA';  -- name of the variant column in your raw table that holds the kapa json data 
set my_json_ff = 'CAPSTONE.PUBLIC.kapa_json_format';  -- fully qualified name of your JSON file format
set kapa_offset = 0; -- kapa offset for resetting replay date

---------------------------------------------------------------------------
-- 3. Get and set the Kapa Offset value
--    The files we stream in are simply replays of the historical KAPA
--    files that are already in the bucket.  The code relies on you already
--    having your raw KAPA table already created with the raw data copied 
--    into it.  We replay files starting from 13-May-2022 and the KAPA_OFFSET
--    variable is used to reset the file replay back to that point in time.
---------------------------------------------------------------------------
-- Get the time difference between today and 13-May-2022.  You will
-- use this number as the time offset to start replaying files from that time.
-- Make the following changes to the sql below:
--   "v" = column name of the json variant column in your KAPA_RAW table
--   "kapa_raw" = the name of your raw kapa table
--   "file_name" = the name of the column that contains the File Name 
select timediff('SECONDS', min(RAW_DATA:clock::timestamp), current_timestamp())
from kapa_raw
where SOURCE_FILE_NAME = 'kapa-0001/year=2022/month=05/day=13/kapa-0001+0+0000017717.json.gz'
;

-- copy and paste the value from the query above into the kapa_offset variable
-- and execute the statement.
-- Once you set this you don't have to set it again unless you want to reset
-- the replay back to 13-May-2022.
set kapa_offset = 96990315;

---------------------------------------------------------------------------
-- 4. Execute the CHECK_S3_WRITE proc to test that you can write a file.
-- 
-- If the proc executes successfully, you should have a file created in a kapa-streams
-- folder in your bucket with the same year/month/day folder structure as kapa-0001.  
-- That means you should be good to proceed.  Delete any files 
-- you create during your testing before going to the next step to create and run the task.
---------------------------------------------------------------------------
call check_s3_write
(
  $kapa_offset,
  $my_raw_table,
  $my_stage,
  $my_json_ff,
  $my_json_data
);

---------------------------------------------------------------------------
-- 5. Execute the CREATE_STREAM_TASK proc to create the Stream Task
--
-- Before running this, make sure your role has EXECUTE MANAGED STREAM
-- and EXECUTE STREAM privileges.
-- Once this executes successfully, you should have a task 
-- that executes to create a new file in your bucket every 6 minutes.
-- Go to the "Managing Your Task" section to manage your task.
---------------------------------------------------------------------------
-- このコマンドはACCOUNTADMINロールで実行する必要があります
use role accountadmin;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE sysadmin;
use role sysadmin;
call create_stream_task
(
  $my_db_schema,
  $my_raw_table, 
  $kapa_offset, 
  $my_stage, 
  $my_json_ff,
  $my_json_data
)
;

---------------------------------------------------------------------------
-- Managing your task
-- Use these commands below to manage your task after creation
---------------------------------------------------------------------------

-- Task will be suspended after creation.  Use the show or desc commands
-- to check the "state" of the task (should be suspended).
show tasks like 'publish_kapa_adsb_data';
desc task publish_kapa_adsb_data;

-- Use the alter task command to resume and suspend the task.
alter task publish_kapa_adsb_data resume;
alter task publish_kapa_adsb_data suspend;

-- Run this sql to check how long til the next task runs.
-- If you look at the "minutes" in the SCHEDULED_TIME and compare it 
-- to the "minutes" on your wall clock, you should get a sense of 
-- when the task will kick-off.  If you execute this and there's no data
-- more than likely the task is already executing.  After the task executes,
-- you should see the next task scheduled for this query.
select timestampdiff(second, current_timestamp, scheduled_time) next_run, scheduled_time, name, state
from table(information_schema.task_history())
where state = 'SCHEDULED' 
and name in ('PUBLISH_KAPA_ADSB_DATA')
order by completed_time desc
;

-- This query will give you all information on the Task.  
select name, state, error_code, error_message, scheduled_time, query_start_time, 
next_scheduled_time, completed_time
from table(information_schema.task_history())
where name in ('PUBLISH_KAPA_ADSB_DATA')
-- order by completed_time desc
;

-- SQL to check Copy history for the serverless task
select *
  from table(information_schema.serverless_task_history(
    date_range_start=>dateadd(D, -7, current_date),
    date_range_end=>current_date,
    task_name=>'PUBLISH_KAPA_ADSB_DATA'));

