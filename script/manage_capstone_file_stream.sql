-- Set your context
use role sysadmin;  -- replace with your role
use warehouse compute_wh_xl ;  -- replace with your warehouse
use database capstone; -- replace with your database
use schema capstone.public;  -- replace with your schema

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

---------------------------------------------------------------------------
-- Resetting your File Stream
---------------------------------------------------------------------------
-- To reset do the following:
-- 1. Suspend and drop the task
-- 2. Pause the snowpipe
-- 2. Delete files from the stage after 2025-05-31
-- 3. Delete records from the kapa-stage-raw table
-- 
alter task capstone26_db.prod.publish_kapa_adsb_data suspend;

drop task capstone26_db.prod.publish_kapa_adsb_data;

alter pipe capstone26_db.prod.dkp_pipe set pipe_execution_paused = true;
-- alter pipe capstone26_db.prod.dkp_pipe refresh;
-- alter pipe capstone26_db.prod.dkp_pipe set pipe_execution_paused = false; -- Restarting the pipe puts the  refresh data back into raw.kapa

select count(*)
from capstone26_db.prod.kapa_stream_raw;

delete from capstone26_db.prod.kapa_stream_raw;



