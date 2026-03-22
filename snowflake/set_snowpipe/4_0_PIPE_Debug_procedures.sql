/***************************************
Check the state of the pipe
****************************************/
SHOW PIPES;

SELECT *
FROM TABLE(SNOWPIPE_RAW.INFORMATION_SCHEMA.PIPE_USAGE_HISTORY('SNOWPIPE_RAW.MDM_RELTIO.RELTIO_USERS'));



--COSTS
SELECT
    -- START_TIME,
    -- PIPE_ID,
    -- COALESCE(PIPE_NAME, 'External table refreshes') AS NAME, -- External table refreshes do not have a pipe name
    -- FILES_INSERTED,
    -- BYTES_INSERTED,
    -- CREDITS_USED AS TOTAL_CREDITS,
    -- 0.06 * FILES_INSERTED / 1000 AS FILES_CREDITS, -- 0.06 credits per 1000 files
    -- TOTAL_CREDITS - FILES_CREDITS AS COMPUTE_CREDITS
    *
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
ORDER BY START_TIME DESC;