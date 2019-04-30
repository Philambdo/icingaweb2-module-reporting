-- --------------------------------------------------- --
-- SLA function for Icinga/IDO                         --
--                                                     --
-- Author    : Icinga Developer Team <infoicinga.org> --
-- Copyright : 2012 Icinga Developer Team              --
-- License   : GPL 2.0                                 --
-- --------------------------------------------------- --

--
-- History
-- 
-- 2012-08-31: Added to Icinga
-- 2013-08-20: Simplified and improved
-- 2013-08-23: Refactored, added SLA time period support
--

DROP FUNCTION IF EXISTS icinga_normalized_history;
CREATE OR REPLACE FUNCTION icinga_normalized_history (
	id BIGINT,
	i_start_ts TIMESTAMP, 
	i_end_ts TIMESTAMP, 
	sla_timeperiod_object_id BIGINT)
RETURNS TABLE (
	r_state_time TIMESTAMP,
	r_type TEXT, 
	r_state INTEGER,
	r_last_state INTEGER
)
AS $_$
DECLARE 
sla_timeperiod_object_id BIGINT			= sla_timeperiod_object_id;
id BIGINT						 		= id;
i_start_ts TIMESTAMP       		 		= i_start_ts;
i_end_ts TIMESTAMP        		 		= i_end_ts;

BEGIN

RAISE NOTICE 'i_start_ts: %, i_end_ts: %', i_start_ts, i_end_ts;

RETURN QUERY SELECT
     state_time,
     CASE state_type WHEN 1 THEN 'hard_state' ELSE 'soft_state' END AS type,
     state,
     -- Workaround for a nasty Icinga issue. In case a hard state is reached
     -- before max_check_attempts, the last_hard_state value is wrong. As of
     -- this we are stepping through all single events, even soft ones. Of
     -- course soft states do not have an influence on the availability:
     CASE state_type WHEN 1 THEN last_state ELSE last_hard_state END AS last_state
  FROM icinga_statehistory
  WHERE object_id = id
    AND state_time >= i_start_ts
    AND state_time <= i_end_ts
  -- STOP fetching statehistory events

  -- START fetching last state BEFORE the given interval as an event
  UNION SELECT * FROM (
    SELECT
      i_start_ts AS state_time,
      'former_state' AS type,
      CASE state_type WHEN 1 THEN state ELSE last_hard_state END AS state,
      CASE state_type WHEN 1 THEN last_state ELSE last_hard_state END AS last_state
    FROM icinga_statehistory h
    WHERE object_id = id
      AND state_time < i_start_ts
    ORDER BY h.state_time DESC LIMIT 1
  ) formerstate
  -- END fetching last state BEFORE the given interval as an event

  -- START fetching first state AFTER the given interval as an event
  UNION SELECT * FROM (
    SELECT
      i_end_ts AS state_time,
      'future_state' AS type,
      CASE state_type WHEN 1 THEN last_state ELSE last_hard_state END AS state,
      CASE state_type WHEN 1 THEN state ELSE last_hard_state END AS last_state
    FROM icinga_statehistory h
    WHERE object_id = id
      AND state_time > i_end_ts
    ORDER BY h.state_time ASC LIMIT 1
  ) futurestate
  -- END fetching first state AFTER the given interval as an event

  -- START ADDING a fake end
  UNION SELECT
    i_end_ts AS state_time,
    'dt_start' AS type,
    NULL AS state,
    NULL AS last_state
  -- FROM DUAL
  -- END ADDING a fake end

  -- START fetching current host state as an event
  -- TODO: This is not 100% correct. state should be fine, last_state sometimes isn't.
  UNION SELECT 
    GREATEST(
      i_start_ts,
      CASE state_type WHEN 1 THEN last_state_change ELSE last_hard_state_change END
    ) AS state_time,
    'current_state' AS type,
    CASE state_type WHEN 1 THEN current_state ELSE last_hard_state END AS state,
    last_hard_state AS last_state
  FROM icinga_hoststatus
  WHERE CASE state_type WHEN 1 THEN last_state_change ELSE last_hard_state_change END < i_start_ts
    AND host_object_id = id
    AND CASE state_type WHEN 1 THEN last_state_change ELSE last_hard_state_change END <= i_end_ts
    AND status_update_time > i_start_ts
  -- END fetching current host state as an event

  -- START fetching current service state as an event
  UNION SELECT 
    GREATEST(
      i_start_ts,
      CASE state_type WHEN 1 THEN last_state_change ELSE last_hard_state_change END
    ) AS state_time,
    'current_state' AS type,
    CASE state_type WHEN 1 THEN current_state ELSE last_hard_state END AS state,
    last_hard_state AS last_state
  FROM icinga_servicestatus
  WHERE CASE state_type WHEN 1 THEN last_state_change ELSE last_hard_state_change END < i_start_ts
    AND service_object_id = id
    AND CASE state_type WHEN 1 THEN last_state_change ELSE last_hard_state_change END <= i_end_ts
    AND status_update_time > i_start_ts
  -- END fetching current service state as an event

  -- START adding add all related downtime start times
  -- TODO: Handling downtimes still being active would be nice.
  --       But pay attention: they could be completely outdated
  UNION SELECT
    GREATEST(actual_start_time, i_start_ts) AS state_time,
    'dt_start' AS type,
    NULL AS state,
    NULL AS last_state
  FROM icinga_downtimehistory
  WHERE object_id = id
    AND actual_start_time < i_end_ts
    AND actual_end_time > i_start_ts
  -- STOP adding add all related downtime start times

  -- START adding add all related downtime end times
  UNION SELECT
    LEAST(actual_end_time, i_end_ts) AS state_time,
    'dt_end' AS type,
    NULL AS state,
    NULL AS last_state
  FROM icinga_downtimehistory
  WHERE object_id = id
    AND actual_start_time < i_end_ts
    AND actual_end_time > i_start_ts
  -- STOP adding add all related downtime end times

  -- START fetching SLA time period start times ---
  UNION ALL
    SELECT
      start_time AS state_time,
      'sla_start' AS type,
      NULL AS state,
      NULL AS last_state
    FROM icinga_outofsla_periods
    WHERE timeperiod_object_id = sla_timeperiod_object_id
      AND start_time >= i_start_ts AND start_time <= i_end_ts
  -- STOP fetching SLA time period start times ---

  -- START fetching SLA time period end times ---
  UNION ALL SELECT
      end_time AS state_time,
      'sla_end' AS type,
      NULL AS state,
      NULL AS last_state
    FROM icinga_outofsla_periods
    WHERE timeperiod_object_id = sla_timeperiod_object_id
      AND end_time >= i_start_ts AND end_time <= i_end_ts
  -- STOP fetching SLA time period end times ---

  ORDER BY state_time ASC;

END;
$_$ LANGUAGE plpgsql SECURITY DEFINER;
