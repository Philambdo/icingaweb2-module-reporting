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

DROP FUNCTION IF EXISTS icinga_sla_updown_period;
CREATE OR REPLACE FUNCTION icinga_sla_updown_period (
	id BIGINT,
	start_ts TIMESTAMP, 
	end_ts TIMESTAMP, 
	sla_timeperiod_object_id BIGINT)
--RETURNS DECIMAL(7, 4)
RETURNS TABLE ( duration INTEGER , current_state INTEGER, next_state INTEGER, addd INTEGER, dt_depth INTEGER, type TEXT, start_time TIMESTAMP, end_time TIMESTAMP)
--RETURNS TABLE ( AVAILABILITY DECIMAL(7,4))
AS $_$
DECLARE 
v_availability DECIMAL(7, 4);
dummy_id BIGINT;
former_id BIGINT						= id;
--tp_lastday 					= "-1 day";
--tp_lastend TIMESTAMP					= 0;
former_sla_timeperiod_object_id BIGINT 	= sla_timeperiod_object_id;
former_start TIMESTAMP 					= start_ts;
former_end TIMESTAMP 				 	= end_ts;
sla_timeperiod_object_id BIGINT			= sla_timeperiod_object_id;
id BIGINT						 		= id;
v_start_ts TIMESTAMP       		 		= start_ts;
v_end_ts TIMESTAMP        		 		= end_ts;
v_last_state INTEGER        		 		:= NULL;
v_last_ts TIMESTAMP         		 		:= NULL;
v_cnt_dt INTEGER            		 		:= NULL;
v_cnt_tp INTEGER           		 		:= NULL;
v_add_duration INTEGER     		 		:= NULL;
v_current_state INTEGER  := NULL;
v_addd INTEGER :=NULL;
v_dt_depth INTEGER := NULL;
v_start_time TIMESTAMP;
v_end_time TIMESTAMP;
v_type_id INTEGER							:= NULL;
evdu RECORD;
t_du RECORD;
v_sum_duration DECIMAL(7,4) =0.0;
v_sum_up INTEGER := 0;
v_multiplicator INTEGER := NULL;


BEGIN
    SELECT objecttype_id INTO v_type_id FROM icinga_objects WHERE object_id = id;
	IF v_type_id NOT IN (1, 2) THEN
	    RETURN NEXT;
	END IF;



CREATE TEMP TABLE events_duration AS (
	SELECT * FROM (
		SELECT r_state_time AS state_time,
				r_type as type,
				r_state as state,
				r_last_state as last_state 
		FROM 
			icinga_normalized_history ( id, v_start_ts, v_end_ts, sla_timeperiod_object_id )
	) events
	  ORDER BY events.state_time ASC,
    	CASE events.type 
	      WHEN 'former_state' THEN 0
	      WHEN 'soft_state' THEN 1
	      WHEN 'hard_state' THEN 2
	      WHEN 'current_state' THEN 3
	      WHEN 'future_state' THEN 4
	      WHEN 'sla_end' THEN 5
	      WHEN 'sla_start' THEN 6
	      WHEN 'dt_start' THEN 7
	      WHEN 'dt_end' THEN 8
	      ELSE 9
	    END ASC
);

RAISE NOTICE 'TEST';

CREATE TEMP TABLE t_duration ( duration INTEGER , current_state INTEGER, next_state INTEGER, addd INTEGER, dt_depth INTEGER, type TEXT, start_time TIMESTAMP, end_time TIMESTAMP);

FOR evdu IN SELECT * FROM events_duration LOOP
  IF v_last_ts IS NULL THEN
    -- ...remember the duration and return 0...
    v_add_duration = ((COALESCE(v_add_duration, 0)
      + UNIX_TIMESTAMP(evdu.state_time)
      - UNIX_TIMESTAMP(COALESCE(v_last_ts, v_start_ts)) + 1
    )::INTEGER - 1);
  ELSE
    -- ...otherwise return a correct duration...
    v_add_duration=(UNIX_TIMESTAMP(evdu.state_time)
      - UNIX_TIMESTAMP(COALESCE(v_last_ts, v_start_ts))
      -- ...and don't forget to add what we remembered 'til now:
      + COALESCE(CASE v_cnt_dt + v_cnt_tp WHEN 0 THEN v_add_duration ELSE NULL END, 0));
  END IF;

  IF v_cnt_dt + v_cnt_tp >= 1 THEN
	v_current_state=0;
  ELSE
	v_current_state=COALESCE(v_last_state, evdu.last_state);
  END IF;

  IF evdu.type in ('hard_state', 'former_state', 'current_state') THEN
	v_last_state := evdu.state;
  ELSIF evdu.type = 'soft_state' THEN
    IF v_last_state is NULL THEN
      v_last_state := evdu.last_state;
	END IF;
  ELSIF evdu.type IN ('dt_start', 'sla_end') THEN
	v_last_state = 0;
  END IF;

  IF v_add_duration IS NOT NULL AND v_cnt_dt = 0 and v_cnt_tp = 0
  THEN
    v_addd := v_add_duration;
  ELSE
    v_addd := 0;
  END IF;


  IF evdu.type = 'dt_start'  THEN v_cnt_dt := COALESCE(v_cnt_dt, 0) + 1;
  ELSIF evdu.type = 'dt_end'  THEN v_cnt_dt := COALESCE(v_cnt_dt -1, 0);
  ELSIF evdu.type = 'sla_end'  THEN v_cnt_tp := COALESCE(v_cnt_tp, 0) + 1;
  ELSIF evdu.type = 'sla_start'  THEN v_cnt_tp := COALESCE(v_cnt_tp-1, 0);
  ELSE v_dt_depth := v_cnt_dt + v_cnt_tp;
  END IF;

  v_start_time := COALESCE(v_last_ts, v_start_ts);

  v_last_ts := evdu.state_time;

  v_end_time := evdu.state_time;

  INSERT INTO t_duration ( duration, current_state, next_state, addd, dt_depth, type, start_time, end_time) VALUES
  (v_add_duration, v_current_state, v_last_state, v_addd, v_dt_depth, evdu.type, v_start_time, v_end_time);
  v_add_duration := NULL;

END LOOP;

FOR t_du in SELECT * from t_duration LOOP
	IF v_type_id=1 THEN
		IF t_du.current_state = 0 THEN
			v_multiplicator := 1;
	    ELSE	
			v_multiplicator := 0;
		END IF;
	ELSE
		IF t_du.current_state < 2 THEN
			v_multiplicator := 1;
		ELSE
			v_multiplicator := 0;
		END IF;
	END IF;
	v_sum_up = v_sum_up + t_du.duration*v_multiplicator;
END LOOP;

v_availability = v_sum_up / (UNIX_TIMESTAMP(end_ts)-UNIX_TIMESTAMP(start_ts))::FLOAT * 100::FLOAT;

--DROP TABLE t_duration;       
--DROP TABLE events_duration;
RETURN QUERY 
	SELECT * FROM t_duration;
DROP TABLE t_duration;       
DROP TABLE events_duration;
RETURN;                     
--RETURN v_availability;

END;
$_$ LANGUAGE plpgsql SECURITY DEFINER;
