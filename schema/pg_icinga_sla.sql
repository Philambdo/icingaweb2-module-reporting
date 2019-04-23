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

DROP FUNCTION IF EXISTS icinga_availability_slatime;
CREATE OR REPLACE FUNCTION icinga_availability_slatime (
	id BIGINT,
	start_ts TIMESTAMP, 
	end_ts TIMESTAMP, 
	sla_timeperiod_object_id BIGINT)
RETURNS DECIMAL(7, 4)
AS $_$
DECLARE 
v_availability DECIMAL(7, 4);
t_du RECORD;
dummy_ID NUMERIC(20);
type_id INTEGER;
v_multiplicator INTEGER;
v_sum_up INTEGER;


BEGIN
  SELECT objecttype_id INTO dummy_id FROM icinga_objects WHERE object_id = id;
  IF dummy_id NOT IN (1, 2) THEN
    RETURN NULL;
  END IF;
  SELECT objecttype_id INTO type_id FROM icinga_objects WHERE object_id = id;


  FOR t_du in SELECT * from icinga_sla_updown_period(id, start_ts, end_ts, sla_timeperiod_object_id)
 LOOP
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

RETURN v_avaiability;

END;
$_$ LANGUAGE plpgsql SECURITY DEFINER;
