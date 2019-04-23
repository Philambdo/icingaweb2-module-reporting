DROP TABLE IF EXISTS icinga_sla_periods;
CREATE TABLE icinga_sla_periods (
  timeperiod_object_id BIGINT NOT NULL,
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP NOT NULL,
  PRIMARY KEY (timeperiod_object_id, start_time)
--  UNIQUE KEY (timeperiod_object_id, end_time)
) ;

DROP TABLE IF EXISTS icinga_outofsla_periods;
CREATE TABLE icinga_outofsla_periods (
  timeperiod_object_id BIGINT NOT NULL,
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP NOT NULL,
  PRIMARY KEY (timeperiod_object_id, start_time)
--  UNIQUE KEY (timeperiod_object_id, end_time)
) ;

