alter table jam.SF_NORM_ALARM add ADM_STATUS VARCHAR2(255);
alter table jam.SF_NORM_ALARM add ADM_START_TIME DATE;
alter table jam.SF_NORM_ALARM add ADM_END_TIME DATE;
alter table jam.SF_NORM_ALARM add ADM_BATCH_ID NUMBER(38);
alter table jam.SF_NORM_ALARM add ADM_QUEUE_START_TIME DATE;

--update
alter table jam.SF_NORM_ALARM add ADM_HEALTH_SCORE VARCHAR2(100);
alter table jam.SF_NORM_ALARM add ADM_THRESHOLD_VALUE VARCHAR2(10);
alter table jam.SF_NORM_ALARM add ADM_POINT_TREND VARCHAR2(255);
alter table jam.SF_NORM_ALARM add ADM_TIME_RETURN DATE;

alter table jam.SF_NORM_ALARM add ADM_STATUS_TIMEOUT_FLAG  VARCHAR2(10);