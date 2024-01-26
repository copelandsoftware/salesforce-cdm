CREATE OR REPLACE PACKAGE BODY sf_dg_innovations_pkg AS

    PROCEDURE dg_innovations_proc (
        errbuf    OUT   VARCHAR2,
        retcode   OUT   VARCHAR2
    ) AS
    currentGMTTime        DATE;
    BEGIN
        dbms_output.put_line('----------------------------------------------------------------------------------'
                             || chr(13)
                             || chr(10)
                             || 'Insert sf_dg_innocations table Started @ '
                             || to_char(sysdate, 'MM/DD/YYYY HH24:MI:SS')
                             || chr(13)
                             || chr(10)
                             || '----------------------------------------------------------------------------------');
        --get current time with GMT timezone
        currentGMTTime := F_Get_Timezone(sysdate);

        MERGE INTO sf_dg_innovations sdi
        USING (
        ----------- Start to calculate dg innovation report data ----------------------------
                  SELECT
                      a.*,
                      regexp_count(exceed_limit_unit, ',') + 1 AS number_of_units_down,
                      total_units
                  FROM
                      (
                          SELECT
                              TRIM(replace(ss.sf_site_name, 'Dollar General ')) AS store,
                              trunc(sna.time_occurred) AS alarm_monitoring_date,
                              sc.sf_cust_name AS store_chain,
                              COUNT(DISTINCT
                                  CASE
                                      WHEN(substr(sna.field3, 1, 2) = '80'
                                           AND sna.descr = 'High Limit Alarm') THEN
                                          sna.alarm_id
                                  END
                              ) exceed_80,
                              COUNT(DISTINCT
                                  CASE
                                      WHEN(substr(sna.field3, 1, 2) = '85'
                                           AND sna.descr = 'Occupied Hi Limit Exceeded') THEN
                                          sna.alarm_id
                                  END
                              ) exceed_85,
                              COUNT(DISTINCT
                                  CASE
                                      WHEN(sna.descr = 'Differential Limit Exceeded') THEN
                                          sna.alarm_id
                                  END
                              ) exceed_limit,
                              COUNT(DISTINCT
                                  CASE
                                      WHEN(sna.descr = 'Appl not keeping set point') THEN
                                          sna.alarm_id
                                  END
                              ) exceed_setpoint,
                              dbms_lob.substr(replace(regexp_replace(LISTAGG(
                                  CASE
                                      WHEN(substr(sna.field3, 1, 2) = '80'
                                           AND sna.descr = 'High Limit Alarm') THEN
                                          sna.source
                                      ELSE
                                          NULL
                                  END
                              ,',') WITHIN GROUP (ORDER BY 1),'([^,]+)(,\1)+', '\1'), 'HVAC UNIT #'), 4000, 1) exceed_80_unit,
                              dbms_lob.substr(replace(regexp_replace(LISTAGG(
                                  CASE
                                      WHEN(substr(sna.field3, 1, 2) = '85'
                                           AND sna.descr = 'Occupied Hi Limit Exceeded') THEN
                                          sna.source
                                      ELSE
                                          NULL
                                  END
                              ,',') WITHIN GROUP (ORDER BY 1),'([^,]+)(,\1)+', '\1'), 'HVAC UNIT #'), 4000, 1) exceed_85_unit,
                              dbms_lob.substr(replace(regexp_replace(LISTAGG(
                                  CASE
                                      WHEN(sna.descr = 'Differential Limit Exceeded') THEN
                                          sna.source
                                      ELSE
                                          NULL
                                  END
                              ,',') WITHIN GROUP (ORDER BY 1),'([^,]+)(,\1)+', '\1'), 'HVAC UNIT #'), 4000, 1) exceed_limit_unit,
                              dbms_lob.substr(replace(regexp_replace(LISTAGG(
                                  CASE
                                      WHEN(sna.descr = 'Appl not keeping set point') THEN
                                          sna.source
                                      ELSE
                                          NULL
                                  END
                              ,',') WITHIN GROUP (ORDER BY 1),'([^,]+)(,\1)+', '\1'), 'HVAC UNIT #'), 4000, 1) exceed_setpoint_unit,
                              COUNT(DISTINCT(
                                  CASE
                                      WHEN(sna.descr = 'Appl not keeping set point') THEN
                                          sna.alarm_id
                                      WHEN(sna.descr = 'Differential Limit Exceeded') THEN
                                          sna.alarm_id
                                      WHEN(substr(sna.field3, 1, 2) = '85'
                                           AND sna.descr = 'Occupied Hi Limit Exceeded') THEN
                                          sna.alarm_id
                                      WHEN(substr(sna.field3, 1, 2) = '80'
                                           AND sna.descr = 'High Limit Alarm') THEN
                                          sna.alarm_id
                                      ELSE
                                          NULL
                                  END
                              )) event_count,
							  ss.state
                          FROM
                              sf_site         ss,
                              sf_customer     sc,
                              sf_norm_alarm   sna
                          WHERE
                              ss.sf_cust_id = sc.sf_cust_id
                              AND sc.sf_cust_name LIKE 'Dollar General Traditional%'
                              AND ss.sf_site_id = sna.sf_site_id
                              AND ( sna.time_occurred ) > trunc(sysdate - 1)
                              AND sna.source LIKE 'HVAC UNIT%'
                              AND sna.field12 = '0'
                              AND sna.descr IN (
                                  'Differential Limit Exceeded',
                                  'Appl not keeping set point',
                                  'High Limit Alarm',
                                  'Occupied Hi Limit Exceeded'
                              )
                          GROUP BY
                              ss.sf_site_name,
                              sc.sf_cust_name,
                              trunc(sna.time_occurred),
							  ss.state
                      ) a,
                      dg_innovation_units_lkp lkp
                  WHERE
                      a.store = to_char(lkp.store(+))
                      AND event_count <> 0
              )
        -------------------- end to calculate dg innovation report data --------------------------------
        alarm ON ( sdi.store = alarm.store and sdi.state = alarm.state
                   AND sdi.alarm_monitoring_date = alarm.alarm_monitoring_date )
        WHEN MATCHED THEN UPDATE
        SET sdi.DEG_80_EXCEEDED_1 = alarm.exceed_80,
            sdi.DEG_85_EXCEEDED_1 = alarm.exceed_85,
            sdi.UNIT_RUNNING_AND_NOT_COOLING_1 = alarm.exceed_limit,
            sdi.SPACE_TEMP_NOT_ACHIEVED_1 = alarm.exceed_setpoint,
            sdi.DEG_80_EXCEEDED_2 = alarm.exceed_80_unit,
            sdi.DEG_85_EXCEEDED_2 = alarm.exceed_85_unit,
            sdi.UNIT_RUNNING_AND_NOT_COOLING_2 = alarm.exceed_limit_unit,
            sdi.SPACE_TEMP_NOT_ACHIEVED_2 = alarm.exceed_setpoint_unit,
            sdi.TOTAL_ALARMS = alarm.event_count,
            sdi.NUMBER_OF_UNITS_DOWN = alarm.Number_of_Units_Down,
            sdi.TOTAL_UNITS = alarm.Total_units,
            sdi.PROCESSED_FLAG = 'YQ',
            modified_on = currentGMTTime
        WHEN NOT MATCHED THEN
        INSERT (
            STORE,
            ALARM_MONITORING_DATE,
            STORE_CHAIN,
            DEG_80_EXCEEDED_1,
            DEG_85_EXCEEDED_1,
            UNIT_RUNNING_AND_NOT_COOLING_1,
            SPACE_TEMP_NOT_ACHIEVED_1,
            DEG_80_EXCEEDED_2,
            DEG_85_EXCEEDED_2,
            UNIT_RUNNING_AND_NOT_COOLING_2,
            SPACE_TEMP_NOT_ACHIEVED_2,
            TOTAL_ALARMS,
            NUMBER_OF_UNITS_DOWN,
            TOTAL_UNITS,
            PROCESSED_FLAG,
			STATE,
            created_on,
            created_by,
            modified_on,
            modified_by )
        VALUES
            ( alarm.STORE,
              alarm.ALARM_MONITORING_DATE,
              alarm.STORE_CHAIN,
              alarm.exceed_80,
              alarm.exceed_85,
              alarm.exceed_limit,
              alarm.exceed_setpoint,
              alarm.exceed_80_unit,
              alarm.exceed_85_unit,
              alarm.exceed_limit_unit,
              alarm.exceed_setpoint_unit,
              alarm.event_count,
              alarm.Number_of_Units_Down,
              alarm.Total_units,
              'YQ',
			  alarm.state,
              currentGMTTime,
              'PLSQL',
              currentGMTTime,
              'PLSQL' );
	  commit;

    END dg_innovations_proc;

    FUNCTION f_get_timezone (
        i_timevalue IN DATE
    ) RETURN DATE IS
        v_updated_date DATE;
    BEGIN
        SELECT
            i_timevalue - ( substr(tz_offset(sessiontimezone), 1, instr(tz_offset(sessiontimezone), ':') - 1) / 24 + substr(tz_offset
            (sessiontimezone), instr(tz_offset(sessiontimezone), ':') + 1, 2) / 1440 )
        INTO v_updated_date
        FROM
            dual;

        RETURN v_updated_date;
          --
    EXCEPTION
        WHEN no_data_found THEN
          --
            dbms_output.put_line('Execution Error in getting the timezone details');
        WHEN OTHERS THEN
          --
            dbms_output.put_line('Unhandled exception in F_Get_Timezone');
            dbms_output.put_line(sqlerrm);
          --
    END f_get_timezone;

END sf_dg_innovations_pkg;